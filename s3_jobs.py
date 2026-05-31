from __future__ import annotations

import hashlib
import json
import os
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any, Callable

import sqlite3
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import network_proxy
from s3_job_operations import UPLOAD_JOB_FAMILY, normalize_job_metadata

try:
    import boto3
except Exception:  # pragma: no cover
    boto3 = None


ACTIVE_JOB_STATUSES = {'queued', 'running', 'cancel_requested'}
DEFAULT_HISTORY_LIMIT = 200
UPLOAD_CLAIM_STALE_SECONDS = 5 * 60
PARTIAL_UPLOAD_STATUS = 'partial'
PREVIEW_ITEM_STATUS = 'preview'


@dataclass
class S3JobState:
    job_id: str
    dataset_id: str
    source: str
    limit: int
    status: str = 'queued'
    processed: int = 0
    uploaded: int = 0
    skipped: int = 0
    failed: int = 0
    total: int = 0
    started_at: float | None = None
    ended_at: float | None = None
    error: str | None = None
    cancel_requested: bool = False
    bucket: str | None = None
    prefix: str | None = None
    concurrency: int = 4
    source_filter: str | None = None
    selection_mode: str = 'pending'
    excluded_complete_count: int = 0
    last_message: str | None = None
    items: list[dict[str, Any]] | None = None
    job_family: str = UPLOAD_JOB_FAMILY
    dry_run: bool = False
    kind: str = 'upload'
    download_stats: dict[str, Any] | None = None


JOB_STATE_FIELDS = {field.name for field in fields(S3JobState)}


class S3JobManager:
    def __init__(
        self,
        store_path: str | Path | None = None,
        history_limit: int = DEFAULT_HISTORY_LIMIT,
        load_jobs_fn: Callable[[], list[dict[str, Any]]] | None = None,
        save_jobs_fn: Callable[[list[dict[str, Any]]], None] | None = None,
        db_connect_fn: Callable[[], Any] | None = None,
    ):
        self._jobs: dict[str, S3JobState] = {}
        self._locks: dict[str, threading.Event] = {}
        self._claimed_upload_products: dict[tuple[str, str], str] = {}
        self._mutex = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._store_path = Path(store_path) if store_path else Path(__file__).resolve().parent / 's3_jobs_state.json'
        self._history_limit = max(10, int(history_limit or DEFAULT_HISTORY_LIMIT))
        self._load_jobs_fn = load_jobs_fn
        self._save_jobs_fn = save_jobs_fn
        self._db_connect_fn = db_connect_fn
        self._load_jobs()

    def list_jobs(self, job_family: str | None = None, *, page: int = 1, page_size: int | None = None):
        with self._mutex:
            jobs = self._sorted_jobs_unlocked()
            if job_family:
                jobs = [job for job in jobs if job.job_family == job_family]
            total = len(jobs)
            resolved_page_size = max(1, int(page_size or total or 1))
            resolved_page = max(1, int(page or 1))
            start = (resolved_page - 1) * resolved_page_size
            end = start + resolved_page_size
            paged = jobs[start:end]
            return {
                'jobs': [asdict(job) for job in paged],
                'pagination': {
                    'page': resolved_page,
                    'pageSize': resolved_page_size,
                    'total': total,
                    'totalPages': max(1, (total + resolved_page_size - 1) // resolved_page_size) if total else 1,
                    'from': 0 if total == 0 else start + 1,
                    'to': min(end, total),
                },
            }

    def count_active_jobs(self, *, job_family: str | None = None, dataset_id: str | None = None) -> int:
        with self._mutex:
            jobs = self._jobs.values()
            if job_family:
                jobs = [job for job in jobs if job.job_family == job_family]
            if dataset_id:
                jobs = [job for job in jobs if job.dataset_id == dataset_id]
            return sum(1 for job in jobs if job.status in ACTIVE_JOB_STATUSES)

    def get_job(self, job_id: str):
        with self._mutex:
            job = self._jobs.get(job_id)
            return asdict(job) if job else None

    def update_job(self, job_id: str, **patch):
        with self._mutex:
            job = self._jobs.get(job_id)
            if not job:
                return None

            normalized_patch = dict(patch)
            if any(key in normalized_patch for key in {'kind', 'job_family', 'dry_run'}):
                metadata = normalize_job_metadata(
                    kind=normalized_patch.get('kind', job.kind),
                    job_family=normalized_patch.get('job_family', job.job_family),
                    dry_run=normalized_patch.get('dry_run', job.dry_run),
                )
                normalized_patch.update(metadata)

            for field_name, value in normalized_patch.items():
                if field_name not in JOB_STATE_FIELDS or field_name == 'job_id' or value is None:
                    continue
                if field_name in {'limit', 'concurrency'}:
                    try:
                        value = max(1, int(value))
                    except Exception:
                        continue
                elif field_name in {'total', 'processed', 'uploaded', 'skipped', 'failed'}:
                    try:
                        value = max(0, int(value))
                    except Exception:
                        continue
                setattr(job, field_name, value)

            self._persist_jobs_best_effort_unlocked()
            return asdict(job)

    def reserve_upload_candidates(
        self,
        *,
        job_id: str,
        dataset_id: str,
        collector: Callable[[set[str]], dict[str, Any]],
        db_conn_binder: Callable[[Any | None], None] | None = None,
    ) -> dict[str, Any]:
        if self._db_connect_fn:
            return self._reserve_upload_candidates_db(job_id=job_id, dataset_id=dataset_id, collector=collector, db_conn_binder=db_conn_binder)
        with self._mutex:
            excluded_product_ids = {
                product_id
                for (claimed_dataset_id, product_id), claimed_job_id in self._claimed_upload_products.items()
                if claimed_dataset_id == dataset_id and claimed_job_id != job_id
            }
            collected = collector(set(excluded_product_ids))
            resolved_dataset_id = str(collected.get('dataset_id') or dataset_id).strip() or dataset_id
            claimed_rows = self._claim_upload_rows_unlocked(
                job_id,
                resolved_dataset_id,
                list(collected.get('rows') or []),
            )
            claimed = dict(collected)
            claimed['rows'] = claimed_rows
            claimed['total'] = len(claimed_rows)
            claimed['limit'] = max(1, len(claimed_rows) or int(claimed.get('limit') or 1))
            return claimed

    def release_job_claims(self, job_id: str) -> None:
        if self._db_connect_fn:
            self._release_job_claims_db(job_id)
        with self._mutex:
            self._release_job_claims_unlocked(job_id)

    def start_job(
        self,
        *,
        job_id: str,
        dataset_id: str,
        source: str,
        bucket: str,
        prefix: str = '',
        limit: int = 100,
        concurrency: int = 4,
        source_filter: str | None = None,
        selection_mode: str = 'pending',
        excluded_complete_count: int = 0,
        rows: list[dict[str, Any]],
        s3_client_factory=None,
        resolve_source_url=None,
        on_uploaded=None,
        dry_run: bool = False,
        job_family: str = UPLOAD_JOB_FAMILY,
    ):
        limited_rows = rows[: max(1, int(limit))]
        try:
            with self._mutex:
                claimed_rows = self._claim_upload_rows_unlocked(job_id, dataset_id, limited_rows) if job_family == UPLOAD_JOB_FAMILY else limited_rows
                self._create_job_unlocked(
                    job_id=job_id,
                    dataset_id=dataset_id,
                    source=source,
                    bucket=bucket,
                    prefix=prefix,
                    limit=max(1, int(limit)),
                    concurrency=max(1, int(concurrency)),
                    source_filter=source_filter,
                    selection_mode=str(selection_mode or 'pending').strip() or 'pending',
                    excluded_complete_count=max(0, int(excluded_complete_count or 0)),
                    total=len(claimed_rows),
                    job_family=job_family,
                    dry_run=dry_run,
                )
        except Exception:
            if job_family == UPLOAD_JOB_FAMILY:
                self.release_job_claims(job_id)
            raise

        try:
            future = self._executor.submit(
                self._run_job,
                job_id,
                claimed_rows,
                s3_client_factory,
                resolve_source_url,
                on_uploaded,
            )
        except Exception:
            with self._mutex:
                current = self._jobs.get(job_id)
                if current:
                    current.status = 'failed'
                    current.error = 'Failed to submit job to executor'
                    current.last_message = current.error
                    current.ended_at = time.time()
                    self._persist_jobs_best_effort_unlocked()
            if job_family == UPLOAD_JOB_FAMILY:
                self.release_job_claims(job_id)
            raise
        return future

    def start_custom_job(
        self,
        *,
        job_id: str,
        dataset_id: str,
        source: str,
        total: int,
        runner: Callable[[Callable[[dict[str, Any]], None], threading.Event], None],
        bucket: str | None = None,
        prefix: str | None = '',
        concurrency: int = 1,
        source_filter: str | None = None,
        selection_mode: str = 'pending',
        limit: int | None = None,
        excluded_complete_count: int = 0,
        dry_run: bool = False,
        job_family: str = UPLOAD_JOB_FAMILY,
        kind: str | None = None,
    ):
        with self._mutex:
            self._create_job_unlocked(
                job_id=job_id,
                dataset_id=dataset_id,
                source=source,
                bucket=bucket,
                prefix=prefix,
                limit=max(1, int(limit or total or 1)),
                concurrency=max(1, int(concurrency or 1)),
                source_filter=source_filter,
                selection_mode=str(selection_mode or 'pending').strip() or 'pending',
                excluded_complete_count=max(0, int(excluded_complete_count or 0)),
                total=max(0, int(total or 0)),
                job_family=job_family,
                dry_run=dry_run,
                kind=kind,
            )
            cancel_event = self._locks[job_id]

        future = self._executor.submit(self._run_custom_job, job_id, runner, cancel_event)
        return future

    def cancel_job(self, job_id: str):
        with self._mutex:
            job = self._jobs.get(job_id)
            if not job or job.status not in ACTIVE_JOB_STATUSES:
                return False
            job.cancel_requested = True
            job.status = 'cancel_requested'
            event = self._locks.get(job_id)
            if event:
                event.set()
            self._persist_jobs_best_effort_unlocked()
            return True

    def _claim_upload_rows_unlocked(self, job_id: str, dataset_id: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        claimed_rows: list[dict[str, Any]] = []
        for row in rows:
            product_id = str(row.get('id') or row.get('product_id') or row.get('goods_id') or '').strip()
            if not product_id:
                claimed_rows.append(row)
                continue
            claim_key = (dataset_id, product_id)
            claimed_job_id = self._claimed_upload_products.get(claim_key)
            if claimed_job_id and claimed_job_id != job_id:
                continue
            self._claimed_upload_products[claim_key] = job_id
            claimed_rows.append(row)
        return claimed_rows

    def _release_job_claims_unlocked(self, job_id: str) -> None:
        if not job_id:
            return
        self._claimed_upload_products = {
            claim_key: claimed_job_id
            for claim_key, claimed_job_id in self._claimed_upload_products.items()
            if claimed_job_id != job_id
        }

    def _reserve_upload_candidates_db(
        self,
        *,
        job_id: str,
        dataset_id: str,
        collector: Callable[[set[str]], dict[str, Any]],
        db_conn_binder: Callable[[Any | None], None] | None = None,
    ) -> dict[str, Any]:
        attempts = 5
        last_exc = None
        for attempt in range(attempts):
            conn = self._db_connect_fn()
            try:
                conn.execute('BEGIN IMMEDIATE')
                self._cleanup_stale_db_claims(conn)
                excluded_rows = conn.execute(
                    'SELECT product_id FROM s3_job_claims WHERE dataset_id = ? AND job_id != ?',
                    (dataset_id, job_id),
                ).fetchall()
                excluded_product_ids = {
                    str(row['product_id'] if hasattr(row, 'keys') else row[0])
                    for row in excluded_rows
                }
                if db_conn_binder:
                    db_conn_binder(conn)
                collected = collector(set(excluded_product_ids))
                resolved_dataset_id = str(collected.get('dataset_id') or dataset_id).strip() or dataset_id
                claimed_rows: list[dict[str, Any]] = []
                for row in list(collected.get('rows') or []):
                    product_id = str(row.get('id') or row.get('product_id') or row.get('goods_id') or '').strip()
                    if not product_id:
                        claimed_rows.append(row)
                        continue
                    cursor = conn.execute(
                        '''
                        INSERT INTO s3_job_claims (dataset_id, product_id, job_id, claimed_at)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(dataset_id, product_id) DO NOTHING
                        ''',
                        (resolved_dataset_id, product_id, job_id, time.time()),
                    )
                    if int(getattr(cursor, 'rowcount', 0) or 0) == 1:
                        claimed_rows.append(row)
                conn.commit()
                claimed = dict(collected)
                claimed['rows'] = claimed_rows
                claimed['total'] = len(claimed_rows)
                claimed['limit'] = max(1, len(claimed_rows) or int(claimed.get('limit') or 1))
                return claimed
            except sqlite3.OperationalError as exc:
                conn.rollback()
                last_exc = exc
                message = str(exc).lower()
                if 'database is locked' not in message or attempt == attempts - 1:
                    raise
                time.sleep(0.15 * (attempt + 1))
            finally:
                if db_conn_binder:
                    try:
                        db_conn_binder(None)
                    except Exception:
                        pass
                conn.close()
        if last_exc:
            raise last_exc
        raise RuntimeError('Failed to reserve upload candidates')

    def _release_job_claims_db(self, job_id: str) -> None:
        if not job_id or not self._db_connect_fn:
            return
        attempts = 5
        last_exc = None
        for attempt in range(attempts):
            conn = self._db_connect_fn()
            try:
                conn.execute('DELETE FROM s3_job_claims WHERE job_id = ?', (job_id,))
                conn.commit()
                return
            except sqlite3.OperationalError as exc:
                conn.rollback()
                last_exc = exc
                message = str(exc).lower()
                if 'database is locked' not in message or attempt == attempts - 1:
                    raise
                time.sleep(0.15 * (attempt + 1))
            finally:
                conn.close()
        if last_exc:
            raise last_exc

    def _cleanup_stale_db_claims(self, conn) -> None:
        active_job_ids = {
            job.job_id
            for job in self._jobs.values()
            if job.status in ACTIVE_JOB_STATUSES
        }
        persisted_active_rows = conn.execute(
            "SELECT job_id FROM s3_jobs WHERE json_extract(payload_json, '$.status') IN ('queued', 'running', 'cancel_requested')"
        ).fetchall()
        active_job_ids.update(
            str(row['job_id'] if hasattr(row, 'keys') else row[0])
            for row in persisted_active_rows
        )
        stale_before = time.time() - UPLOAD_CLAIM_STALE_SECONDS
        if active_job_ids:
            placeholders = ','.join('?' for _ in active_job_ids)
            conn.execute(
                f'DELETE FROM s3_job_claims WHERE claimed_at < ? AND job_id NOT IN ({placeholders})',
                (stale_before, *tuple(active_job_ids)),
            )
        else:
            conn.execute('DELETE FROM s3_job_claims WHERE claimed_at < ?', (stale_before,))

    def _create_job_unlocked(
        self,
        *,
        job_id: str,
        dataset_id: str,
        source: str,
        bucket: str | None,
        prefix: str | None,
        limit: int,
        concurrency: int,
        source_filter: str | None,
        selection_mode: str,
        excluded_complete_count: int,
        total: int,
        job_family: str,
        dry_run: bool,
        kind: str | None = None,
    ) -> S3JobState:
        existing = self._jobs.get(job_id)
        if existing and existing.status in ACTIVE_JOB_STATUSES:
            raise ValueError(f'Job already running: {job_id}')
        metadata = normalize_job_metadata(kind=kind, job_family=job_family, dry_run=dry_run)
        job = S3JobState(
            job_id=job_id,
            dataset_id=dataset_id,
            source=source,
            bucket=bucket,
            prefix=prefix,
            limit=max(1, int(limit)),
            concurrency=max(1, int(concurrency)),
            source_filter=source_filter,
            selection_mode=str(selection_mode or 'pending').strip() or 'pending',
            excluded_complete_count=max(0, int(excluded_complete_count or 0)),
            total=max(0, int(total or 0)),
            status='running',
            started_at=time.time(),
            items=[],
            job_family=metadata['job_family'],
            dry_run=metadata['dry_run'],
            kind=metadata['kind'],
        )
        self._jobs[job_id] = job
        self._locks[job_id] = threading.Event()
        self._persist_jobs_best_effort_unlocked()
        return job

    def _record_item_unlocked(self, current: S3JobState, item: dict[str, Any]):
        current.processed += 1
        if current.items is None:
            current.items = []
        current.items.append(item)
        status = str(item.get('status') or '').lower()
        if status == 'uploaded':
            current.uploaded += 1
        elif status == 'skipped':
            current.skipped += 1
        elif status == PARTIAL_UPLOAD_STATUS:
            current.uploaded += 1
        elif status == PREVIEW_ITEM_STATUS and current.dry_run:
            current.uploaded += 1
        else:
            current.failed += 1
        self._merge_download_stats_unlocked(current, item)
        message = item.get('message')
        if message:
            current.last_message = str(message)
        self._persist_jobs_best_effort_unlocked()

    def _notify_terminal_job_best_effort(self, job: dict[str, Any]) -> None:
        try:
            self._send_terminal_notification(job)
        except Exception:
            return

    def _run_custom_job(self, job_id: str, runner: Callable[[Callable[[dict[str, Any]], None], threading.Event], None], cancel_event: threading.Event):
        def record(item: dict[str, Any]):
            with self._mutex:
                current = self._jobs[job_id]
                self._record_item_unlocked(current, item)

        try:
            runner(record, cancel_event)
            with self._mutex:
                current = self._jobs[job_id]
                current.status = 'cancelled' if current.cancel_requested else 'completed'
                current.ended_at = time.time()
                self._persist_jobs_best_effort_unlocked()
                terminal_job = asdict(current)
            self._notify_terminal_job_best_effort(terminal_job)
        except Exception as exc:
            with self._mutex:
                current = self._jobs[job_id]
                if current.cancel_requested or cancel_event.is_set():
                    current.status = 'cancelled'
                    current.last_message = str(exc)
                    current.ended_at = time.time()
                    self._persist_jobs_best_effort_unlocked()
                    terminal_job = asdict(current)
                else:
                    current.status = 'failed'
                    current.error = str(exc)
                    current.last_message = str(exc)
                    current.ended_at = time.time()
                    if current.items is None:
                        current.items = []
                    current.items.append({
                        'status': 'failed',
                        'message': str(exc),
                        'timestamp': time.time(),
                    })
                    self._persist_jobs_best_effort_unlocked()
                    terminal_job = asdict(current)
            self._notify_terminal_job_best_effort(terminal_job)
        finally:
            self.release_job_claims(job_id)

    def _run_job(self, job_id: str, rows: list[dict[str, Any]], s3_client_factory, resolve_source_url, on_uploaded):
        with self._mutex:
            job = self._jobs[job_id]
        try:
            s3 = None
            if not job.dry_run:
                if boto3 is None:
                    raise RuntimeError('boto3 is not available')
                s3_client_factory = s3_client_factory or (lambda: boto3.client('s3'))
                s3 = s3_client_factory()
            with ThreadPoolExecutor(max_workers=job.concurrency) as pool:
                futures = []
                for row in rows:
                    if job.cancel_requested:
                        break
                    futures.append(pool.submit(self._process_row, s3, job, row, resolve_source_url, on_uploaded))
                for future in as_completed(futures):
                    if job.cancel_requested:
                        break
                    try:
                        result = future.result()
                        with self._mutex:
                            current = self._jobs[job_id]
                            if isinstance(result, dict):
                                self._record_item_unlocked(current, result)
                            elif result == 'uploaded':
                                current.processed += 1
                                current.uploaded += 1
                                self._persist_jobs_best_effort_unlocked()
                            elif result == 'skipped':
                                current.processed += 1
                                current.skipped += 1
                                self._persist_jobs_best_effort_unlocked()
                            elif result == PARTIAL_UPLOAD_STATUS:
                                current.processed += 1
                                current.uploaded += 1
                                self._persist_jobs_best_effort_unlocked()
                            elif result == PREVIEW_ITEM_STATUS and current.dry_run:
                                current.processed += 1
                                current.uploaded += 1
                                self._persist_jobs_best_effort_unlocked()
                            else:
                                current.processed += 1
                                current.failed += 1
                                self._persist_jobs_best_effort_unlocked()
                    except Exception as exc:
                        with self._mutex:
                            current = self._jobs[job_id]
                            current.processed += 1
                            current.failed += 1
                            current.last_message = str(exc)
                            if current.items is None:
                                current.items = []
                            current.items.append({
                                'status': 'failed',
                                'message': str(exc),
                                'timestamp': time.time(),
                            })
                            self._persist_jobs_best_effort_unlocked()
            with self._mutex:
                current = self._jobs[job_id]
                current.status = 'cancelled' if current.cancel_requested else 'completed'
                current.ended_at = time.time()
                self._persist_jobs_best_effort_unlocked()
                terminal_job = asdict(current)
            self._notify_terminal_job_best_effort(terminal_job)
        except Exception as exc:
            with self._mutex:
                current = self._jobs[job_id]
                current.status = 'failed'
                current.error = str(exc)
                current.last_message = str(exc)
                current.ended_at = time.time()
                self._persist_jobs_best_effort_unlocked()
                terminal_job = asdict(current)
            self._notify_terminal_job_best_effort(terminal_job)
        finally:
            self.release_job_claims(job_id)

    def _process_row(self, s3, job: S3JobState, row: dict[str, Any], resolve_source_url, on_uploaded):
        item = {
            'timestamp': time.time(),
            'dataset_id': job.dataset_id,
            'product_id': str(row.get('id') or ''),
            'goods_id': str(row.get('goods_id') or row.get('id') or ''),
            'name': row.get('name') or row.get('title') or '',
            'source_url': None,
            'key': None,
            'source_urls': [],
            's3_urls': [],
            's3_keys': [],
            'image_pairs': [],
            'image_total': 0,
            'image_uploaded': 0,
            'image_existing': 0,
            'image_failed': 0,
            'saved_on_s3': False,
            'failures': [],
            'status': 'skipped',
            'message': None,
            'download_stats': None,
        }
        if job.cancel_requested:
            item['message'] = 'Cancelled before processing'
            return item
        dry_run = bool(getattr(job, 'dry_run', False))
        raw_candidates = resolve_source_url(row) if callable(resolve_source_url) else None
        if isinstance(raw_candidates, (list, tuple)):
            candidates = [str(url).strip() for url in raw_candidates if isinstance(url, str) and url.strip() and str(url).strip().lower().startswith(('http://', 'https://'))]
        elif isinstance(raw_candidates, str) and raw_candidates.strip() and raw_candidates.strip().lower().startswith(('http://', 'https://')):
            candidates = [raw_candidates.strip()]
        else:
            candidates = []
        candidates = list(dict.fromkeys(candidates))
        item['source_urls'] = candidates
        item['image_total'] = len(candidates)
        if candidates:
            item['source_url'] = candidates[0]
            item['key'] = self._build_key(job, row, candidates[0])
        if not candidates:
            item['status'] = 'failed'
            item['message'] = 'No source URL available'
            return item

        last_error = None
        for source_url in candidates:
            key = self._build_key(job, row, source_url)
            s3_url = f's3://{job.bucket}/{key}'
            if s3 is not None and self._object_exists(s3, job.bucket, key):
                item['image_existing'] += 1
                item['s3_urls'].append(s3_url)
                item['s3_keys'].append(key)
                item['image_pairs'].append({
                    'source_url': source_url,
                    's3_url': s3_url,
                    'key': key,
                    'status': 'existing',
                })
                continue
            if dry_run:
                item['image_uploaded'] += 1
                item['s3_urls'].append(s3_url)
                item['s3_keys'].append(key)
                item['image_pairs'].append({
                    'source_url': source_url,
                    's3_url': s3_url,
                    'key': key,
                    'status': PREVIEW_ITEM_STATUS,
                })
                continue
            try:
                content, content_type, download_meta = self._download(source_url, dataset_id=job.dataset_id)
                if not content:
                    raise RuntimeError('Empty content')
                if content_type and not str(content_type).lower().startswith('image/'):
                    raise RuntimeError(f'Unexpected content type: {content_type}')
                s3.put_object(
                    Bucket=job.bucket,
                    Key=key,
                    Body=content,
                    ContentType=content_type or 'application/octet-stream',
                    Metadata={
                        'source_url': source_url,
                        'dataset_id': job.dataset_id,
                        'goods_id': str(row.get('goods_id') or row.get('id') or ''),
                    },
                )
                item['image_uploaded'] += 1
                item['s3_urls'].append(s3_url)
                item['s3_keys'].append(key)
                item['image_pairs'].append({
                    'source_url': source_url,
                    's3_url': s3_url,
                    'key': key,
                    'status': 'uploaded',
                })
                item['download_stats'] = download_meta
            except Exception as exc:
                last_error = exc
                item['image_failed'] += 1
                failure_meta = getattr(exc, 'download_meta', {}) if hasattr(exc, 'download_meta') else {}
                item['download_stats'] = failure_meta or item.get('download_stats')
                item['failures'].append({
                    'source_url': source_url,
                    'error': f'{type(exc).__name__}: {exc}',
                    'hostname': failure_meta.get('hostname'),
                    'attempt_count': failure_meta.get('attempt_count'),
                    'proxy_used': failure_meta.get('proxy_used'),
                    'timeout_seconds': failure_meta.get('timeout_seconds'),
                    'error_type': failure_meta.get('error_type'),
                    'http_status': failure_meta.get('http_status'),
                })

        uploaded_or_existing = len(item['s3_urls'])
        item['saved_on_s3'] = bool(candidates) and item['image_failed'] == 0 and uploaded_or_existing == len(candidates)
        if dry_run:
            would_upload = item['image_uploaded']
            if item['saved_on_s3']:
                item['status'] = PREVIEW_ITEM_STATUS
                if would_upload and item['image_existing']:
                    item['message'] = f"Preview: {would_upload} image(s) would be uploaded; {item['image_existing']} already exist on S3"
                elif would_upload:
                    item['message'] = f"Preview: {would_upload} image(s) would be uploaded"
                else:
                    item['message'] = 'Preview: all product images already exist on S3'
            elif uploaded_or_existing > 0:
                item['status'] = PREVIEW_ITEM_STATUS
                partial_bits = []
                if would_upload:
                    partial_bits.append(f"{would_upload} would upload")
                if item['image_existing']:
                    partial_bits.append(f"{item['image_existing']} already existed")
                partial_summary = '; '.join(partial_bits) if partial_bits else f'{uploaded_or_existing} available on S3'
                item['message'] = f"Preview: partial availability {uploaded_or_existing}/{len(candidates)} image(s); {item['image_failed']} would fail ({partial_summary})"
            else:
                item['status'] = 'failed'
                item['message'] = f'Preview failed: {last_error}' if last_error else 'Preview failed'
            return item

        if item['saved_on_s3']:
            if item['image_uploaded'] and item['image_existing']:
                item['status'] = 'uploaded'
                item['message'] = f"Uploaded {item['image_uploaded']} image(s); {item['image_existing']} already existed on S3"
            elif item['image_uploaded']:
                item['status'] = 'uploaded'
                item['message'] = f"Uploaded {item['image_uploaded']} image(s)"
            else:
                item['status'] = 'skipped'
                item['message'] = 'All product images already exist on S3'
        elif uploaded_or_existing > 0:
            item['status'] = PARTIAL_UPLOAD_STATUS
            partial_bits = []
            if item['image_uploaded']:
                partial_bits.append(f"{item['image_uploaded']} uploaded")
            if item['image_existing']:
                partial_bits.append(f"{item['image_existing']} already existed")
            partial_summary = '; '.join(partial_bits) if partial_bits else f'{uploaded_or_existing} available on S3'
            item['message'] = f"Partial success: {uploaded_or_existing}/{len(candidates)} image(s) available on S3; {item['image_failed']} failed ({partial_summary})"
        else:
            item['status'] = 'failed'
            item['message'] = f'All product image uploads failed: {last_error}' if last_error else 'All product image uploads failed'

        if callable(on_uploaded) and not dry_run:
            try:
                on_uploaded(row, item)
            except Exception as exc:
                item['status'] = 'failed'
                item['saved_on_s3'] = False
                item['message'] = f'Persistence error: {exc}'
        return item

    def _notifications_enabled(self) -> bool:
        return bool(os.getenv('TELEGRAM_BOT_TOKEN', '').strip() and os.getenv('TELEGRAM_CHAT_ID', '').strip())

    def _send_terminal_notification(self, job: dict[str, Any]) -> None:
        if not self._notifications_enabled():
            return
        token = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
        chat_id = os.getenv('TELEGRAM_CHAT_ID', '').strip()
        if not token or not chat_id:
            return
        raw_family = str(job.get('job_family') or job.get('kind') or 'job').strip().lower()
        family_title = {
            'upload': 'Upload',
            'url_migration': 'Migration URL',
            'state_cleanup': 'Cleanup',
            'migration': 'Migration URL',
            'cleanup': 'Cleanup',
        }.get(raw_family, 'Job S3')
        status = str(job.get('status') or 'completed').strip().lower() or 'completed'
        status_label = {
            'completed': 'Terminé',
            'failed': 'Échec',
            'cancelled': 'Annulé',
        }.get(status, status)
        dataset_id = str(job.get('dataset_id') or 'all')
        job_id = str(job.get('job_id') or '')
        total = int(job.get('total') or 0)
        processed = int(job.get('processed') or 0)
        uploaded = int(job.get('uploaded') or 0)
        skipped = int(job.get('skipped') or 0)
        failed = int(job.get('failed') or 0)
        excluded_complete_count = int(job.get('excluded_complete_count') or 0)
        dry_run = bool(job.get('dry_run'))
        error = str(job.get('error') or job.get('last_message') or '').strip()
        prefix = '🔎' if dry_run else ('✅' if status == 'completed' else '⚠️' if status == 'cancelled' else '❌')
        message_lines = [
            f"{prefix} *{family_title} terminé*",
            f"- Job: `{job_id}`",
            f"- Dataset: *{dataset_id}*",
            f"- Statut: *{status_label}*{' · dry-run' if dry_run else ''}",
            f"- Progression: *{processed}/{total}*",
            f"- Résultat: *{uploaded}* réussis · *{skipped}* ignorés · *{failed}* erreurs",
        ]
        if excluded_complete_count > 0:
            message_lines.append(f"- Produits déjà complets exclus: *{excluded_complete_count}*")
        if error and status in {'failed', 'cancelled'}:
            message_lines.append(f"- Détail: `{error[:500]}`")
        payload = {
            'chat_id': chat_id,
            'text': '\n'.join(message_lines),
            'parse_mode': 'Markdown',
        }
        request = Request(
            f'https://api.telegram.org/bot{token}/sendMessage',
            data=json.dumps(payload).encode('utf-8'),
            headers={'Content-Type': 'application/json; charset=utf-8'},
            method='POST',
        )
        with urlopen(request, timeout=10) as response:
            response.read()

    def _download(self, url: str, dataset_id: str | None = None):
        parsed = urlparse(url)
        hostname = (parsed.hostname or '').lower()
        referer = f'{parsed.scheme}://{parsed.netloc}/' if parsed.scheme and parsed.netloc else None
        timeout_plan = (20, 35, 50)
        backoff_plan: tuple[int, ...] = ()
        proxy_used = network_proxy.proxy_enabled()
        if hostname.endswith('asos-media.com') or str(dataset_id or '').strip().lower() == 'asos':
            referer = 'https://www.asos.com/'
            timeout_plan = (10, 20, 30)
            backoff_plan = (1, 3)
        elif hostname.endswith('ltwebstatic.com'):
            referer = 'https://us.shein.com/'

        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Sec-Fetch-Dest': 'image',
            'Sec-Fetch-Mode': 'no-cors',
            'Sec-Fetch-Site': 'cross-site',
            'Connection': 'close',
        }
        if referer:
            headers['Referer'] = referer

        req = Request(url, headers=headers)
        opener = network_proxy.build_urllib_opener(use_proxy=proxy_used)
        last_error = None
        last_meta = {
            'hostname': hostname,
            'proxy_used': proxy_used,
            'proxy_mode': 'proxy' if proxy_used else 'direct',
            'attempt_count': 0,
            'timeout_seconds': timeout_plan[-1],
            'error_type': None,
            'http_status': None,
        }
        for index, timeout_seconds in enumerate(timeout_plan, start=1):
            last_meta['attempt_count'] = index
            last_meta['timeout_seconds'] = timeout_seconds
            try:
                with opener.open(req, timeout=timeout_seconds) as resp:
                    chunks = []
                    while True:
                        chunk = resp.read(1024 * 256)
                        if not chunk:
                            break
                        chunks.append(chunk)
                    data = b''.join(chunks)
                    content_type = resp.headers.get_content_type()
                if not data:
                    raise RuntimeError('Empty content')
                last_meta['error_type'] = None
                last_meta['http_status'] = None
                return data, content_type, dict(last_meta)
            except HTTPError as exc:
                last_error = exc
                last_meta['http_status'] = int(getattr(exc, 'code', 0) or 0) or None
                last_meta['error_type'] = 'http_403' if last_meta['http_status'] == 403 else 'http_error'
                if last_meta['http_status'] != 403 or index >= len(timeout_plan):
                    break
            except (TimeoutError, socket.timeout) as exc:
                last_error = exc
                last_meta['error_type'] = 'timeout'
                last_meta['http_status'] = None
                if index >= len(timeout_plan):
                    break
            except URLError as exc:
                last_error = exc
                reason = getattr(exc, 'reason', None)
                if isinstance(reason, socket.timeout):
                    last_meta['error_type'] = 'timeout'
                else:
                    last_meta['error_type'] = 'network_error'
                last_meta['http_status'] = None
                if index >= len(timeout_plan):
                    break
            except Exception as exc:
                last_error = exc
                last_meta['error_type'] = 'network_error'
                last_meta['http_status'] = None
                if index >= len(timeout_plan):
                    break

            if index <= len(backoff_plan):
                time.sleep(backoff_plan[index - 1])

        if last_error is None:
            last_error = RuntimeError('Download failed')
        try:
            setattr(last_error, 'download_meta', dict(last_meta))
        except Exception:
            pass
        raise last_error

    def _merge_download_stats_unlocked(self, current: S3JobState, item: dict[str, Any]) -> None:
        item_stats = item.get('download_stats')
        if not isinstance(item_stats, dict):
            return
        hostname = str(item_stats.get('hostname') or 'unknown')
        proxy_mode = str(item_stats.get('proxy_mode') or ('proxy' if item_stats.get('proxy_used') else 'direct'))
        error_type = str(item_stats.get('error_type') or '')
        item_status = str(item.get('status') or '').lower()
        if item_status in {'uploaded', PARTIAL_UPLOAD_STATUS}:
            result_status = 'success'
        elif item_status == 'skipped':
            result_status = 'skipped'
        elif error_type:
            result_status = error_type
        else:
            result_status = 'failed'
        if current.download_stats is None or not isinstance(current.download_stats, dict):
            current.download_stats = {'by_host': {}}
        by_host = current.download_stats.setdefault('by_host', {})
        host_entry = by_host.setdefault(hostname, {'proxy_mode': {}})
        proxy_entry = host_entry['proxy_mode'].setdefault(proxy_mode, {})
        proxy_entry[result_status] = int(proxy_entry.get(result_status, 0) or 0) + 1

    def _object_exists(self, s3, bucket: str, key: str):
        try:
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            return False

    def _build_key(self, job: S3JobState, row: dict[str, Any], source_url: str):
        goods_id = str(row.get('goods_id') or row.get('id') or 'unknown')
        digest = hashlib.sha1(source_url.encode('utf-8')).hexdigest()[:12]
        return f"{job.prefix.strip('/') + '/' if job.prefix else ''}{job.dataset_id}/{goods_id}/{digest}.jpg"

    def _load_jobs(self):
        if self._load_jobs_fn:
            try:
                raw_jobs = self._load_jobs_fn()
            except Exception:
                return
        else:
            if not self._store_path.exists():
                return
            try:
                payload = json.loads(self._store_path.read_text(encoding='utf-8'))
            except Exception:
                return
            raw_jobs = payload.get('jobs') if isinstance(payload, dict) else payload
        if not isinstance(raw_jobs, list):
            return
        loaded: dict[str, S3JobState] = {}
        now = time.time()
        changed = False
        for raw_job in raw_jobs:
            job = self._coerce_job(raw_job)
            if not job:
                continue
            if job.status in ACTIVE_JOB_STATUSES:
                job.status = 'interrupted'
                job.cancel_requested = False
                job.ended_at = job.ended_at or now
                interruption_note = 'Server restarted before this job completed'
                if not job.last_message:
                    job.last_message = interruption_note
                elif interruption_note not in job.last_message:
                    job.last_message = f'{job.last_message} · {interruption_note}'
                changed = True
            loaded[job.job_id] = job
        with self._mutex:
            self._jobs = loaded
            self._prune_jobs_unlocked()
            if changed:
                self._persist_jobs_best_effort_unlocked()

    def _coerce_job(self, raw_job: Any) -> S3JobState | None:
        if not isinstance(raw_job, dict):
            return None
        payload = {key: raw_job.get(key) for key in JOB_STATE_FIELDS if key in raw_job}
        for required in ('job_id', 'dataset_id', 'source', 'limit'):
            if required not in payload or payload.get(required) in {None, ''}:
                return None
        metadata = normalize_job_metadata(
            kind=raw_job.get('kind'),
            job_family=raw_job.get('job_family'),
            dry_run=raw_job.get('dry_run'),
        )
        payload['job_id'] = str(payload['job_id'])
        payload['dataset_id'] = str(payload['dataset_id'])
        payload['source'] = str(payload['source'])
        payload['limit'] = max(1, int(payload.get('limit') or 1))
        payload['processed'] = max(0, int(payload.get('processed') or 0))
        payload['uploaded'] = max(0, int(payload.get('uploaded') or 0))
        payload['skipped'] = max(0, int(payload.get('skipped') or 0))
        payload['failed'] = max(0, int(payload.get('failed') or 0))
        payload['total'] = max(0, int(payload.get('total') or 0))
        payload['concurrency'] = max(1, int(payload.get('concurrency') or 1))
        payload['cancel_requested'] = bool(payload.get('cancel_requested'))
        payload['job_family'] = metadata['job_family']
        payload['dry_run'] = metadata['dry_run']
        payload['kind'] = metadata['kind']
        if not isinstance(payload.get('items'), list):
            payload['items'] = [] if payload.get('items') is not None else None
        return S3JobState(**payload)

    def _sorted_jobs_unlocked(self) -> list[S3JobState]:
        return sorted(
            self._jobs.values(),
            key=lambda job: (
                float(job.started_at or 0),
                float(job.ended_at or 0),
                job.job_id,
            ),
            reverse=True,
        )

    def _prune_jobs_unlocked(self):
        active_ids = {job.job_id for job in self._jobs.values() if job.status in ACTIVE_JOB_STATUSES}
        sorted_jobs = self._sorted_jobs_unlocked()
        if len(sorted_jobs) <= self._history_limit and not active_ids:
            return
        kept: dict[str, S3JobState] = {}
        for job in sorted_jobs:
            if job.job_id in active_ids or len(kept) < self._history_limit:
                kept[job.job_id] = job
        self._jobs = kept

    def _persist_jobs_best_effort_unlocked(self):
        try:
            self._persist_jobs_unlocked()
        except Exception as exc:
            active_job = next((job for job in self._jobs.values() if job.status in ACTIVE_JOB_STATUSES), None)
            if active_job and not str(active_job.last_message or '').startswith('History persistence warning:'):
                active_job.last_message = f'History persistence warning: {exc}'

    def _persist_jobs_unlocked(self):
        self._prune_jobs_unlocked()
        jobs_payload = [asdict(job) for job in self._sorted_jobs_unlocked()]
        if self._save_jobs_fn:
            self._save_jobs_fn(jobs_payload)
            return
        payload = {
            'jobs': jobs_payload,
        }
        self._store_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._store_path.with_suffix(f'{self._store_path.suffix}.tmp')
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, separators=(',', ':')), encoding='utf-8')
        tmp_path.replace(self._store_path)

    def mark_saved(self, resource: dict[str, Any], s3_url: str | None = None):
        resource['saved_on_s3'] = bool(s3_url)
        resource['s3_url'] = s3_url
        return resource

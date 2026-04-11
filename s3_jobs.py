from __future__ import annotations

import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

try:
    import boto3
except Exception:  # pragma: no cover
    boto3 = None


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
    last_message: str | None = None
    items: list[dict[str, Any]] | None = None


class S3JobManager:
    def __init__(self):
        self._jobs: dict[str, S3JobState] = {}
        self._locks: dict[str, threading.Event] = {}
        self._mutex = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=2)

    def list_jobs(self):
        with self._mutex:
            return [asdict(job) for job in self._jobs.values()]

    def get_job(self, job_id: str):
        with self._mutex:
            job = self._jobs.get(job_id)
            return asdict(job) if job else None

    def start_job(self, *, job_id: str, dataset_id: str, source: str, bucket: str, prefix: str = '', limit: int = 100, concurrency: int = 4, source_filter: str | None = None, rows: list[dict[str, Any]], s3_client_factory=None, resolve_source_url=None, on_uploaded=None):
        with self._mutex:
            if job_id in self._jobs and self._jobs[job_id].status in {'queued', 'running'}:
                raise ValueError(f'Job already running: {job_id}')
            job = S3JobState(
                job_id=job_id,
                dataset_id=dataset_id,
                source=source,
                bucket=bucket,
                prefix=prefix,
                limit=max(1, int(limit)),
                concurrency=max(1, int(concurrency)),
                source_filter=source_filter,
                total=min(len(rows), max(1, int(limit))),
                status='running',
                started_at=time.time(),
                items=[],
            )
            self._jobs[job_id] = job
            self._locks[job_id] = threading.Event()

        future = self._executor.submit(
            self._run_job,
            job_id,
            rows[: job.limit],
            s3_client_factory,
            resolve_source_url,
            on_uploaded,
        )
        return future

    def cancel_job(self, job_id: str):
        with self._mutex:
            job = self._jobs.get(job_id)
            if not job:
                return False
            job.cancel_requested = True
            job.status = 'cancel_requested'
            event = self._locks.get(job_id)
            if event:
                event.set()
            return True

    def _run_job(self, job_id: str, rows: list[dict[str, Any]], s3_client_factory, resolve_source_url, on_uploaded):
        with self._mutex:
            job = self._jobs[job_id]
        try:
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
                            job.processed += 1
                            if isinstance(result, dict):
                                job.items = (job.items or []) + [result]
                                if result.get('status') == 'uploaded':
                                    job.uploaded += 1
                                elif result.get('status') == 'skipped':
                                    job.skipped += 1
                                else:
                                    job.failed += 1
                            elif result == 'uploaded':
                                job.uploaded += 1
                            elif result == 'skipped':
                                job.skipped += 1
                            else:
                                job.failed += 1
                    except Exception as exc:
                        with self._mutex:
                            job.processed += 1
                            job.failed += 1
                            job.last_message = str(exc)
                            job.items = (job.items or []) + [{
                                'status': 'failed',
                                'message': str(exc),
                                'timestamp': time.time(),
                            }]
            with self._mutex:
                if job.cancel_requested:
                    job.status = 'cancelled'
                else:
                    job.status = 'completed'
                job.ended_at = time.time()
        except Exception as exc:
            with self._mutex:
                job.status = 'failed'
                job.error = str(exc)
                job.ended_at = time.time()

    def _process_row(self, s3, job: S3JobState, row: dict[str, Any], resolve_source_url, on_uploaded):
        item = {
            'timestamp': time.time(),
            'dataset_id': job.dataset_id,
            'product_id': str(row.get('id') or ''),
            'goods_id': str(row.get('goods_id') or row.get('id') or ''),
            'name': row.get('name') or row.get('title') or '',
            'source_url': None,
            'key': None,
            'status': 'skipped',
            'message': None,
        }
        if job.cancel_requested:
            item['message'] = 'Cancelled before processing'
            return item
        raw_candidates = resolve_source_url(row) if callable(resolve_source_url) else None
        if isinstance(raw_candidates, (list, tuple)):
            candidates = [str(url).strip() for url in raw_candidates if isinstance(url, str) and url.strip() and str(url).strip().lower().startswith(('http://', 'https://'))]
        elif isinstance(raw_candidates, str) and raw_candidates.strip() and raw_candidates.strip().lower().startswith(('http://', 'https://')):
            candidates = [raw_candidates.strip()]
        else:
            candidates = []
        if not candidates:
            item['message'] = 'No source URL available'
            return item

        last_error = None
        for source_url in candidates:
            item['source_url'] = source_url
            key = self._build_key(job, row, source_url)
            item['key'] = key
            if self._object_exists(s3, job.bucket, key):
                item['status'] = 'skipped'
                item['message'] = 'Already exists on S3'
                return item
            try:
                content, content_type = self._download(source_url)
                if not content:
                    raise RuntimeError('Empty content')
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
                if callable(on_uploaded):
                    on_uploaded(row, source_url, key)
                item['status'] = 'uploaded'
                item['message'] = 'Uploaded successfully'
                return item
            except Exception as exc:
                last_error = exc
                item['message'] = f'{type(exc).__name__}: {exc}'
                continue

        item['status'] = 'failed'
        item['message'] = f'All candidate URLs failed: {last_error}' if last_error else 'All candidate URLs failed'
        return item

    def _download(self, url: str):
        req = Request(url, headers={
            'User-Agent': 'Mozilla/5.0',
            'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
            'Connection': 'close',
        })
        last_error = None
        for timeout_seconds in (15, 20):
            try:
                with urlopen(req, timeout=timeout_seconds) as resp:
                    data = resp.read()
                    content_type = resp.headers.get_content_type()
                return data, content_type
            except Exception as exc:
                last_error = exc
        raise last_error or RuntimeError('Download failed')

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

    def mark_saved(self, resource: dict[str, Any], s3_url: str | None = None):
        resource['saved_on_s3'] = bool(s3_url)
        resource['s3_url'] = s3_url
        return resource

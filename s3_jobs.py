from __future__ import annotations

import hashlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse
from urllib.request import Request, urlopen

try:
    import boto3
except Exception:  # pragma: no cover
    boto3 = None


ACTIVE_JOB_STATUSES = {'queued', 'running', 'cancel_requested'}
DEFAULT_HISTORY_LIMIT = 200


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


JOB_STATE_FIELDS = {field.name for field in fields(S3JobState)}


class S3JobManager:
    def __init__(
        self,
        store_path: str | Path | None = None,
        history_limit: int = DEFAULT_HISTORY_LIMIT,
        load_jobs_fn: Callable[[], list[dict[str, Any]]] | None = None,
        save_jobs_fn: Callable[[list[dict[str, Any]]], None] | None = None,
    ):
        self._jobs: dict[str, S3JobState] = {}
        self._locks: dict[str, threading.Event] = {}
        self._mutex = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=2)
        self._store_path = Path(store_path) if store_path else Path(__file__).resolve().parent / 's3_jobs_state.json'
        self._history_limit = max(10, int(history_limit or DEFAULT_HISTORY_LIMIT))
        self._load_jobs_fn = load_jobs_fn
        self._save_jobs_fn = save_jobs_fn
        self._load_jobs()

    def list_jobs(self):
        with self._mutex:
            return [asdict(job) for job in self._sorted_jobs_unlocked()]

    def get_job(self, job_id: str):
        with self._mutex:
            job = self._jobs.get(job_id)
            return asdict(job) if job else None

    def start_job(self, *, job_id: str, dataset_id: str, source: str, bucket: str, prefix: str = '', limit: int = 100, concurrency: int = 4, source_filter: str | None = None, rows: list[dict[str, Any]], s3_client_factory=None, resolve_source_url=None, on_uploaded=None):
        with self._mutex:
            if job_id in self._jobs and self._jobs[job_id].status in ACTIVE_JOB_STATUSES:
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
            self._persist_jobs_best_effort_unlocked()

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
            self._persist_jobs_best_effort_unlocked()
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
                                if job.items is None:
                                    job.items = []
                                job.items.append(result)
                                if result.get('status') == 'uploaded':
                                    job.uploaded += 1
                                elif result.get('status') == 'skipped':
                                    job.skipped += 1
                                else:
                                    job.failed += 1
                                job.last_message = result.get('message') or job.last_message
                            elif result == 'uploaded':
                                job.uploaded += 1
                            elif result == 'skipped':
                                job.skipped += 1
                            else:
                                job.failed += 1
                            self._persist_jobs_best_effort_unlocked()
                    except Exception as exc:
                        with self._mutex:
                            job.processed += 1
                            job.failed += 1
                            job.last_message = str(exc)
                            if job.items is None:
                                job.items = []
                            job.items.append({
                                'status': 'failed',
                                'message': str(exc),
                                'timestamp': time.time(),
                            })
                            self._persist_jobs_best_effort_unlocked()
            with self._mutex:
                if job.cancel_requested:
                    job.status = 'cancelled'
                else:
                    job.status = 'completed'
                job.ended_at = time.time()
                self._persist_jobs_best_effort_unlocked()
        except Exception as exc:
            with self._mutex:
                job.status = 'failed'
                job.error = str(exc)
                job.last_message = str(exc)
                job.ended_at = time.time()
                self._persist_jobs_best_effort_unlocked()

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
        candidates = list(dict.fromkeys(candidates))
        item['source_urls'] = candidates
        item['image_total'] = len(candidates)
        if candidates:
            item['source_url'] = candidates[0]
            item['key'] = self._build_key(job, row, candidates[0])
        if not candidates:
            item['message'] = 'No source URL available'
            return item

        last_error = None
        for source_url in candidates:
            key = self._build_key(job, row, source_url)
            s3_url = f's3://{job.bucket}/{key}'
            if self._object_exists(s3, job.bucket, key):
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
            try:
                content, content_type = self._download(source_url)
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
            except Exception as exc:
                last_error = exc
                item['image_failed'] += 1
                item['failures'].append({
                    'source_url': source_url,
                    'error': f'{type(exc).__name__}: {exc}',
                })

        item['saved_on_s3'] = bool(candidates) and item['image_failed'] == 0 and len(item['s3_urls']) == len(candidates)
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
        else:
            item['status'] = 'failed'
            if item['s3_urls']:
                item['message'] = f"Uploaded {len(item['s3_urls'])}/{len(candidates)} image(s); {item['image_failed']} failed"
            else:
                item['message'] = f'All product image uploads failed: {last_error}' if last_error else 'All product image uploads failed'

        if callable(on_uploaded):
            try:
                on_uploaded(row, item)
            except Exception as exc:
                item['status'] = 'failed'
                item['saved_on_s3'] = False
                item['message'] = f'Persistence error: {exc}'
        return item

    def _download(self, url: str):
        parsed = urlparse(url)
        referer = f'{parsed.scheme}://{parsed.netloc}/' if parsed.scheme and parsed.netloc else None
        hostname = (parsed.hostname or '').lower()
        if hostname.endswith('asos-media.com'):
            referer = 'https://www.asos.com/'
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

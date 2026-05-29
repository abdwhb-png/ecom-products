#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from s3_jobs import S3JobManager  # noqa: E402


class FakeFuture:
    def __init__(self, result):
        self._result = result

    def result(self):
        return self._result


class FakePool:
    def __init__(self, results):
        self._results = results

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def submit(self, fn, *args, **kwargs):
        return FakeFuture(next(self._results))


def main() -> int:
    manager = S3JobManager()
    job_id = 'unit-partial-accounting'
    manager._jobs[job_id] = manager._coerce_job({
        'job_id': job_id,
        'dataset_id': 'shein',
        'source': 'products',
        'limit': 3,
        'status': 'running',
        'processed': 0,
        'uploaded': 0,
        'skipped': 0,
        'failed': 0,
        'total': 3,
        'bucket': 'unit-bucket',
        'prefix': 'unit-prefix',
        'concurrency': 1,
        'items': [],
        'kind': 'upload',
    })

    results = iter([
        {'status': 'uploaded', 'message': 'Uploaded 3 image(s)'},
        {'status': 'partial', 'message': 'Partial success: 8/9 image(s) available on S3; 1 failed'},
        {'status': 'failed', 'message': 'All product image uploads failed'},
    ])

    original_thread_pool = sys.modules['s3_jobs'].ThreadPoolExecutor
    original_as_completed = sys.modules['s3_jobs'].as_completed
    original_boto3 = sys.modules['s3_jobs'].boto3

    def fake_as_completed(futures):
        return futures

    sys.modules['s3_jobs'].ThreadPoolExecutor = lambda max_workers=1: FakePool(results)  # type: ignore[assignment]
    sys.modules['s3_jobs'].as_completed = fake_as_completed  # type: ignore[assignment]
    sys.modules['s3_jobs'].boto3 = object()  # type: ignore[assignment]
    try:
        manager._run_job(
            job_id,
            rows=[{'id': '1'}, {'id': '2'}, {'id': '3'}],
            s3_client_factory=lambda: object(),
            resolve_source_url=lambda row: [],
            on_uploaded=None,
        )
    finally:
        sys.modules['s3_jobs'].ThreadPoolExecutor = original_thread_pool  # type: ignore[assignment]
        sys.modules['s3_jobs'].as_completed = original_as_completed  # type: ignore[assignment]
        sys.modules['s3_jobs'].boto3 = original_boto3  # type: ignore[assignment]

    job = manager.get_job(job_id)
    assert job is not None, 'job missing'
    assert job['uploaded'] == 2, job
    assert job['failed'] == 1, job
    assert job['processed'] == 3, job
    assert job['status'] == 'completed', job

    print('OK: partial product uploads count as successful job items while full failures still count as failed')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
from __future__ import annotations

import os
import socket
import sys
import tempfile
from pathlib import Path
from urllib.error import HTTPError, URLError

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import network_proxy  # noqa: E402
import s3_jobs  # noqa: E402
from s3_job_operations import collect_upload_candidates  # noqa: E402
from s3_jobs import S3JobManager  # noqa: E402


PROXY_KEYS = [
    'FAST_FASHION_PROXY_HOST',
    'FAST_FASHION_PROXY_PORT',
    'FAST_FASHION_PROXY_LOGIN',
    'FAST_FASHION_PROXY_PASSWORD',
]


class FakeJob:
    def __init__(self, dataset_id: str = 'asos'):
        self.dataset_id = dataset_id
        self.bucket = 'unit-test-bucket'
        self.prefix = 'unit-prefix'
        self.cancel_requested = False
        self.dry_run = False


class FakeS3:
    def head_object(self, Bucket: str, Key: str):
        raise RuntimeError('NotFound')

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str, Metadata: dict):
        return {'ETag': 'fake'}


def restore_env(previous: dict[str, str | None]) -> None:
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def set_proxy_env(enabled: bool) -> None:
    for key in PROXY_KEYS:
        os.environ.pop(key, None)
    if enabled:
        os.environ['FAST_FASHION_PROXY_HOST'] = 'gw.dataimpulse.com'
        os.environ['FAST_FASHION_PROXY_PORT'] = '823'
        os.environ['FAST_FASHION_PROXY_LOGIN'] = 'proxy-user'
        os.environ['FAST_FASHION_PROXY_PASSWORD'] = 'proxy-pass'


def make_context():
    return {
        'allowed_datasets': {'shein', 'asos'},
        'effective_s3_config': lambda: {'bucket': 'env-bucket', 'prefix': 'env-prefix', 'endpoint_url': None, 'region_name': None},
        'db_connect': lambda: (_ for _ in ()).throw(RuntimeError('db_connect should not be called in this unit test')),
    }


def main() -> int:
    previous = {key: os.environ.get(key) for key in PROXY_KEYS}
    original_sleep = s3_jobs.time.sleep
    original_build_urllib_opener = network_proxy.build_urllib_opener
    try:
        set_proxy_env(False)
        context = make_context()
        original_db_connect = context['db_connect']

        class FakeConn:
            def execute(self, *_args, **_kwargs):
                class FakeResult:
                    def fetchall(self_inner):
                        return [{'id': '2001'}]
                return FakeResult()

            def close(self):
                return None

        context['db_connect'] = lambda: FakeConn()
        collected = collect_upload_candidates({'dataset_id': 'asos', 'limit': 1, 'concurrency': 9}, context)
        assert collected['concurrency'] == 2, collected
        context['db_connect'] = original_db_connect

        set_proxy_env(True)
        context = make_context()
        context['db_connect'] = lambda: FakeConn()
        collected = collect_upload_candidates({'dataset_id': 'asos', 'limit': 1, 'concurrency': 9}, context)
        assert collected['concurrency'] == 4, collected

        sleeps: list[float] = []
        s3_jobs.time.sleep = lambda seconds: sleeps.append(seconds)  # type: ignore[assignment]

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = S3JobManager(store_path=Path(tmpdir) / 'jobs.json')
            fake_s3 = FakeS3()
            calls: list[tuple[str, int, bool]] = []
            attempts = {'count': 0}

            def fake_download(url: str):
                attempts['count'] += 1
                return manager._download(url)

            class FakeHeaders:
                def get_content_type(self):
                    return 'image/jpeg'

            class FakeResp:
                headers = FakeHeaders()

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

                def read(self, size=-1):
                    if getattr(self, '_done', False):
                        return b''
                    self._done = True
                    return b'image-bytes'

            class FakeOpener:
                def open(self, request, timeout=0):
                    calls.append((request.full_url, timeout, 'Referer' in dict(request.header_items())))
                    if timeout == 10:
                        raise HTTPError(request.full_url, 403, 'Forbidden', hdrs=None, fp=None)
                    if timeout == 20:
                        raise socket.timeout('timed out')
                    return FakeResp()

            network_proxy.build_urllib_opener = lambda **_kwargs: FakeOpener()  # type: ignore[assignment]
            row = {
                'id': '2001',
                'name': 'ASOS proxy retry test',
                'image': 'https://images.asos-media.com/products/test/2001-1?$n_1920w$&wid=1926&fit=constrain',
                'image_urls_json': '[]',
            }
            item = manager._process_row(
                fake_s3,
                FakeJob('asos'),
                row,
                lambda _row: [row['image']],
                None,
            )

            assert item['status'] == 'uploaded', item
            assert item['image_uploaded'] == 1, item
            assert sleeps == [1, 3], sleeps
            assert [timeout for _url, timeout, _has_referer in calls] == [10, 20, 30], calls
            assert all(has_referer for _url, _timeout, has_referer in calls), calls
            assert item.get('download_stats', {}).get('proxy_mode') == 'proxy', item
            assert item.get('download_stats', {}).get('attempt_count') == 3, item

            manager._create_job_unlocked(
                job_id='stats-job',
                dataset_id='asos',
                source='products',
                bucket='unit-test-bucket',
                prefix='unit-prefix',
                limit=1,
                concurrency=1,
                source_filter=None,
                total=1,
                job_family='upload',
                dry_run=False,
            )
            job_state = manager._jobs['stats-job']
            manager._record_item_unlocked(job_state, item)
            by_host = job_state.download_stats['by_host']['images.asos-media.com']['proxy_mode']['proxy']
            assert by_host['success'] == 1, job_state.download_stats

            # Final failure path
            sleeps.clear()
            calls.clear()
            class AlwaysTimeoutOpener:
                def open(self, request, timeout=0):
                    calls.append((request.full_url, timeout, 'Referer' in dict(request.header_items())))
                    raise URLError(socket.timeout('timed out'))

            network_proxy.build_urllib_opener = lambda **_kwargs: AlwaysTimeoutOpener()  # type: ignore[assignment]
            failed = manager._process_row(
                fake_s3,
                FakeJob('asos'),
                row,
                lambda _row: [row['image']],
                None,
            )

            assert failed['status'] == 'failed', failed
            assert failed['image_failed'] == 1, failed
            assert failed['failures'], failed
            failure = failed['failures'][0]
            assert failure['proxy_used'] is True, failure
            assert failure['attempt_count'] == 3, failure
            assert failure['timeout_seconds'] == 30, failure
            assert failure['error_type'] in {'timeout', 'network_error'}, failure
            assert sleeps == [1, 3], sleeps

            manager._create_job_unlocked(
                job_id='stats-job-fail',
                dataset_id='asos',
                source='products',
                bucket='unit-test-bucket',
                prefix='unit-prefix',
                limit=1,
                concurrency=1,
                source_filter=None,
                total=1,
                job_family='upload',
                dry_run=False,
            )
            fail_job_state = manager._jobs['stats-job-fail']
            manager._record_item_unlocked(fail_job_state, failed)
            fail_by_host = fail_job_state.download_stats['by_host']['images.asos-media.com']['proxy_mode']['proxy']
            assert fail_by_host[failure['error_type']] == 1, fail_job_state.download_stats
    finally:
        s3_jobs.time.sleep = original_sleep  # type: ignore[assignment]
        network_proxy.build_urllib_opener = original_build_urllib_opener  # type: ignore[assignment]
        restore_env(previous)

    print('OK: ASOS proxy-aware retry policy enforces concurrency, timeouts, backoff, and safe failure metadata')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

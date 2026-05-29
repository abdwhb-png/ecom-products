#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import s3_jobs  # noqa: E402
from s3_jobs import S3JobManager  # noqa: E402


class FakeS3:
    def __init__(self):
        self.objects: dict[str, dict] = {}

    def head_object(self, Bucket: str, Key: str):
        if Key not in self.objects:
            raise RuntimeError('NotFound')
        return self.objects[Key]

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str, Metadata: dict):
        self.objects[Key] = {
            'Bucket': Bucket,
            'Key': Key,
            'Body': Body,
            'ContentType': ContentType,
            'Metadata': Metadata,
        }
        return {'ETag': 'fake'}


URLS = [
    'https://example.test/main.jpg',
    'https://example.test/alt-1.jpg',
]


def resolve_source_url(_row):
    return URLS


def main() -> int:
    with tempfile.TemporaryDirectory() as tmpdir:
        store_path = Path(tmpdir) / 's3_jobs_state.json'

        original_boto3 = s3_jobs.boto3
        s3_jobs.boto3 = object()
        try:
            manager = S3JobManager(store_path=store_path, history_limit=50)
            fake_s3 = FakeS3()
            payloads = {url: (f'bytes-for:{url}'.encode('utf-8'), 'image/jpeg') for url in URLS}
            downloads: list[str] = []

            def fake_download(url: str):
                downloads.append(url)
                return payloads[url]

            manager._download = fake_download  # type: ignore[method-assign]

            row = {
                'id': 'product-1',
                'name': 'Persistent job test product',
                'image': URLS[0],
                'image_urls_json': json.dumps(URLS),
            }

            future = manager.start_job(
                job_id='shein-history-1',
                dataset_id='shein',
                source='products',
                bucket='unit-test-bucket',
                prefix='unit-history',
                limit=10,
                concurrency=2,
                rows=[row],
                s3_client_factory=lambda: fake_s3,
                resolve_source_url=resolve_source_url,
            )
            future.result(timeout=10)

            persisted = json.loads(store_path.read_text(encoding='utf-8'))
            assert isinstance(persisted.get('jobs'), list) and persisted['jobs'], persisted
            persisted_job = persisted['jobs'][0]
            assert persisted_job['job_id'] == 'shein-history-1', persisted_job
            assert persisted_job['status'] == 'completed', persisted_job
            assert persisted_job['processed'] == 1, persisted_job
            assert persisted_job['uploaded'] == 1, persisted_job
            assert persisted_job['job_family'] == 'upload', persisted_job
            assert persisted_job['dry_run'] is False, persisted_job
            assert len(persisted_job['items']) == 1, persisted_job
            assert downloads == URLS, downloads

            reloaded = S3JobManager(store_path=store_path, history_limit=50)
            reloaded_job = reloaded.get_job('shein-history-1')
            assert reloaded_job is not None, reloaded.list_jobs()
            assert reloaded_job['status'] == 'completed', reloaded_job
            assert reloaded_job['uploaded'] == 1, reloaded_job
            assert reloaded_job['job_family'] == 'upload', reloaded_job
            assert reloaded_job['dry_run'] is False, reloaded_job

            legacy_payload = {
                'jobs': [
                    {
                        'job_id': 'legacy-migration-preview',
                        'dataset_id': 'all',
                        'source': 'migration',
                        'limit': 5,
                        'status': 'completed',
                        'processed': 2,
                        'uploaded': 2,
                        'skipped': 0,
                        'failed': 0,
                        'total': 2,
                        'started_at': 100.0,
                        'ended_at': 101.0,
                        'concurrency': 1,
                        'items': [{'status': 'preview'}],
                        'kind': 'migration_preview',
                    },
                    {
                        'job_id': 'legacy-cleanup',
                        'dataset_id': 'all',
                        'source': 'cleanup',
                        'limit': 5,
                        'status': 'completed',
                        'processed': 1,
                        'uploaded': 1,
                        'skipped': 0,
                        'failed': 0,
                        'total': 1,
                        'started_at': 102.0,
                        'ended_at': 103.0,
                        'concurrency': 1,
                        'items': [{'status': 'uploaded'}],
                        'kind': 'cleanup',
                    },
                    {
                        'job_id': 'legacy-upload',
                        'dataset_id': 'shein',
                        'source': 'products',
                        'limit': 5,
                        'status': 'completed',
                        'processed': 1,
                        'uploaded': 1,
                        'skipped': 0,
                        'failed': 0,
                        'total': 1,
                        'started_at': 104.0,
                        'ended_at': 105.0,
                        'concurrency': 1,
                        'items': [{'status': 'uploaded'}],
                        'kind': 'upload',
                    },
                ]
            }
            store_path.write_text(json.dumps(legacy_payload), encoding='utf-8')
            legacy_manager = S3JobManager(store_path=store_path, history_limit=50)
            migration_preview = legacy_manager.get_job('legacy-migration-preview')
            cleanup_job = legacy_manager.get_job('legacy-cleanup')
            upload_job = legacy_manager.get_job('legacy-upload')
            assert migration_preview['job_family'] == 'url_migration', migration_preview
            assert migration_preview['dry_run'] is True, migration_preview
            assert cleanup_job['job_family'] == 'state_cleanup', cleanup_job
            assert cleanup_job['dry_run'] is False, cleanup_job
            assert upload_job['job_family'] == 'upload', upload_job
            assert upload_job['dry_run'] is False, upload_job

            interrupted_payload = {
                'jobs': [
                    {
                        'job_id': 'shein-history-2',
                        'dataset_id': 'shein',
                        'source': 'products',
                        'limit': 25,
                        'status': 'running',
                        'processed': 7,
                        'uploaded': 5,
                        'skipped': 1,
                        'failed': 1,
                        'total': 25,
                        'started_at': 1234567890.0,
                        'ended_at': None,
                        'bucket': 'unit-test-bucket',
                        'prefix': 'unit-history',
                        'concurrency': 4,
                        'items': [{'status': 'uploaded', 'timestamp': 1234567891.0}],
                    }
                ]
            }
            store_path.write_text(json.dumps(interrupted_payload), encoding='utf-8')
            interrupted_manager = S3JobManager(store_path=store_path, history_limit=50)
            interrupted_job = interrupted_manager.get_job('shein-history-2')
            assert interrupted_job is not None, interrupted_manager.list_jobs()
            assert interrupted_job['status'] == 'interrupted', interrupted_job
            assert interrupted_job['ended_at'] is not None, interrupted_job
            assert 'Server restarted before this job completed' in (interrupted_job['last_message'] or ''), interrupted_job
        finally:
            s3_jobs.boto3 = original_boto3

    print('OK: S3 job history persists across reloads and running jobs become interrupted after restart')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

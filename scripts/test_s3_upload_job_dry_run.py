#!/usr/bin/env python3
from __future__ import annotations

import json
import tempfile
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from s3_jobs import S3JobManager  # noqa: E402


class FakeS3:
    def __init__(self):
        self.head_calls = []
        self.put_calls = []

    def head_object(self, Bucket: str, Key: str):
        self.head_calls.append((Bucket, Key))
        raise RuntimeError('NotFound')

    def put_object(self, **kwargs):
        self.put_calls.append(kwargs)
        return {'ETag': 'fake'}


def main() -> int:
    with tempfile.TemporaryDirectory() as tmpdir:
        store_path = Path(tmpdir) / 's3_jobs_state.json'
        manager = S3JobManager(store_path=store_path)
        fake_s3 = FakeS3()
        downloads = []
        persisted = []

        def fake_download(url: str):
            downloads.append(url)
            return (b'img-bytes', 'image/jpeg')

        manager._download = fake_download  # type: ignore[method-assign]
        row = {
            'id': 'product-1',
            'goods_id': 'shein:product-1',
            'name': 'Dry run upload product',
            'image': 'https://example.test/main.jpg',
            'image_urls_json': json.dumps(['https://example.test/main.jpg', 'https://example.test/alt.jpg']),
        }

        future = manager.start_job(
            job_id='upload-dry-run-1',
            dataset_id='shein',
            source='products',
            bucket='unit-test-bucket',
            prefix='dry-run',
            limit=10,
            concurrency=1,
            rows=[row],
            s3_client_factory=lambda: fake_s3,
            resolve_source_url=lambda _row: ['https://example.test/main.jpg', 'https://example.test/alt.jpg'],
            on_uploaded=lambda _row, item: persisted.append(item),
            dry_run=True,
            job_family='upload',
        )
        future.result(timeout=10)

        job = manager.get_job('upload-dry-run-1')
        assert job is not None, manager.list_jobs()
        assert job['job_family'] == 'upload', job
        assert job['dry_run'] is True, job
        assert 'download_stats' in job, job
        assert job['status'] == 'completed', job
        assert job['uploaded'] == 1, job
        assert job['failed'] == 0, job
        assert fake_s3.put_calls == [], fake_s3.put_calls
        assert downloads == [], downloads
        assert persisted == [], persisted
        assert job['items'], job
        first_item = job['items'][0]
        assert first_item['status'] == 'preview', first_item
        assert first_item['image_uploaded'] == 2, first_item
        assert first_item['saved_on_s3'] is True, first_item

    print('OK: upload dry-run records preview items without S3 or SQLite writes')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

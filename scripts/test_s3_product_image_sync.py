#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

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


class FakeJob:
    dataset_id = 'shein'
    bucket = 'unit-test-bucket'
    prefix = 'unit-sync'
    cancel_requested = False


URLS = [
    'https://example.test/main.jpg',
    'https://example.test/alt-1.jpg',
    'https://example.test/alt-2.jpg',
]


def resolve_source_url(_row):
    return [URLS[0], *URLS, URLS[1]]


def main() -> int:
    row = {
        'id': 'product-123',
        'name': 'Unit test product',
        'image': URLS[0],
        'image_urls_json': json.dumps(URLS),
    }

    manager = S3JobManager()
    s3 = FakeS3()
    downloads: list[str] = []
    captured: list[dict] = []

    payloads = {url: (f'bytes-for:{url}'.encode('utf-8'), 'image/jpeg') for url in URLS}

    def fake_download(url: str, dataset_id=None):
        downloads.append(url)
        return payloads[url], 'image/jpeg', {'hostname': 'example.test', 'proxy_used': False, 'proxy_mode': 'direct', 'attempt_count': 1, 'timeout_seconds': 20, 'error_type': None, 'http_status': None}

    manager._download = fake_download  # type: ignore[method-assign]

    def on_processed(_row, item):
        captured.append(item)

    result = manager._process_row(s3, FakeJob(), row, resolve_source_url, on_processed)

    assert result['status'] == 'uploaded', result
    assert result['image_total'] == 3, result
    assert result['image_uploaded'] == 3, result
    assert result['image_failed'] == 0, result
    assert result['saved_on_s3'] is True, result
    assert result['source_urls'] == URLS, result
    assert len(result['s3_urls']) == 3, result
    assert len(result['s3_keys']) == 3, result
    assert downloads == URLS, downloads
    assert len(captured) == 1, captured
    assert captured[0]['saved_on_s3'] is True, captured[0]
    assert captured[0]['download_stats']['proxy_mode'] == 'direct', captured[0]

    partial_manager = S3JobManager()
    partial_s3 = FakeS3()
    partial_downloads: list[str] = []

    def fake_partial_download(url: str, dataset_id=None):
        partial_downloads.append(url)
        if url == URLS[1]:
            raise RuntimeError('forced failure for alt-1')
        return payloads[url], 'image/jpeg', {'hostname': 'example.test', 'proxy_used': False, 'proxy_mode': 'direct', 'attempt_count': 1, 'timeout_seconds': 20, 'error_type': None, 'http_status': None}

    partial_manager._download = fake_partial_download  # type: ignore[method-assign]
    partial_result = partial_manager._process_row(partial_s3, FakeJob(), row, resolve_source_url, None)

    assert partial_result['status'] == 'partial', partial_result
    assert partial_result['image_total'] == 3, partial_result
    assert partial_result['image_uploaded'] == 2, partial_result
    assert partial_result['image_failed'] == 1, partial_result
    assert partial_result['saved_on_s3'] is False, partial_result
    assert len(partial_result['s3_urls']) == 2, partial_result
    assert 'Partial success' in str(partial_result['message']), partial_result
    assert partial_downloads == URLS, partial_downloads
    assert partial_result['download_stats']['proxy_mode'] == 'direct', partial_result

    print('OK: all product images are aggregated, and partial uploads stay visible without being counted as fully saved')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

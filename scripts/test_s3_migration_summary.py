#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import server  # noqa: E402


def override_server_db_path(db_path: Path):
    os.environ['FAST_FASHION_DB_PATH'] = str(db_path)
    server.DB_PATH = db_path


def seed_db(db_path: Path):
    override_server_db_path(db_path)
    conn = server.db_connect()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO s3_objects (goods_id, dataset_id, product_id, source_url, s3_url, bucket, object_key, source_image_urls_json, s3_image_urls_json, image_pairs_json, source_image_count, s3_image_count, failed_image_count, saved_on_s3, saved_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                'shein:sum-1', 'shein', 'sum-1', 'https://img.example/1.jpg', 's3://ecom-products/shein/sum-1/main.jpg',
                'ecom-products', 'shein/sum-1/main.jpg', json.dumps(['https://img.example/1.jpg']), json.dumps(['s3://ecom-products/shein/sum-1/main.jpg']),
                json.dumps([{'source_url': 'https://img.example/1.jpg', 's3_url': 's3://ecom-products/shein/sum-1/main.jpg', 'key': 'shein/sum-1/main.jpg', 'status': 'uploaded'}]),
                1, 1, 0, 1, 1.0, 1.0,
            ),
        )
        conn.execute(
            "INSERT OR REPLACE INTO s3_objects (goods_id, dataset_id, product_id, source_url, s3_url, bucket, object_key, source_image_urls_json, s3_image_urls_json, image_pairs_json, source_image_count, s3_image_count, failed_image_count, saved_on_s3, saved_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                'shein:sum-2', 'shein', 'sum-2', 'https://img.example/2.jpg', 'https://pub.example.dev/shein/sum-2/main.jpg',
                'ecom-products', 'shein/sum-2/main.jpg', json.dumps(['https://img.example/2.jpg']), json.dumps(['https://pub.example.dev/shein/sum-2/main.jpg']),
                json.dumps([{'source_url': 'https://img.example/2.jpg', 's3_url': 'https://pub.example.dev/shein/sum-2/main.jpg', 'key': 'shein/sum-2/main.jpg', 'status': 'uploaded'}]),
                1, 1, 0, 1, 1.0, 1.0,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def main() -> int:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'catalog.db'
        os.environ['AWS_URL'] = 'https://pub.example.dev'
        override_server_db_path(db_path)
        seed_db(db_path)

        class FakeHeaders(dict):
            def get(self, key, default=None):
                return super().get(key, default)

        class FakeHandler(server.Handler):
            def __init__(self):
                pass
            def send_response(self, *args, **kwargs):
                self._status = args[0]
            def send_header(self, *args, **kwargs):
                pass
            def end_headers(self):
                pass

        handler = FakeHandler.__new__(FakeHandler)
        handler.headers = FakeHeaders({})
        handler.wfile = type('W', (), {'write': lambda self, b: setattr(handler, '_body', b)})()
        server.Handler.handle_s3_migration_summary(handler)
        payload = json.loads(handler._body.decode('utf-8'))
        data = payload['data']
        assert data['total'] == 1, data
        assert data['sample'][0]['goods_id'] == 'shein:sum-1', data
        assert data['sample'][0]['new_s3_url'] == 'https://pub.example.dev/shein/sum-1/main.jpg', data

    print('OK: migration summary reports impacted item count and sample URLs')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

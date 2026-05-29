#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sqlite3
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
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS s3_objects (
                goods_id TEXT PRIMARY KEY,
                dataset_id TEXT NOT NULL,
                product_id TEXT NOT NULL,
                source_url TEXT,
                s3_url TEXT,
                bucket TEXT,
                object_key TEXT,
                source_image_urls_json TEXT NOT NULL DEFAULT '[]',
                s3_image_urls_json TEXT NOT NULL DEFAULT '[]',
                image_pairs_json TEXT NOT NULL DEFAULT '[]',
                source_image_count INTEGER NOT NULL DEFAULT 0,
                s3_image_count INTEGER NOT NULL DEFAULT 0,
                failed_image_count INTEGER NOT NULL DEFAULT 0,
                saved_on_s3 INTEGER NOT NULL DEFAULT 0,
                saved_at REAL,
                updated_at REAL NOT NULL
            )
            '''
        )
        conn.execute(
            "INSERT OR REPLACE INTO s3_objects (goods_id, dataset_id, product_id, source_url, s3_url, bucket, object_key, source_image_urls_json, s3_image_urls_json, image_pairs_json, source_image_count, s3_image_count, failed_image_count, saved_on_s3, saved_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                'shein:cleanup-summary-1', 'shein', 'cleanup-summary-1', 'https://img.example/source.jpg', 'https://pub.old.dev/old-prefix/shein/cleanup-summary-1/main.jpg',
                'old-bucket', 'old-prefix/shein/cleanup-summary-1/main.jpg', json.dumps(['https://img.example/source.jpg']), json.dumps(['https://pub.old.dev/old-prefix/shein/cleanup-summary-1/main.jpg']),
                json.dumps([{'source_url': 'https://img.example/source.jpg', 's3_url': 'https://pub.old.dev/old-prefix/shein/cleanup-summary-1/main.jpg', 'key': 'old-prefix/shein/cleanup-summary-1/main.jpg', 'status': 'uploaded'}]),
                1, 1, 0, 1, 1.0, 1.0,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def main() -> int:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'catalog.db'
        os.environ['AWS_BUCKET'] = 'new-bucket'
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
        server.Handler.handle_s3_family_summary(handler, 'state_cleanup')
        payload = json.loads(handler._body.decode('utf-8'))
        data = payload['data']
        assert data['total'] == 1, data
        assert data['current_bucket'] == 'new-bucket', data
        assert data['sample'][0]['goods_id'] == 'shein:cleanup-summary-1', data
        assert data['sample'][0]['reason'] == 'bucket_mismatch', data

    print('OK: cleanup summary reports stale saved_on_s3 records and current bucket context')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

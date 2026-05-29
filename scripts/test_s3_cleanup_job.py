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
                'shein:cleanup-1', 'shein', 'cleanup-1', 'https://img.example/source.jpg', 'https://pub.old.dev/old-prefix/shein/cleanup-1/main.jpg',
                'old-bucket', 'old-prefix/shein/cleanup-1/main.jpg', json.dumps(['https://img.example/source.jpg']), json.dumps(['https://pub.old.dev/old-prefix/shein/cleanup-1/main.jpg']),
                json.dumps([{'source_url': 'https://img.example/source.jpg', 's3_url': 'https://pub.old.dev/old-prefix/shein/cleanup-1/main.jpg', 'key': 'old-prefix/shein/cleanup-1/main.jpg', 'status': 'uploaded'}]),
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

        conn = server.db_connect()
        try:
            changes = server.s3_cleanup_collect(conn, bucket=os.environ['AWS_BUCKET'])
            assert len(changes) == 1, changes
            assert changes[0]['goods_id'] == 'shein:cleanup-1', changes
        finally:
            conn.close()

        conn2 = server.db_connect()
        try:
            local_changes = server.s3_cleanup_collect(conn2, bucket=os.environ['AWS_BUCKET'])
            assert len(local_changes) == 1, local_changes
            backup_path = Path(tmpdir) / 'cleanup-backup.json'
            server.s3_cleanup_write_backup(conn2, backup_path)
            seen = []

            def on_progress(change):
                seen.append(change['goods_id'])

            updated = server.s3_cleanup_apply(conn2, local_changes, updated_at=2.0, progress_cb=on_progress)
            assert updated == 1, updated
            assert seen == ['shein:cleanup-1'], seen
            assert backup_path.exists(), backup_path
        finally:
            conn2.close()

        conn3 = server.db_connect()
        try:
            row = conn3.execute("SELECT s3_url, s3_image_urls_json, image_pairs_json, s3_image_count, failed_image_count, saved_on_s3, saved_at FROM s3_objects WHERE goods_id = ?", ('shein:cleanup-1',)).fetchone()
            assert row is not None
            assert row['s3_url'] is None, row
            assert json.loads(row['s3_image_urls_json']) == [], row['s3_image_urls_json']
            assert json.loads(row['image_pairs_json']) == [], row['image_pairs_json']
            assert row['s3_image_count'] == 0, row
            assert row['failed_image_count'] == 0, row
            assert row['saved_on_s3'] == 0, row
            assert row['saved_at'] is None, row
        finally:
            conn3.close()

    print('OK: stale S3 cleanup clears persisted saved_on_s3 state and produces a backup')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

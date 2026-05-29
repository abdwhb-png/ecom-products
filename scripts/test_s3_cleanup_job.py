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


def run_preview_job(tmpdir: str) -> dict:
    job_id = 'cleanup-preview-test'

    def runner(record_item, cancel_event):
        conn = server.db_connect()
        try:
            changes = server.s3_cleanup_collect(conn, bucket=os.environ['AWS_BUCKET'])
            for change in changes:
                if cancel_event.is_set():
                    raise RuntimeError('cancelled')
                record_item({
                    'status': 'preview',
                    'message': 'Preview stale S3-state cleanup item',
                    'timestamp': 1.0,
                    'goods_id': change['goods_id'],
                    'reason': change['reason'],
                })
        finally:
            conn.close()

    future = server.S3_JOB_MANAGER.start_custom_job(
        job_id=job_id,
        dataset_id='all',
        source='cleanup',
        total=1,
        runner=runner,
        concurrency=1,
        limit=1,
        dry_run=True,
        job_family='state_cleanup',
    )
    future.result(timeout=10)
    return server.S3_JOB_MANAGER.get_job(job_id)


def run_write_job(tmpdir: str) -> dict:
    job_id = 'cleanup-write-test'

    def runner(record_item, cancel_event):
        conn = server.db_connect()
        try:
            local_changes = server.s3_cleanup_collect(conn, bucket=os.environ['AWS_BUCKET'])
            backup_path = Path(tmpdir) / 'cleanup-backup.json'
            server.s3_cleanup_write_backup(conn, backup_path)
            for change in local_changes:
                if cancel_event.is_set():
                    raise RuntimeError('Cleanup cancelled')
                server.s3_cleanup_apply(conn, [change], updated_at=2.0)
                record_item({
                    'status': 'uploaded',
                    'message': 'Cleared stale saved_on_s3 state',
                    'timestamp': 2.0,
                    'goods_id': change['goods_id'],
                    'reason': change['reason'],
                    'backup_path': str(backup_path),
                })
        finally:
            conn.close()

    future = server.S3_JOB_MANAGER.start_custom_job(
        job_id=job_id,
        dataset_id='all',
        source='cleanup',
        total=1,
        runner=runner,
        concurrency=1,
        limit=1,
        dry_run=False,
        job_family='state_cleanup',
    )
    future.result(timeout=10)
    return server.S3_JOB_MANAGER.get_job(job_id)


def main() -> int:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'catalog.db'
        os.environ['AWS_BUCKET'] = 'new-bucket'
        override_server_db_path(db_path)
        seed_db(db_path)

        preview_job = run_preview_job(tmpdir)
        assert preview_job is not None
        assert preview_job['job_family'] == 'state_cleanup', preview_job
        assert preview_job['dry_run'] is True, preview_job
        assert preview_job['uploaded'] == 1, preview_job
        assert preview_job['items'][0]['status'] == 'preview', preview_job

        conn = server.db_connect()
        try:
            row = conn.execute("SELECT saved_on_s3 FROM s3_objects WHERE goods_id = ?", ('shein:cleanup-1',)).fetchone()
            assert row['saved_on_s3'] == 1, row
        finally:
            conn.close()

        write_job = run_write_job(tmpdir)
        assert write_job is not None
        assert write_job['job_family'] == 'state_cleanup', write_job
        assert write_job['dry_run'] is False, write_job
        assert write_job['uploaded'] == 1, write_job
        assert write_job['items'][0]['backup_path'].endswith('cleanup-backup.json'), write_job

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

    print('OK: stale S3 cleanup dry-run and write mode both expose canonical family metadata')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

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
                'shein:123',
                'shein',
                '123',
                'https://img.example/source.jpg',
                's3://ecom-products/shein/123/main.jpg',
                'ecom-products',
                'shein/123/main.jpg',
                json.dumps(['https://img.example/source.jpg']),
                json.dumps(['s3://ecom-products/shein/123/main.jpg']),
                json.dumps([{'source_url': 'https://img.example/source.jpg', 's3_url': 's3://ecom-products/shein/123/main.jpg', 'key': 'shein/123/main.jpg', 'status': 'uploaded'}]),
                1,
                1,
                0,
                1,
                1.0,
                1.0,
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

        conn = server.db_connect()
        try:
            changes = server.migrate_collect_changes(conn, os.environ['AWS_URL'])
            assert len(changes) == 1, changes
        finally:
            conn.close()

        job_id = 'migration-test-1'

        def runner(record_item, cancel_event):
            conn2 = server.db_connect()
            try:
                local_changes = server.migrate_collect_changes(conn2, os.environ['AWS_URL'])
                backup_path = Path(tmpdir) / 'backup.json'
                server.migrate_write_backup(conn2, backup_path)

                for change in local_changes:
                    assert not cancel_event.is_set()
                    conn2.execute(
                        "UPDATE s3_objects SET s3_url = ?, s3_image_urls_json = ?, image_pairs_json = ?, updated_at = ? WHERE goods_id = ?",
                        (
                            change['new_s3_url'],
                            json.dumps(change['new_urls'], ensure_ascii=False),
                            json.dumps(change['new_pairs'], ensure_ascii=False),
                            2.0,
                            change['goods_id'],
                        ),
                    )
                    conn2.commit()
                    record_item({
                        'status': 'uploaded',
                        'message': 'Migrated stored URLs to AWS_URL',
                        'timestamp': 1.0,
                        'goods_id': change['goods_id'],
                        'old_s3_url': change['old_s3_url'],
                        'new_s3_url': change['new_s3_url'],
                        'changed_fields': change['changed_fields'],
                        'backup_path': str(backup_path),
                    })
            finally:
                conn2.close()

        future = server.S3_JOB_MANAGER.start_custom_job(
            job_id=job_id,
            dataset_id='all',
            source='migration',
            kind='migration',
            total=1,
            runner=runner,
            concurrency=1,
            limit=1,
        )
        future.result(timeout=10)
        job = server.S3_JOB_MANAGER.get_job(job_id)
        assert job is not None, server.S3_JOB_MANAGER.list_jobs()
        assert job['status'] == 'completed', job
        assert job['processed'] == 1, job
        assert job['uploaded'] == 1, job
        assert job['kind'] == 'migration', job

        conn3 = server.db_connect()
        try:
            row = conn3.execute("SELECT s3_url, s3_image_urls_json, image_pairs_json FROM s3_objects WHERE goods_id = ?", ('shein:123',)).fetchone()
            assert row is not None
            assert row['s3_url'] == 'https://pub.example.dev/shein/123/main.jpg', row['s3_url']
            assert json.loads(row['s3_image_urls_json']) == ['https://pub.example.dev/shein/123/main.jpg']
            pair = json.loads(row['image_pairs_json'])[0]
            assert pair['s3_url'] == 'https://pub.example.dev/shein/123/main.jpg', pair
        finally:
            conn3.close()

    print('OK: migration background job rewrites stored URLs and records job history')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

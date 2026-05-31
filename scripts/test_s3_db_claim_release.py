#!/usr/bin/env python3
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from s3_jobs import S3JobManager  # noqa: E402


class ImmediateFuture:
    def __init__(self, fn, args, kwargs):
        self._fn = fn
        self._args = args
        self._kwargs = kwargs

    def result(self, timeout=None):
        return self._fn(*self._args, **self._kwargs)


class InlineExecutor:
    def submit(self, fn, *args, **kwargs):
        return ImmediateFuture(fn, args, kwargs)


def main() -> int:
    with tempfile.TemporaryDirectory() as tmpdir:
        store_path = Path(tmpdir) / 's3_jobs_state.json'
        db_path = Path(tmpdir) / 'claims.db'

        def db_connect():
            conn = sqlite3.connect(db_path, timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute('PRAGMA busy_timeout = 30000')
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS s3_jobs (
                    job_id TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    started_at REAL,
                    updated_at REAL NOT NULL
                )
                '''
            )
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS s3_job_claims (
                    dataset_id TEXT NOT NULL,
                    product_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    claimed_at REAL NOT NULL,
                    PRIMARY KEY (dataset_id, product_id)
                )
                '''
            )
            conn.commit()
            return conn

        manager = S3JobManager(store_path=store_path, db_connect_fn=db_connect)
        manager._executor = InlineExecutor()  # type: ignore[assignment]

        row = {
            'id': 'prod-1',
            'goods_id': 'shein:prod-1',
            'name': 'Claim release product',
            'image': 'https://example.test/main.jpg',
            'image_urls_json': json.dumps(['https://example.test/main.jpg']),
        }

        reserved_first = manager.reserve_upload_candidates(
            job_id='job-1',
            dataset_id='shein',
            collector=lambda excluded: {
                'dataset_id': 'shein',
                'rows': [] if 'prod-1' in excluded else [row],
                'limit': 1,
            },
        )
        assert [item['id'] for item in reserved_first['rows']] == ['prod-1'], reserved_first

        manager.start_job(
            job_id='job-1',
            dataset_id='shein',
            source='products',
            bucket='unit-bucket',
            prefix='',
            limit=1,
            concurrency=1,
            rows=reserved_first['rows'],
            resolve_source_url=lambda _row: ['https://example.test/main.jpg'],
            dry_run=True,
        ).result(timeout=10)

        conn = db_connect()
        try:
            remaining = conn.execute('SELECT count(*) FROM s3_job_claims WHERE job_id = ?', ('job-1',)).fetchone()[0]
        finally:
            conn.close()
        assert remaining == 0, remaining

        reserved_second = manager.reserve_upload_candidates(
            job_id='job-2',
            dataset_id='shein',
            collector=lambda excluded: {
                'dataset_id': 'shein',
                'rows': [] if 'prod-1' in excluded else [row],
                'limit': 1,
            },
        )
        assert [item['id'] for item in reserved_second['rows']] == ['prod-1'], reserved_second

    print('OK: completed DB-backed upload jobs release product claims for later concurrent launches')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

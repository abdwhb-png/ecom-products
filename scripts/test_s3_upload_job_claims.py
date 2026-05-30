#!/usr/bin/env python3
from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from s3_jobs import S3JobManager  # noqa: E402


class FakeExecutor:
    def __init__(self):
        self.submissions: list[tuple] = []

    def submit(self, fn, *args, **kwargs):
        self.submissions.append((fn, args, kwargs))
        return {'submitted': True}


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
        fake_executor = FakeExecutor()
        manager._executor = fake_executor  # type: ignore[assignment]

        first_rows = [
            {'id': 'prod-1', 'goods_id': 'shein:prod-1'},
            {'id': 'prod-2', 'goods_id': 'shein:prod-2'},
        ]
        second_rows = [
            {'id': 'prod-2', 'goods_id': 'shein:prod-2'},
            {'id': 'prod-3', 'goods_id': 'shein:prod-3'},
        ]

        reserved_first = manager.reserve_upload_candidates(
            job_id='job-1',
            dataset_id='shein',
            collector=lambda excluded: {
                'dataset_id': 'shein',
                'rows': [row for row in first_rows if row['id'] not in excluded],
                'limit': 10,
            },
        )
        reserved_second = manager.reserve_upload_candidates(
            job_id='job-2',
            dataset_id='shein',
            collector=lambda excluded: {
                'dataset_id': 'shein',
                'rows': [row for row in second_rows if row['id'] not in excluded],
                'limit': 10,
            },
        )

        manager.start_job(
            job_id='job-1',
            dataset_id='shein',
            source='products',
            bucket='bucket-a',
            prefix='prefix-a',
            limit=10,
            concurrency=1,
            rows=reserved_first['rows'],
            dry_run=True,
        )
        manager.start_job(
            job_id='job-2',
            dataset_id='shein',
            source='products',
            bucket='bucket-a',
            prefix='prefix-a',
            limit=10,
            concurrency=1,
            rows=reserved_second['rows'],
            dry_run=True,
        )

        assert len(fake_executor.submissions) == 2, fake_executor.submissions
        first_dispatched = fake_executor.submissions[0][1][1]
        second_dispatched = fake_executor.submissions[1][1][1]
        assert [row['id'] for row in first_dispatched] == ['prod-1', 'prod-2'], first_dispatched
        assert [row['id'] for row in second_dispatched] == ['prod-3'], second_dispatched

        first_job = manager.get_job('job-1')
        second_job = manager.get_job('job-2')
        assert first_job['total'] == 2, first_job
        assert second_job['total'] == 1, second_job

        claim_conn = db_connect()
        try:
            rows = claim_conn.execute('SELECT dataset_id, product_id, job_id FROM s3_job_claims ORDER BY dataset_id, product_id').fetchall()
            assert [(row['dataset_id'], row['product_id'], row['job_id']) for row in rows] == [
                ('shein', 'prod-1', 'job-1'),
                ('shein', 'prod-2', 'job-1'),
                ('shein', 'prod-3', 'job-2'),
            ], [(row['dataset_id'], row['product_id'], row['job_id']) for row in rows]
        finally:
            claim_conn.close()

        manager.release_job_claims('job-1')
        manager.release_job_claims('job-2')
        reserved_after_release = manager.reserve_upload_candidates(
            job_id='job-3',
            dataset_id='shein',
            collector=lambda excluded: {
                'dataset_id': 'shein',
                'rows': [row for row in second_rows if row['id'] not in excluded],
                'limit': 10,
            },
        )
        assert [row['id'] for row in reserved_after_release['rows']] == ['prod-2', 'prod-3'], reserved_after_release

        claim_conn = db_connect()
        try:
            rows = claim_conn.execute('SELECT dataset_id, product_id, job_id FROM s3_job_claims ORDER BY dataset_id, product_id').fetchall()
            assert [(row['dataset_id'], row['product_id'], row['job_id']) for row in rows] == [
                ('shein', 'prod-2', 'job-3'),
                ('shein', 'prod-3', 'job-3'),
            ], [(row['dataset_id'], row['product_id'], row['job_id']) for row in rows]
        finally:
            claim_conn.close()

    print('OK: concurrent upload jobs claim distinct products before dispatch')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

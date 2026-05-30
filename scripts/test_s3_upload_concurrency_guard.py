#!/usr/bin/env python3
from __future__ import annotations

import io
import json
import os
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import server  # noqa: E402
from s3_jobs import S3JobState  # noqa: E402


class FakeHeaders(dict):
    def get(self, key, default=None):
        return super().get(key, default)


class FakeHandler(server.Handler):
    def __init__(self):
        pass

    def send_response(self, status, *args, **kwargs):
        self._status = status

    def send_header(self, key, value):
        self._headers[key] = value

    def end_headers(self):
        pass


def override_server_db_path(db_path: Path):
    os.environ['FAST_FASHION_DB_PATH'] = str(db_path)
    server.DB_PATH = db_path


def make_handler(body: dict | None = None):
    handler = FakeHandler.__new__(FakeHandler)
    raw = json.dumps(body).encode('utf-8') if body is not None else b''
    handler.headers = FakeHeaders({'Content-Length': str(len(raw))})
    handler.rfile = io.BytesIO(raw)
    handler.wfile = io.BytesIO()
    handler._headers = {}
    handler.path = '/api/s3/upload-jobs'
    return handler


def read_json(handler):
    handler.wfile.seek(0)
    return json.loads(handler.wfile.read().decode('utf-8'))


def seed_products(db_path: Path):
    override_server_db_path(db_path)
    source = REPO / 'catalog.db'
    db_path.write_bytes(source.read_bytes())
    conn = server.db_connect()
    try:
        for index in range(1, 5):
            conn.execute(
                "INSERT INTO products (dataset_id, id, name, description, category, category_path, price, price_text, rating, reviews_count, brand, color, size_text, sizes_json, image, image_urls_json, image_count, url, source, search_text) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    'shein',
                    f'prod-{index}',
                    f'Concurrency product {index}',
                    'desc',
                    'tops',
                    'tops',
                    10.0,
                    '10.00',
                    4.5,
                    2,
                    'Brand',
                    'Blue',
                    'M',
                    '[]',
                    f'https://example.test/images/{index}.jpg',
                    json.dumps([f'https://example.test/images/{index}.jpg']),
                    1,
                    f'https://example.test/products/{index}',
                    'SHEIN',
                    f'concurrency product {index}',
                ),
            )
        conn.commit()
    finally:
        conn.close()


def main() -> int:
    previous_db_env = os.environ.get('FAST_FASHION_DB_PATH')
    previous_db_path = server.DB_PATH
    previous_bucket = os.environ.get('AWS_BUCKET')
    original_manager = server.S3_JOB_MANAGER
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'catalog.db'
        seed_products(db_path)
        os.environ['AWS_BUCKET'] = 'unit-bucket'
        manager = server.S3JobManager(store_path=Path(tmpdir) / 'jobs.json', db_connect_fn=server.db_connect)
        server.S3_JOB_MANAGER = manager
        manager._jobs.clear()

        active_job = S3JobState(
            job_id='active-upload-1',
            dataset_id='shein',
            source='products',
            limit=10,
            status='running',
            total=2,
            job_family='upload',
            dry_run=False,
        )
        manager._jobs[active_job.job_id] = active_job
        claim_conn = server.db_connect()
        try:
            claim_conn.execute(
                'INSERT INTO s3_job_claims (dataset_id, product_id, job_id, claimed_at) VALUES (?, ?, ?, ?)',
                ('shein', 'prod-1', active_job.job_id, 1.0),
            )
            claim_conn.execute(
                'INSERT INTO s3_job_claims (dataset_id, product_id, job_id, claimed_at) VALUES (?, ?, ?, ?)',
                ('shein', 'prod-2', active_job.job_id, 1.0),
            )
            claim_conn.commit()
        finally:
            claim_conn.close()

        broad_handler = make_handler({'dataset_id': 'shein', 'limit': 50, 'concurrency': 1, 'selection_mode': 'pending'})
        original_start_job = manager.start_job
        captured = {}

        def fake_start_job(**kwargs):
            captured.update(kwargs)
            job = manager.get_job(kwargs['job_id'])
            if job is None:
                manager._jobs[kwargs['job_id']] = S3JobState(
                    job_id=kwargs['job_id'],
                    dataset_id=kwargs['dataset_id'],
                    source=kwargs['source'],
                    limit=kwargs['limit'],
                    status='queued',
                    total=len(kwargs['rows']),
                    bucket=kwargs['bucket'],
                    prefix=kwargs['prefix'],
                    concurrency=kwargs['concurrency'],
                    job_family='upload',
                    dry_run=bool(kwargs.get('dry_run')),
                )
            return None

        manager.start_job = fake_start_job  # type: ignore[assignment]
        try:
            server.Handler.handle_s3_family_job_create(broad_handler, 'upload')
        finally:
            manager.start_job = original_start_job  # type: ignore[assignment]
        broad_payload = read_json(broad_handler)
        assert broad_handler._status == 202, broad_payload
        returned_ids = {str(row['id']) for row in captured['rows']}
        assert 'prod-1' not in returned_ids, captured
        assert 'prod-2' not in returned_ids, captured
        assert returned_ids, captured

        no_free_handler = make_handler({'dataset_id': 'shein', 'limit': 1, 'concurrency': 1, 'source_filter': 'prod-1', 'selection_mode': 'pending'})
        server.Handler.handle_s3_family_job_create(no_free_handler, 'upload')
        no_free_payload = read_json(no_free_handler)
        assert no_free_handler._status == 409, no_free_payload
        assert no_free_payload['error']['code'] == 's3_upload_no_free_products', no_free_payload

    server.S3_JOB_MANAGER = original_manager
    if previous_db_env is None:
        os.environ.pop('FAST_FASHION_DB_PATH', None)
    else:
        os.environ['FAST_FASHION_DB_PATH'] = previous_db_env
    server.DB_PATH = previous_db_path
    if previous_bucket is None:
        os.environ.pop('AWS_BUCKET', None)
    else:
        os.environ['AWS_BUCKET'] = previous_bucket

    print('OK: concurrent upload creation still works on free products and only blocks when no free products remain')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

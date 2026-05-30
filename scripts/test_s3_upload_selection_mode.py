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


def seed_products_and_s3_state(db_path: Path):
    override_server_db_path(db_path)
    source = REPO / 'catalog.db'
    db_path.write_bytes(source.read_bytes())
    conn = server.db_connect()
    try:
        products = [
            ('shein', 'prod-1', 'Selection Mode Complete product', 'https://example.test/products/selection-mode/complete', 'https://example.test/images/selection-mode-complete.jpg'),
            ('shein', 'prod-2', 'Selection Mode Partial product', 'https://example.test/products/selection-mode/partial', 'https://example.test/images/selection-mode-partial.jpg'),
            ('shein', 'prod-3', 'Selection Mode Unsynced product', 'https://example.test/products/selection-mode/unsynced', 'https://example.test/images/selection-mode-unsynced.jpg'),
        ]
        for dataset_id, product_id, name, url, image in products:
            conn.execute(
                "INSERT INTO products (dataset_id, id, name, description, category, category_path, price, price_text, rating, reviews_count, brand, color, size_text, sizes_json, image, image_urls_json, image_count, url, source, search_text) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    dataset_id,
                    product_id,
                    name,
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
                    image,
                    json.dumps([image]),
                    1,
                    url,
                    'SHEIN',
                    name.lower(),
                ),
            )

        s3_rows = [
            ('shein:prod-1', 'shein', 'prod-1', 'https://example.test/images/complete.jpg', 'https://cdn.example.test/prod-1.jpg', 'bucket-a', 'shein/prod-1/main.jpg', 1, 1, 0, 1),
            ('shein:prod-2', 'shein', 'prod-2', 'https://example.test/images/partial.jpg', 'https://cdn.example.test/prod-2.jpg', 'bucket-a', 'shein/prod-2/main.jpg', 2, 1, 1, 0),
        ]
        for goods_id, dataset_id, product_id, source_url, s3_url, bucket, object_key, source_image_count, s3_image_count, failed_image_count, saved_on_s3 in s3_rows:
            conn.execute(
                "INSERT OR REPLACE INTO s3_objects (goods_id, dataset_id, product_id, source_url, s3_url, bucket, object_key, source_image_urls_json, s3_image_urls_json, image_pairs_json, source_image_count, s3_image_count, failed_image_count, saved_on_s3, saved_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    goods_id,
                    dataset_id,
                    product_id,
                    source_url,
                    s3_url,
                    bucket,
                    object_key,
                    json.dumps([source_url]),
                    json.dumps([s3_url]),
                    json.dumps([{'source_url': source_url, 's3_url': s3_url, 'key': object_key, 'status': 'uploaded'}]),
                    source_image_count,
                    s3_image_count,
                    failed_image_count,
                    saved_on_s3,
                    1.0,
                    1.0,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def main() -> int:
    previous_db_env = os.environ.get('FAST_FASHION_DB_PATH')
    previous_db_path = server.DB_PATH
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'catalog.db'
        seed_products_and_s3_state(db_path)

        manager = server.S3_JOB_MANAGER
        manager._jobs.clear()
        original_start_job = manager.start_job
        captured_calls: list[dict] = []

        def fake_start_job(**kwargs):
            captured_calls.append(kwargs)
            manager._create_job_unlocked(
                job_id=kwargs['job_id'],
                dataset_id=kwargs['dataset_id'],
                source=kwargs['source'],
                bucket=kwargs['bucket'],
                prefix=kwargs['prefix'],
                limit=kwargs['limit'],
                concurrency=kwargs['concurrency'],
                source_filter=kwargs.get('source_filter'),
                selection_mode=kwargs.get('selection_mode'),
                excluded_complete_count=int(kwargs.get('excluded_complete_count') or 0),
                total=len(kwargs['rows']),
                job_family=kwargs.get('job_family', 'upload'),
                dry_run=bool(kwargs.get('dry_run')),
            )
            return None

        manager.start_job = fake_start_job  # type: ignore[assignment]
        try:
            pending_handler = make_handler({'dataset_id': 'shein', 'limit': 10, 'concurrency': 1, 'source_filter': 'selection-mode'})
            server.Handler.handle_s3_family_job_create(pending_handler, 'upload')
            pending_payload = read_json(pending_handler)
            pending_call = captured_calls[-1]
            assert pending_call['selection_mode'] == 'pending', pending_call
            assert [row['id'] for row in pending_call['rows']] == ['prod-2', 'prod-3'], pending_call['rows']
            assert pending_call['excluded_complete_count'] == 1, pending_call
            assert pending_payload['data']['selection_mode'] == 'pending', pending_payload
            assert pending_payload['data']['excluded_complete_count'] == 1, pending_payload

            pending_only_handler = make_handler({'dataset_id': 'shein', 'limit': 10, 'concurrency': 1, 'source_filter': 'selection-mode', 'selection_mode': 'pending_only'})
            server.Handler.handle_s3_family_job_create(pending_only_handler, 'upload')
            pending_only_payload = read_json(pending_only_handler)
            pending_only_call = captured_calls[-1]
            assert pending_only_call['selection_mode'] == 'pending_only', pending_only_call
            assert [row['id'] for row in pending_only_call['rows']] == ['prod-3'], pending_only_call['rows']
            assert pending_only_call['excluded_complete_count'] == 1, pending_only_call
            assert pending_only_payload['data']['selection_mode'] == 'pending_only', pending_only_payload
            assert pending_only_payload['data']['excluded_complete_count'] == 1, pending_only_payload

            all_handler = make_handler({'dataset_id': 'shein', 'limit': 10, 'concurrency': 1, 'source_filter': 'selection-mode', 'selection_mode': 'all'})
            server.Handler.handle_s3_family_job_create(all_handler, 'upload')
            all_payload = read_json(all_handler)
            all_call = captured_calls[-1]
            assert all_call['selection_mode'] == 'all', all_call
            assert [row['id'] for row in all_call['rows']] == ['prod-1', 'prod-2', 'prod-3'], all_call['rows']
            assert all_call['excluded_complete_count'] == 0, all_call
            assert all_payload['data']['selection_mode'] == 'all', all_payload
            assert all_payload['data']['excluded_complete_count'] == 0, all_payload

            partial_handler = make_handler({'dataset_id': 'shein', 'limit': 10, 'concurrency': 1, 'source_filter': 'selection-mode', 'selection_mode': 'partial'})
            server.Handler.handle_s3_family_job_create(partial_handler, 'upload')
            partial_payload = read_json(partial_handler)
            partial_call = captured_calls[-1]
            assert partial_call['selection_mode'] == 'partial', partial_call
            assert [row['id'] for row in partial_call['rows']] == ['prod-2'], partial_call['rows']
            assert partial_call['excluded_complete_count'] == 1, partial_call
            assert partial_payload['data']['selection_mode'] == 'partial', partial_payload
            assert partial_payload['data']['excluded_complete_count'] == 1, partial_payload
        finally:
            manager.start_job = original_start_job  # type: ignore[assignment]
            if previous_db_env is None:
                os.environ.pop('FAST_FASHION_DB_PATH', None)
            else:
                os.environ['FAST_FASHION_DB_PATH'] = previous_db_env
            server.DB_PATH = previous_db_path

    print('OK: upload jobs support pending, pending_only, partial, and all candidate selection modes')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

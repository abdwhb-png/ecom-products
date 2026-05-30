#!/usr/bin/env python3
from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import server  # noqa: E402
from s3_jobs import S3JobState  # noqa: E402


def override_server_db_path(db_path: Path):
    os.environ['FAST_FASHION_DB_PATH'] = str(db_path)
    server.DB_PATH = db_path


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


class FakeS3:
    def __init__(self):
        self.objects = {}

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


def make_handler(body: dict | None = None):
    handler = FakeHandler.__new__(FakeHandler)
    handler.headers = FakeHeaders({'Content-Length': str(len(json.dumps(body).encode('utf-8'))) if body is not None else '0'})
    handler.rfile = io.BytesIO(json.dumps(body).encode('utf-8') if body is not None else b'')
    handler.wfile = io.BytesIO()
    handler._headers = {}
    handler.path = '/'
    return handler


def read_json(handler):
    handler.wfile.seek(0)
    return json.loads(handler.wfile.read().decode('utf-8'))


def seed_products(db_path: Path):
    override_server_db_path(db_path)
    if not db_path.exists():
        source = Path(__file__).resolve().parents[1] / 'catalog.db'
        db_path.write_bytes(source.read_bytes())
    conn = server.db_connect()
    try:
        conn.execute(
            "INSERT INTO products (dataset_id, id, name, description, category, category_path, price, price_text, rating, reviews_count, brand, color, size_text, sizes_json, image, image_urls_json, image_count, url, source, search_text) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                'shein', 'prod-1', 'Env authoritative product', 'desc', 'tops', 'tops', 10.0, '10.00', 4.5, 2,
                'Brand', 'Blue', 'M', '[]', 'https://example.test/image-main.jpg', json.dumps(['https://example.test/image-main.jpg']), 1,
                'https://example.test/product', 'SHEIN', 'env authoritative product'
            ),
        )
        conn.execute(
            "INSERT OR REPLACE INTO s3_config (config_key, config_value, updated_at) VALUES (?, ?, ?)",
            ('config', json.dumps({
                'bucket': 'stale-bucket',
                'prefix': 'stale-prefix',
                'endpoint_url': 'https://stale.example.invalid',
                'public_url': 'https://stale-public.example.invalid',
                'region_name': 'eu-west-1',
            }), 1.0),
        )
        conn.commit()
    finally:
        conn.close()


def main() -> int:
    proxy_prev = {key: os.environ.get(key) for key in ['FAST_FASHION_PROXY_HOST', 'FAST_FASHION_PROXY_PORT', 'FAST_FASHION_PROXY_LOGIN', 'FAST_FASHION_PROXY_PASSWORD']}
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'catalog.db'
            os.environ['AWS_BUCKET'] = 'env-bucket'
            os.environ['AWS_PREFIX'] = 'env-prefix'
            os.environ['AWS_ENDPOINT_URL'] = 'https://example-account.r2.cloudflarestorage.com'
            os.environ['AWS_URL'] = 'https://pub.example.dev'
            os.environ['AWS_REGION'] = ''
            os.environ['AWS_DEFAULT_REGION'] = ''

            override_server_db_path(db_path)
            seed_products(db_path)

            os.environ.pop('FAST_FASHION_PROXY_HOST', None)
            os.environ.pop('FAST_FASHION_PROXY_PORT', None)
            os.environ.pop('FAST_FASHION_PROXY_LOGIN', None)
            os.environ.pop('FAST_FASHION_PROXY_PASSWORD', None)

            get_handler = make_handler()
            server.Handler.handle_s3_config_get(get_handler)
            get_payload = read_json(get_handler)
            assert get_payload['data']['bucket'] == 'env-bucket', get_payload
            assert get_payload['data']['prefix'] == 'env-prefix', get_payload
            assert get_payload['data']['endpoint_url'] == 'https://example-account.r2.cloudflarestorage.com', get_payload
            assert get_payload['data']['public_url'] == 'https://pub.example.dev', get_payload
            assert get_payload['data']['region_name'] == 'auto', get_payload
            assert get_payload['data']['config_source'] == 'env', get_payload
            assert get_payload['data']['proxy_enabled'] is False, get_payload
            assert get_payload['data']['egress_proxy_mode'] == 'direct', get_payload

            os.environ['FAST_FASHION_PROXY_HOST'] = 'gw.dataimpulse.com'
            os.environ['FAST_FASHION_PROXY_PORT'] = '823'
            os.environ['FAST_FASHION_PROXY_LOGIN'] = 'proxy-user'
            os.environ['FAST_FASHION_PROXY_PASSWORD'] = 'proxy-pass'
            os.environ.pop('FAST_FASHION_ASOS_PROXY_MAX_CONCURRENCY', None)
            get_handler_proxy = make_handler()
            server.Handler.handle_s3_config_get(get_handler_proxy)
            proxy_payload = read_json(get_handler_proxy)
            assert proxy_payload['data']['proxy_enabled'] is True, proxy_payload
            assert proxy_payload['data']['egress_proxy_mode'] == 'proxy', proxy_payload
            assert proxy_payload['data']['asos_max_concurrency'] == 8, proxy_payload

            os.environ['FAST_FASHION_ASOS_PROXY_MAX_CONCURRENCY'] = '12'
            get_handler_proxy_custom = make_handler()
            server.Handler.handle_s3_config_get(get_handler_proxy_custom)
            proxy_custom_payload = read_json(get_handler_proxy_custom)
            assert proxy_custom_payload['data']['asos_max_concurrency'] == 12, proxy_custom_payload

            manager = server.S3_JOB_MANAGER
            manager._jobs.clear()

            fake_s3 = FakeS3()
            original_download = manager._download
            original_start_job = manager.start_job
            manager._download = lambda url, dataset_id=None: (b'img-bytes', 'image/jpeg', {'hostname': 'example.test', 'proxy_used': True, 'proxy_mode': 'proxy', 'attempt_count': 1, 'timeout_seconds': 10, 'error_type': None, 'http_status': None})  # type: ignore[method-assign]
            captured = {}

            def fake_start_job(**kwargs):
                captured.update(kwargs)
                job = server.S3_JOB_MANAGER.get_job(kwargs['job_id'])
                if job is None:
                    server.S3_JOB_MANAGER._jobs[kwargs['job_id']] = S3JobState(
                        job_id=kwargs['job_id'],
                        dataset_id=kwargs['dataset_id'],
                        source=kwargs['source'],
                        limit=kwargs['limit'],
                        concurrency=kwargs['concurrency'],
                        bucket=kwargs['bucket'],
                        prefix=kwargs['prefix'],
                        total=len(kwargs['rows']),
                        status='queued',
                        job_family='upload',
                        dry_run=bool(kwargs.get('dry_run')),
                    )
                return None

            manager.start_job = fake_start_job  # type: ignore[assignment]
            try:
                create_handler = make_handler({'dataset_id': 'shein', 'limit': 1, 'concurrency': 1, 'bucket': 'ignored-bucket', 'prefix': 'ignored-prefix'})
                server.Handler.handle_s3_family_job_create(create_handler, 'upload')
                create_payload = read_json(create_handler)
                job = create_payload['data']
                assert captured['bucket'] == 'env-bucket', captured
                assert captured['prefix'] == 'env-prefix', captured
                assert job['bucket'] == 'env-bucket', job
                assert job['prefix'] == 'env-prefix', job

                create_handler_second = make_handler({'dataset_id': 'shein', 'limit': 1, 'concurrency': 1})
                server.Handler.handle_s3_family_job_create(create_handler_second, 'upload')
                second_payload = read_json(create_handler_second)
                assert second_payload['data']['job_id'] != job['job_id'], (job, second_payload)
                server.S3_JOB_MANAGER.release_job_claims(job['job_id'])
                server.S3_JOB_MANAGER.release_job_claims(second_payload['data']['job_id'])

                create_handler_asos = make_handler({'dataset_id': 'asos', 'limit': 1, 'concurrency': 24})
                server.Handler.handle_s3_family_job_create(create_handler_asos, 'upload')
                assert captured['concurrency'] == 12, captured

                server.S3_JOB_MANAGER.release_job_claims('env-authoritative-test')

                # Run synchronously with env config and no stale DB override.
                future = original_start_job(
                    job_id='env-authoritative-test',
                    dataset_id='shein',
                    source='products',
                    bucket='env-bucket',
                    prefix='env-prefix',
                    limit=1,
                    concurrency=1,
                    rows=[{
                        'id': 'prod-1',
                        'goods_id': 'prod-1',
                        'name': 'Env authoritative product',
                        'image': 'https://example.test/image-main.jpg',
                        'image_urls_json': json.dumps(['https://example.test/image-main.jpg']),
                    }],
                    s3_client_factory=lambda: fake_s3,
                    resolve_source_url=lambda row: ['https://example.test/image-main.jpg'],
                    on_uploaded=None,
                )
                deadline = time.time() + 15
                while time.time() < deadline:
                    job_state = server.S3_JOB_MANAGER.get_job('env-authoritative-test')
                    if job_state and job_state.get('status') == 'completed':
                        break
                    time.sleep(0.1)
                else:
                    raise AssertionError(server.S3_JOB_MANAGER.get_job('env-authoritative-test'))
                if future.done():
                    future.result(timeout=0)
                assert any(key.startswith('env-prefix/shein/prod-1/') for key in fake_s3.objects), fake_s3.objects
            finally:
                manager._download = original_download  # type: ignore[method-assign]
                manager.start_job = original_start_job  # type: ignore[assignment]
    finally:
        for key, value in proxy_prev.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    print('OK: S3 config is env-authoritative and upload jobs use env-derived bucket/prefix and a configurable prudent proxy-aware ASOS cap')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

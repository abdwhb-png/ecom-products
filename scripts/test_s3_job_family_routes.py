#!/usr/bin/env python3
from __future__ import annotations

import io
import json
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


def make_handler():
    handler = FakeHandler.__new__(FakeHandler)
    handler.headers = FakeHeaders({})
    handler.wfile = io.BytesIO()
    handler._headers = {}
    handler.path = '/'
    return handler


def read_json(handler):
    handler.wfile.seek(0)
    return json.loads(handler.wfile.read().decode('utf-8'))


def main() -> int:
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = server.S3JobManager(store_path=Path(tmpdir) / 'routes-jobs.json')
        server.S3_JOB_MANAGER = manager
        manager._jobs.clear()
        for payload in [
            {
                'job_id': 'upload-1',
                'dataset_id': 'shein',
                'source': 'products',
                'limit': 1,
                'status': 'completed',
                'total': 1,
                'job_family': 'upload',
                'dry_run': True,
                'items': [{'status': 'preview'}],
            },
            {
                'job_id': 'migration-1',
                'dataset_id': 'all',
                'source': 'migration',
                'limit': 1,
                'status': 'completed',
                'total': 1,
                'kind': 'migration_preview',
                'items': [{'status': 'preview'}],
            },
            {
                'job_id': 'cleanup-1',
                'dataset_id': 'all',
                'source': 'cleanup',
                'limit': 1,
                'status': 'completed',
                'total': 1,
                'kind': 'cleanup_preview',
                'items': [{'status': 'preview'}],
            },
        ]:
            job = manager._coerce_job(payload)
            assert job is not None
            manager._jobs[job.job_id] = job

        upload_handler = make_handler()
        upload_handler.path = '/api/s3/upload-jobs?page=1&pageSize=20'
        server.Handler.handle_s3_family_jobs_list(upload_handler, 'upload')
        upload_payload = read_json(upload_handler)
        assert [job['job_id'] for job in upload_payload['data']] == ['upload-1'], upload_payload
        assert upload_payload['data'][0]['dry_run'] is True, upload_payload
        assert upload_payload['pagination']['page'] == 1, upload_payload
        assert upload_payload['pagination']['pageSize'] == 20, upload_payload

        migration_handler = make_handler()
        migration_handler.path = '/api/s3/url-migration-jobs?page=1&pageSize=20'
        server.Handler.handle_s3_family_jobs_list(migration_handler, 'url_migration')
        migration_payload = read_json(migration_handler)
        assert [job['job_id'] for job in migration_payload['data']] == ['migration-1'], migration_payload
        assert migration_payload['data'][0]['job_family'] == 'url_migration', migration_payload

        cleanup_handler = make_handler()
        cleanup_handler.path = '/api/s3/state-cleanup-jobs?page=1&pageSize=20'
        server.Handler.handle_s3_family_jobs_list(cleanup_handler, 'state_cleanup')
        cleanup_payload = read_json(cleanup_handler)
        assert [job['job_id'] for job in cleanup_payload['data']] == ['cleanup-1'], cleanup_payload
        assert cleanup_payload['data'][0]['job_family'] == 'state_cleanup', cleanup_payload

    print('OK: family-specific S3 list routes only return their own jobs')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

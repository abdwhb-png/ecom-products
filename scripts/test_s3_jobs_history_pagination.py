#!/usr/bin/env python3
from __future__ import annotations

import io
import json
import tempfile
import sys
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


def make_handler(path: str):
    handler = FakeHandler.__new__(FakeHandler)
    handler.headers = FakeHeaders({})
    handler.wfile = io.BytesIO()
    handler._headers = {}
    handler.path = path
    return handler


def read_json(handler):
    handler.wfile.seek(0)
    return json.loads(handler.wfile.read().decode('utf-8'))


def main() -> int:
    with tempfile.TemporaryDirectory() as tmpdir:
        manager = server.S3JobManager(store_path=Path(tmpdir) / 'history-pagination.json')
        original_manager = server.S3_JOB_MANAGER
        server.S3_JOB_MANAGER = manager
        try:
            manager._jobs.clear()
            for index in range(45):
                job = manager._coerce_job({
                    'job_id': f'upload-{index:02d}',
                    'dataset_id': 'shein',
                    'source': 'products',
                    'limit': 1,
                    'status': 'completed',
                    'total': 1,
                    'started_at': 1000 + index,
                    'ended_at': 1001 + index,
                    'job_family': 'upload',
                    'dry_run': False,
                    'items': [],
                })
                assert job is not None
                manager._jobs[job.job_id] = job

            first_handler = make_handler('/api/s3/upload-jobs?page=1&pageSize=20')
            server.Handler.handle_s3_family_jobs_list(first_handler, 'upload')
            first_payload = read_json(first_handler)
            assert len(first_payload['data']) == 20, first_payload
            assert first_payload['pagination']['page'] == 1, first_payload
            assert first_payload['pagination']['pageSize'] == 20, first_payload
            assert first_payload['pagination']['total'] == 45, first_payload
            assert first_payload['pagination']['totalPages'] == 3, first_payload
            assert first_payload['pagination']['from'] == 1, first_payload
            assert first_payload['pagination']['to'] == 20, first_payload
            assert first_payload['data'][0]['job_id'] == 'upload-44', first_payload['data'][0]

            third_handler = make_handler('/api/s3/upload-jobs?page=3&pageSize=20')
            server.Handler.handle_s3_family_jobs_list(third_handler, 'upload')
            third_payload = read_json(third_handler)
            assert len(third_payload['data']) == 5, third_payload
            assert third_payload['pagination']['page'] == 3, third_payload
            assert third_payload['pagination']['from'] == 41, third_payload
            assert third_payload['pagination']['to'] == 45, third_payload
            assert third_payload['data'][-1]['job_id'] == 'upload-00', third_payload['data'][-1]
        finally:
            server.S3_JOB_MANAGER = original_manager

    print('OK: S3 family history routes paginate jobs at requested page sizes')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

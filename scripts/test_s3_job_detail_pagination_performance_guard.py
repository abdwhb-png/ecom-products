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
        manager = server.S3JobManager(store_path=Path(tmpdir) / 'detail-jobs.json')
        server.S3_JOB_MANAGER = manager
        manager._jobs.clear()

        items = []
        for index in range(120):
            items.append({
                'status': 'uploaded',
                'message': f'item-{index}',
                'timestamp': 1700000000 + index,
                'goods_id': f'goods-{index}',
                's3_url': f's3://unit-bucket/path/to/item-{index}.jpg',
            })

        job = manager._coerce_job({
            'job_id': 'detail-heavy-job',
            'dataset_id': 'shein',
            'source': 'products',
            'limit': 120,
            'status': 'running',
            'processed': 80,
            'uploaded': 80,
            'skipped': 0,
            'failed': 0,
            'total': 120,
            'bucket': 'unit-bucket',
            'prefix': 'unit-prefix',
            'items': items,
            'job_family': 'upload',
        })
        assert job is not None
        manager._jobs[job.job_id] = job

        list_handler = make_handler('/api/s3/upload-jobs?page=1&pageSize=20')
        server.Handler.handle_s3_family_jobs_list(list_handler, 'upload')
        list_payload = read_json(list_handler)
        listed_job = list_payload['data'][0]
        assert 'items' not in listed_job, listed_job
        assert listed_job['job_id'] == 'detail-heavy-job', listed_job

        detail_handler = make_handler('/api/s3/jobs/detail-heavy-job?page=2&page_size=10')
        server.Handler.handle_s3_job_detail(detail_handler, 'detail-heavy-job')
        detail_payload = read_json(detail_handler)
        detail = detail_payload['data']
        assert 'items' not in detail['job'], detail['job']
        assert detail['page'] == 2, detail
        assert detail['page_size'] == 10, detail
        assert detail['total_items'] == 120, detail
        assert detail['total_pages'] == 12, detail
        assert len(detail['items']) == 10, detail
        assert detail['items'][0]['goods_id'] == 'goods-10', detail['items'][0]
        assert detail['items'][-1]['goods_id'] == 'goods-19', detail['items'][-1]
        assert str(detail['items'][0]['s3_url']).startswith('https://'), detail['items'][0]

    print('OK: S3 job lists omit heavy items and detail endpoint publicizes only the requested page slice')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

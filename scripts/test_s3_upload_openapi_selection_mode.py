#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PORT = int(os.environ.get('FAST_FASHION_TEST_PORT', '8881'))
TOKEN = os.environ.get('FAST_FASHION_TEST_TOKEN', 'test-token-123')
BASE = f'http://127.0.0.1:{PORT}'


def main() -> int:
    env = os.environ.copy()
    env['FAST_FASHION_API_TOKEN'] = TOKEN
    env['FAST_FASHION_HOST'] = '127.0.0.1'
    env['FAST_FASHION_PORT'] = str(PORT)
    server = subprocess.Popen(
        [sys.executable, 'server.py'],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        time.sleep(1.0)
        with urllib.request.urlopen(f'{BASE}/openapi.json', timeout=10) as resp:
            spec = json.loads(resp.read().decode('utf-8'))
        schemas = spec.get('components', {}).get('schemas', {})
        create_schema = schemas.get('S3UploadJobCreateRequest', {}).get('properties', {})
        state_schema = schemas.get('S3JobState', {}).get('properties', {})
        upload_post = spec.get('paths', {}).get('/api/s3/upload-jobs', {}).get('post', {})
        out = {
            'create_has_selection_mode': 'selection_mode' in create_schema,
            'create_selection_default': create_schema.get('selection_mode', {}).get('default'),
            'create_selection_enum': create_schema.get('selection_mode', {}).get('enum'),
            'state_has_selection_mode': 'selection_mode' in state_schema,
            'state_selection_enum': state_schema.get('selection_mode', {}).get('enum'),
            'state_has_excluded_complete_count': 'excluded_complete_count' in state_schema,
            'upload_post_description': upload_post.get('description', ''),
        }
        print(json.dumps(out, ensure_ascii=False))
        return 0 if (
            out['create_has_selection_mode']
            and out['create_selection_default'] == 'pending'
            and out['create_selection_enum'] == ['pending', 'pending_only', 'all', 'partial']
            and out['state_has_selection_mode']
            and out['state_selection_enum'] == ['pending', 'pending_only', 'all', 'partial']
            and out['state_has_excluded_complete_count']
            and 'selection_mode' in out['upload_post_description']
        ) else 1
    finally:
        server.terminate()
        try:
            server.wait(timeout=3)
        except subprocess.TimeoutExpired:
            server.kill()


if __name__ == '__main__':
    raise SystemExit(main())

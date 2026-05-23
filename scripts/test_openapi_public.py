#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PORT = int(os.environ.get('FAST_FASHION_TEST_PORT', '8878'))
TOKEN = os.environ.get('FAST_FASHION_TEST_TOKEN', 'test-token-123')
BASE = f'http://127.0.0.1:{PORT}'


def request(path: str, token: str | None = None):
    headers = {}
    if token:
        headers['Authorization'] = f'Bearer {token}'
    req = urllib.request.Request(f'{BASE}{path}', headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, resp.read().decode('utf-8')
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode('utf-8')


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
        pub_status, pub_body = request('/openapi.json')
        alias_status, alias_body = request('/api/openapi.json')
        protected_status, _ = request('/api/datasets')
        print(json.dumps({
            'public_openapi_status': pub_status,
            'alias_openapi_status': alias_status,
            'protected_datasets_status': protected_status,
            'public_paths': sorted(list(json.loads(pub_body).get('paths', {}).keys()))[:4],
            'alias_title': json.loads(alias_body).get('info', {}).get('title'),
        }, ensure_ascii=False))
        return 0 if pub_status == 200 and alias_status == 200 and protected_status == 401 else 1
    finally:
        server.terminate()
        try:
            server.wait(timeout=3)
        except subprocess.TimeoutExpired:
            server.kill()


if __name__ == '__main__':
    raise SystemExit(main())

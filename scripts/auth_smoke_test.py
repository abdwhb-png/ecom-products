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
PORT = int(os.environ.get('FAST_FASHION_TEST_PORT', '8876'))
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
        time.sleep(1.2)
        noauth_status, noauth_body = request('/api/datasets')
        auth_status, auth_body = request('/api/datasets', token=TOKEN)
        print(json.dumps({
            'noauth_status': noauth_status,
            'auth_status': auth_status,
            'auth_datasets_count': len(json.loads(auth_body).get('datasets', [])) if auth_status == 200 else None,
            'noauth_body': json.loads(noauth_body),
        }, ensure_ascii=False))
        return 0 if noauth_status == 401 and auth_status == 200 else 1
    finally:
        server.terminate()
        try:
            server.wait(timeout=3)
        except subprocess.TimeoutExpired:
            server.kill()


if __name__ == '__main__':
    raise SystemExit(main())

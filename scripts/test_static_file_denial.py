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
PORT = int(os.environ.get('FAST_FASHION_TEST_PORT', '8881'))
BASE = f'http://127.0.0.1:{PORT}'


def request(path: str):
    req = urllib.request.Request(f'{BASE}{path}')
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, resp.read().decode('utf-8', errors='replace')
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode('utf-8', errors='replace')


def main() -> int:
    env = os.environ.copy()
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
        blocked_paths = ['/.env', '/Dockerfile', '/docker-compose.yml', '/server.py']
        results = {path: request(path)[0] for path in blocked_paths}
        public_status, _ = request('/')
        print(json.dumps({'blocked': results, 'dashboard_status': public_status}, ensure_ascii=False))
        return 0 if public_status == 200 and all(status == 404 for status in results.values()) else 1
    finally:
        server.terminate()
        try:
            server.wait(timeout=3)
        except subprocess.TimeoutExpired:
            server.kill()


if __name__ == '__main__':
    raise SystemExit(main())

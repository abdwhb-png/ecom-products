#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PORT = int(os.environ.get('FAST_FASHION_TEST_PORT', '8879'))
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
        out = {
            'version': spec.get('info', {}).get('version'),
            'has_schemas': 'schemas' in spec.get('components', {}),
            'has_parameters': 'parameters' in spec.get('components', {}),
            'has_datasets_path': '/api/datasets' in spec.get('paths', {}),
            'has_upload_jobs_path': '/api/s3/upload-jobs' in spec.get('paths', {}),
            'has_url_migration_jobs_path': '/api/s3/url-migration-jobs' in spec.get('paths', {}),
            'has_state_cleanup_jobs_path': '/api/s3/state-cleanup-jobs' in spec.get('paths', {}),
            'datasets_has_responses': 'responses' in spec['paths']['/api/datasets']['get'],
            'products_has_parameters': len(spec['paths']['/api/products']['get'].get('parameters', [])),
            'health_has_response_schema': 'content' in spec['paths']['/healthz']['get']['responses']['200'],
            'has_job_family_schema': 'job_family' in schemas.get('S3JobState', {}).get('properties', {}),
            'has_dry_run_schema': 'dry_run' in schemas.get('S3JobState', {}).get('properties', {}),
            'schema_count': len(schemas),
        }
        print(json.dumps(out, ensure_ascii=False))
        return 0 if all([
            out['has_schemas'],
            out['has_parameters'],
            out['has_datasets_path'],
            out['has_upload_jobs_path'],
            out['has_url_migration_jobs_path'],
            out['has_state_cleanup_jobs_path'],
            out['has_job_family_schema'],
            out['has_dry_run_schema'],
            out['schema_count'] >= 10,
        ]) else 1
    finally:
        server.terminate()
        try:
            server.wait(timeout=3)
        except subprocess.TimeoutExpired:
            server.kill()


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
from __future__ import annotations

import importlib
import os
import sys
import tempfile
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

S3_HTML = REPO / 'public' / 's3.html'


def main() -> int:
    html = S3_HTML.read_text(encoding='utf-8')
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / 'catalog.db'
        os.environ['FAST_FASHION_DB_PATH'] = str(db_path)
        server = importlib.import_module('server')
        server.DB_PATH = db_path
        request_schema = server.OPENAPI_SPEC['paths']['/api/s3/upload-jobs']['post']['requestBody']['content']['application/json']['schema']
        schema_name = str(request_schema['$ref']).rsplit('/', 1)[-1]
        selection_mode = server.OPENAPI_SPEC['components']['schemas'][schema_name]['properties']['selection_mode']

    assert '<option value="pending_only">Pending seulement</option>' in html, 'missing pending_only option in s3.html'
    assert selection_mode['default'] == 'pending', f"unexpected selection_mode default: {selection_mode['default']}"
    assert selection_mode['enum'] == ['pending', 'pending_only', 'all', 'partial'], f"unexpected selection_mode enum: {selection_mode['enum']}"

    print('OK: S3 UI and OpenAPI expose pending_only selection mode')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
S3_HTML = REPO / 's3.html'
SERVER = REPO / 'server.py'


def main() -> int:
    html = S3_HTML.read_text(encoding='utf-8')
    server = SERVER.read_text(encoding='utf-8')

    assert '<option value="pending_only">Pending seulement</option>' in html, 'missing pending_only option in s3.html'
    assert "['pending', 'pending_only', 'all', 'partial']" in server, 'missing pending_only in OpenAPI enum'

    print('OK: S3 UI and OpenAPI expose pending_only selection mode')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

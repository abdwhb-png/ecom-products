from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PUBLIC_DIR = ROOT / 'public'
INDEX_PATH = PUBLIC_DIR / 'index.html'
DOCS_PATH = PUBLIC_DIR / 'docs.html'
S3_PAGE_PATH = PUBLIC_DIR / 's3.html'


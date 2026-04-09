#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

from dataset_service import DatasetDownloadService, load_dotenv

ROOT = Path(__file__).resolve().parent
DATASETS_FILE = ROOT / 'datasets.json'
DOWNLOAD_DIR = ROOT / 'data' / 'downloads'


def main():
    load_dotenv()
    payload = json.loads(DATASETS_FILE.read_text(encoding='utf-8'))
    retained = payload.get('retained', [])
    registry = {item['id']: item for item in retained}
    service = DatasetDownloadService(registry, download_root=DOWNLOAD_DIR)
    artifacts = service.sync_all(force='--force' in __import__('sys').argv)
    for artifact in artifacts:
        print(f'{artifact.dataset_id}: {artifact.provider} -> {artifact.local_path}')


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
from __future__ import annotations

import base64
import json
import os
import shutil
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / 'data'
DOWNLOAD_DIR = DATA_DIR / 'downloads'
ENV_PATH = ROOT / '.env'


def load_dotenv(path: Path = ENV_PATH) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding='utf-8', errors='replace').splitlines():
        line = raw_line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _slugify(value: str) -> str:
    cleaned = ''.join(ch.lower() if ch.isalnum() else '-' for ch in value)
    while '--' in cleaned:
        cleaned = cleaned.replace('--', '-')
    return cleaned.strip('-') or 'dataset'


def _default_filename_from_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    candidate = Path(parsed.path).name or 'download.bin'
    if not candidate:
        candidate = 'download.bin'
    return candidate


def _write_response_to_file(url: str, destination: Path, headers: Optional[Dict[str, str]] = None) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_suffix(destination.suffix + '.tmp')
    request = urllib.request.Request(url, headers=headers or {'User-Agent': 'Mozilla/5.0'})

    with urllib.request.urlopen(request, timeout=120) as response:
        with tmp_path.open('wb') as handle:
            shutil.copyfileobj(response, handle)

    tmp_path.replace(destination)
    return destination


@dataclass(frozen=True)
class DatasetArtifact:
    dataset_id: str
    provider: str
    local_path: Path
    meta: Dict[str, Any]


class DirectProvider:
    name = 'direct'

    def download(self, url: str, destination_dir: Path, filename: Optional[str] = None) -> Path:
        destination_dir.mkdir(parents=True, exist_ok=True)
        target_name = filename or _default_filename_from_url(url)
        destination = destination_dir / target_name
        headers = {'User-Agent': 'Mozilla/5.0'}
        return _write_response_to_file(url, destination, headers=headers)


class HuggingFaceProvider:
    name = 'huggingface'

    def download(
        self,
        repo_id: str,
        file_path: str,
        destination_dir: Path,
        revision: str = 'main',
    ) -> Path:
        destination_dir.mkdir(parents=True, exist_ok=True)
        target_name = Path(file_path).name
        destination = destination_dir / target_name
        url = f'https://huggingface.co/datasets/{repo_id}/resolve/{revision}/{file_path}'
        headers = {'User-Agent': 'Mozilla/5.0'}
        token = os.getenv('HF_TOKEN') or os.getenv('HUGGINGFACE_HUB_TOKEN')
        if token:
            headers['Authorization'] = f'Bearer {token}'
        return _write_response_to_file(url, destination, headers=headers)


class KaggleProvider:
    name = 'kaggle'

    def download(
        self,
        dataset: str,
        destination_dir: Path,
        version: Optional[str] = None,
        unzip: bool = True,
    ) -> Path:
        destination_dir.mkdir(parents=True, exist_ok=True)
        username = os.getenv('KAGGLE_USERNAME')
        key = os.getenv('KAGGLE_KEY')
        if not username or not key:
            raise RuntimeError(
                'Kaggle credentials missing. Set KAGGLE_USERNAME and KAGGLE_KEY in .env or environment.'
            )

        slug = dataset.strip().strip('/')
        url = f'https://www.kaggle.com/api/v1/datasets/download/{slug}'
        if version:
            url = f'{url}?datasetVersionNumber={version}'

        creds = base64.b64encode(f'{username}:{key}'.encode('latin-1')).decode('ascii')
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Authorization': f'Basic {creds}',
        }

        archive_name = f'{_slugify(slug)}.zip'
        archive_path = destination_dir / archive_name
        _write_response_to_file(url, archive_path, headers=headers)

        if not unzip:
            return archive_path

        extract_dir = destination_dir / _slugify(slug)
        if extract_dir.exists():
            shutil.rmtree(extract_dir)
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(archive_path) as zf:
            zf.extractall(extract_dir)
        return extract_dir


class DatasetDownloadService:
    def __init__(self, registry: Dict[str, Dict[str, Any]], download_root: Path = DOWNLOAD_DIR):
        self.registry = registry
        self.download_root = download_root
        self.direct = DirectProvider()
        self.huggingface = HuggingFaceProvider()
        self.kaggle = KaggleProvider()

    def download_named(self, dataset_id: str, force: bool = False) -> DatasetArtifact:
        if dataset_id not in self.registry:
            raise KeyError(f'Unknown dataset id: {dataset_id}')

        spec = self.registry[dataset_id]
        destination_dir = self.download_root / dataset_id
        destination_dir.mkdir(parents=True, exist_ok=True)
        expected_name = spec.get('local_filename')
        if expected_name:
            expected_path = destination_dir / expected_name
            if expected_path.exists() and not force:
                return DatasetArtifact(dataset_id=dataset_id, provider=spec['provider'], local_path=expected_path, meta=spec)
        else:
            existing = sorted(destination_dir.iterdir())
            if existing and not force:
                return DatasetArtifact(dataset_id=dataset_id, provider=spec['provider'], local_path=existing[0], meta=spec)

        provider = spec['provider']
        if provider == 'direct':
            path = self.direct.download(spec['url'], destination_dir, spec.get('local_filename'))
        elif provider == 'huggingface':
            path = self.huggingface.download(
                repo_id=spec['repo_id'],
                file_path=spec['file_path'],
                destination_dir=destination_dir,
                revision=spec.get('revision', 'main'),
            )
        elif provider == 'kaggle':
            path = self.kaggle.download(
                dataset=spec['dataset'],
                destination_dir=destination_dir,
                version=spec.get('version'),
                unzip=spec.get('unzip', True),
            )
        else:
            raise RuntimeError(f'Unsupported provider: {provider}')

        return DatasetArtifact(dataset_id=dataset_id, provider=provider, local_path=path, meta=spec)

    def sync_many(self, dataset_ids: Iterable[str], force: bool = False) -> list[DatasetArtifact]:
        artifacts: list[DatasetArtifact] = []
        for dataset_id in dataset_ids:
            artifacts.append(self.download_named(dataset_id, force=force))
        return artifacts

    def sync_all(self, force: bool = False) -> list[DatasetArtifact]:
        return self.sync_many(self.registry.keys(), force=force)


__all__ = [
    'DATA_DIR',
    'DOWNLOAD_DIR',
    'DatasetArtifact',
    'DatasetDownloadService',
    'DirectProvider',
    'HuggingFaceProvider',
    'KaggleProvider',
    'load_dotenv',
]

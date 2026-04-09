#!/usr/bin/env python3
from __future__ import annotations

import ast
import csv
import json
import re
import sqlite3
import sys
from pathlib import Path

from dataset_service import DatasetDownloadService, load_dotenv

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / 'data'
DOWNLOADS_DIR = DATA_DIR / 'downloads'
DB_PATH = ROOT / 'catalog.db'

SHEIN_SAMPLE_URL = 'https://raw.githubusercontent.com/luminati-io/Shein-dataset-samples/main/shein-products.csv'
ASOS_HF_REPO = 'UniqueData/asos-e-commerce-dataset'
ASOS_HF_FILE = 'products_asos.csv'

RETAINED_DATASETS = {
    'shein': {
        'provider': 'direct',
        'url': SHEIN_SAMPLE_URL,
        'local_filename': 'shein-products.csv',
    },
    'asos': {
        'provider': 'huggingface',
        'repo_id': ASOS_HF_REPO,
        'file_path': ASOS_HF_FILE,
        'revision': 'main',
        'local_filename': ASOS_HF_FILE,
    },
}


def parse_jsonish_list(value):
    if not value or value == 'null':
        return []
    try:
        return json.loads(value)
    except Exception:
        try:
            normalized = value.replace('None', 'null').replace('True', 'true').replace('False', 'false')
            return json.loads(normalized)
        except Exception:
            return []


def parse_python_list(value):
    if not value or value == 'null':
        return []
    try:
        parsed = ast.literal_eval(value)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def number_from_text(value):
    if value is None:
        return None
    cleaned = re.sub(r'[^0-9.\-]+', '', str(value))
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except Exception:
        return None


def split_sizes(value):
    if not value:
        return []
    return [part.strip() for part in str(value).split(',') if part.strip()]


def infer_category_from_name(name, url=''):
    text = f'{name} {url}'.lower()
    rules = [
        ('Dresses', ['dress', 'gown']),
        ('Skirts', ['skirt']),
        ('Shorts', ['shorts', 'short ']),
        ('Jeans', ['jean']),
        ('Pants', ['pants', 'trousers', 'legging', 'jogger']),
        ('Swimwear', ['swimsuit', 'bikini', 'swim']),
        ('Lingerie', ['bra', 'panty', 'lingerie', 'sleepwear', 'nightgown']),
        ('Tops', ['top', 'tee', 't-shirt', 'shirt', 'blouse', 'cami', 'tank']),
        ('Outerwear', ['jacket', 'coat', 'hoodie', 'sweatshirt', 'blazer', 'cardigan']),
        ('Shoes', ['shoe', 'sneaker', 'heel', 'boot', 'sandal', 'loafer']),
        ('Bags', ['bag', 'backpack', 'purse', 'wallet']),
        ('Jewelry', ['necklace', 'ring', 'bracelet', 'earring']),
        ('Accessories', ['accessory', 'belt', 'hat', 'scarf', 'cap', 'mask', 'covering chain']),
        ('Beauty', ['lipstick', 'eyeliner', 'mascara', 'beauty', 'makeup']),
        ('Home', ['storage cabinet', 'cabinet', 'pantry', 'furniture', 'decor', 'crystal', 'kitchen', 'bathroom']),
    ]
    for label, keywords in rules:
        if any(keyword in text for keyword in keywords):
            return label
    match = re.search(r'-cat-(\d+)\.html', url)
    if match:
        return f'cat-{match.group(1)}'
    return 'Other'


def shein_local_rows(path: Path):
    with path.open(newline='', encoding='utf-8', errors='replace') as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader, start=1):
            images = [img for img in parse_python_list(row.get('images') or row.get('image_urls')) if isinstance(img, str) and img.strip()]
            attrs_list = parse_python_list(row.get('description'))
            attr_pairs = []
            for item in attrs_list:
                if isinstance(item, dict):
                    attr_pairs.extend((str(k), str(v)) for k, v in item.items())
            color = next((v for k, v in attr_pairs if k.lower() == 'color'), '')
            name = (row.get('name') or '').strip()
            category = infer_category_from_name(name, row.get('url') or '')
            description = ' • '.join(f'{k}: {v}' for k, v in attr_pairs)
            sku = (row.get('sku') or '').replace('SKU:', '').strip()
            size_text = (row.get('size') or '').strip()
            yield {
                'dataset_id': 'shein',
                'id': sku or f'shein-{index}',
                'name': name or 'Sans nom',
                'description': description,
                'category': category,
                'category_path': '',
                'price': number_from_text(row.get('price')),
                'price_text': (row.get('price') or '').strip() or 'Prix non disponible',
                'rating': None,
                'reviews_count': None,
                'brand': (row.get('brand') or '').strip() or 'SHEIN',
                'color': color,
                'size_text': size_text,
                'sizes_json': json.dumps(split_sizes(size_text), ensure_ascii=False),
                'image': images[0] if images else '',
                'image_urls_json': json.dumps(images, ensure_ascii=False),
                'image_count': len(images),
                'url': (row.get('url') or '').strip(),
                'source': 'Shein Bright Data sample',
                'search_text': ' '.join(filter(None, [name, description, category, color, size_text, row.get('brand', ''), row.get('url', '')])).lower(),
            }


def asos_local_rows(path: Path):
    with path.open(newline='', encoding='utf-8', errors='replace') as handle:
        reader = csv.DictReader(handle)
        for index, row in enumerate(reader, start=1):
            images = [img for img in parse_python_list(row.get('images')) if isinstance(img, str) and img.strip()]
            name = (row.get('name') or '').strip()
            category = (row.get('category') or infer_category_from_name(name, row.get('url') or '')).strip() or 'Other'
            description = (row.get('description') or '').strip()
            sku = str(row.get('sku') or '').strip()
            sizes = split_sizes(row.get('size'))
            color = (row.get('color') or '').strip()
            search_text = ' '.join(filter(None, [name, description, row.get('category', ''), color, row.get('size', ''), sku, row.get('url', '')])).lower()
            yield {
                'dataset_id': 'asos',
                'id': sku or f'asos-{index}',
                'name': name or 'Sans nom',
                'description': description,
                'category': category,
                'category_path': '',
                'price': number_from_text(row.get('price')),
                'price_text': f"{row.get('price', '').strip()}".strip() or 'Prix non disponible',
                'rating': None,
                'reviews_count': None,
                'brand': 'ASOS / UniqueData',
                'color': color,
                'size_text': (row.get('size') or '').strip(),
                'sizes_json': json.dumps(sizes, ensure_ascii=False),
                'image': images[0] if images else '',
                'image_urls_json': json.dumps(images, ensure_ascii=False),
                'image_count': len(images),
                'url': (row.get('url') or '').strip(),
                'source': 'ASOS Hugging Face sample',
                'search_text': search_text,
            }


def ensure_catalog_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)


def rebuild_db(force_download: bool = False):
    load_dotenv()
    ensure_catalog_dirs()

    downloader = DatasetDownloadService(RETAINED_DATASETS, download_root=DOWNLOADS_DIR)
    artifacts = downloader.sync_all(force=force_download)

    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = sqlite3.connect(DB_PATH)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute(
        '''
        CREATE TABLE products (
            dataset_id TEXT NOT NULL,
            id TEXT NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            category TEXT,
            category_path TEXT,
            price REAL,
            price_text TEXT,
            rating REAL,
            reviews_count INTEGER,
            brand TEXT,
            color TEXT,
            size_text TEXT,
            sizes_json TEXT,
            image TEXT,
            image_urls_json TEXT,
            image_count INTEGER,
            url TEXT,
            source TEXT,
            search_text TEXT
        )
        '''
    )
    conn.execute(
        '''
        CREATE TABLE datasets (
            id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            source TEXT NOT NULL,
            total_count INTEGER NOT NULL,
            with_images_count INTEGER NOT NULL,
            with_reviews_count INTEGER NOT NULL,
            local_path TEXT NOT NULL,
            provider TEXT NOT NULL
        )
        '''
    )

    def insert_many(rows):
        conn.executemany(
            '''
            INSERT INTO products (
              dataset_id, id, name, description, category, category_path, price, price_text,
              rating, reviews_count, brand, color, size_text, sizes_json, image,
              image_urls_json, image_count, url, source, search_text
            ) VALUES (
              :dataset_id, :id, :name, :description, :category, :category_path, :price, :price_text,
              :rating, :reviews_count, :brand, :color, :size_text, :sizes_json, :image,
              :image_urls_json, :image_count, :url, :source, :search_text
            )
            ''',
            rows,
        )
        conn.commit()

    print('Importing shein…', file=sys.stderr)
    shein_path = DOWNLOADS_DIR / 'shein' / 'shein-products.csv'
    insert_many(list(shein_local_rows(shein_path)))

    print('Importing asos…', file=sys.stderr)
    asos_path = DOWNLOADS_DIR / 'asos' / 'products_asos.csv'
    insert_many(list(asos_local_rows(asos_path)))

    conn.execute('CREATE INDEX idx_products_dataset ON products(dataset_id)')
    conn.execute('CREATE INDEX idx_products_dataset_category ON products(dataset_id, category)')
    conn.execute('CREATE INDEX idx_products_dataset_price ON products(dataset_id, price)')
    conn.execute('CREATE INDEX idx_products_dataset_image_count ON products(dataset_id, image_count)')
    conn.execute('CREATE INDEX idx_products_dataset_name ON products(dataset_id, name)')
    conn.commit()

    for artifact in artifacts:
        total_count, with_images_count, with_reviews_count = conn.execute(
            '''
            SELECT COUNT(*),
                   SUM(CASE WHEN image <> '' THEN 1 ELSE 0 END),
                   SUM(CASE WHEN COALESCE(reviews_count, 0) > 0 THEN 1 ELSE 0 END)
            FROM products
            WHERE dataset_id = ?
            ''',
            (artifact.dataset_id,),
        ).fetchone()
        conn.execute(
            '''
            INSERT INTO datasets (
                id, label, source, total_count, with_images_count, with_reviews_count, local_path, provider
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                artifact.dataset_id,
                'Shein Bright Data sample' if artifact.dataset_id == 'shein' else 'ASOS Hugging Face sample',
                artifact.meta.get('provider', 'local'),
                total_count,
                with_images_count or 0,
                with_reviews_count or 0,
                str(artifact.local_path),
                artifact.provider,
            ),
        )
    conn.commit()
    conn.close()
    print(f'Catalog built at {DB_PATH}', file=sys.stderr)


if __name__ == '__main__':
    rebuild_db(force_download='--force-download' in sys.argv)

#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import os
import signal
import socket
import sqlite3
import time
import traceback
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse

from s3_jobs import S3JobManager

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / 'catalog.db'
DOCS_PATH = ROOT / 'docs.html'
S3_STATE_PATH = ROOT / 's3_state.json'
ALLOWED_DATASETS = {'shein', 'asos'}
DEFAULT_PAGE_SIZE = 24
MAX_PAGE_SIZE = 200
S3_JOB_MANAGER = S3JobManager()
S3_STATE: dict = {'config': {}, 'objects': {}}
S3_STATE_MTIME = 0.0

ALLOWED_SORTS = {
    'relevance': 'COALESCE(s.ok, 0) DESC, p.image_count DESC, p.id ASC',
    'price-asc': 'p.price ASC NULLS LAST, p.id ASC',
    'price-desc': 'p.price DESC NULLS LAST, p.id ASC',
    'rating-desc': 'p.rating DESC NULLS LAST, p.id ASC',
    'reviews-desc': 'p.reviews_count DESC NULLS LAST, p.id ASC',
    'name-asc': 'p.name COLLATE NOCASE ASC, p.id ASC',
}

CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
    'Access-Control-Max-Age': '86400',
    'X-Content-Type-Options': 'nosniff',
    'Referrer-Policy': 'no-referrer',
    'X-Frame-Options': 'DENY',
}

OPENAPI_SPEC = {
    'openapi': '3.1.0',
    'info': {
        'title': 'Fast Fashion Dashboard API',
        'version': '1.0.1',
        'description': 'Read-only API for categories/products plus background S3 jobs.',
    },
    'servers': [{'url': '/'}],
    'paths': {
        '/api/openapi.json': {'get': {'summary': 'OpenAPI document'}},
        '/api/categories': {'get': {'summary': 'List categories'}},
        '/api/categories/{slug}': {'get': {'summary': 'Get category'}},
        '/api/products': {'get': {'summary': 'List products'}},
        '/api/products/{goods_id}': {'get': {'summary': 'Get product'}},
        '/api/s3/jobs': {'get': {'summary': 'List jobs'}, 'post': {'summary': 'Create job'}},
        '/api/s3/jobs/{job_id}/cancel': {'post': {'summary': 'Cancel job'}},
        '/api/s3/config': {'get': {'summary': 'Get S3 config'}, 'post': {'summary': 'Update S3 config'}},
    },
}


def load_s3_state():
    global S3_STATE, S3_STATE_MTIME
    if not S3_STATE_PATH.exists():
        S3_STATE = {'config': {}, 'objects': {}}
        S3_STATE_MTIME = 0.0
        return S3_STATE
    mtime = S3_STATE_PATH.stat().st_mtime
    if mtime == S3_STATE_MTIME:
        return S3_STATE
    try:
        loaded = json.loads(S3_STATE_PATH.read_text(encoding='utf-8'))
        S3_STATE = {
            'config': loaded.get('config', {}),
            'objects': loaded.get('objects', {}),
        }
        # Never persist secrets on disk.
        for secret_key in ('aws_access_key_id', 'aws_secret_access_key', 'aws_session_token'):
            S3_STATE['config'].pop(secret_key, None)
    except Exception:
        S3_STATE = {'config': {}, 'objects': {}}
    S3_STATE_MTIME = mtime
    return S3_STATE


def save_s3_state():
    payload = {
        'config': {k: v for k, v in S3_STATE.get('config', {}).items() if k not in {'aws_access_key_id', 'aws_secret_access_key', 'aws_session_token'}},
        'objects': S3_STATE.get('objects', {}),
    }
    S3_STATE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS image_status (
            dataset_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            image_url TEXT,
            ok INTEGER NOT NULL,
            status_code INTEGER,
            content_type TEXT,
            checked_at REAL NOT NULL,
            PRIMARY KEY (dataset_id, product_id)
        )
        '''
    )
    conn.execute('CREATE INDEX IF NOT EXISTS idx_image_status_dataset_ok ON image_status(dataset_id, ok)')
    return conn


def json_response(handler, payload, status=HTTPStatus.OK):
    body = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    handler.send_response(status)
    handler.send_header('Content-Type', 'application/json; charset=utf-8')
    handler.send_header('Content-Length', str(len(body)))
    handler.send_header('Cache-Control', 'no-store')
    for key, value in CORS_HEADERS.items():
        handler.send_header(key, value)
    handler.end_headers()
    handler.wfile.write(body)


def html_response(handler, content: bytes, status=HTTPStatus.OK):
    handler.send_response(status)
    handler.send_header('Content-Type', 'text/html; charset=utf-8')
    handler.send_header('Content-Length', str(len(content)))
    handler.send_header('Cache-Control', 'no-store')
    for key, value in CORS_HEADERS.items():
        handler.send_header(key, value)
    handler.end_headers()
    handler.wfile.write(content)


def error_response(handler, message, status=HTTPStatus.BAD_REQUEST, code='bad_request'):
    json_response(handler, {'error': {'code': code, 'message': message}}, status=status)


def parse_positive_int(value, default, minimum=1, maximum=None):
    try:
        parsed = int(str(value).strip())
    except Exception:
        parsed = default
    parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(maximum, parsed)
    return parsed


def parse_bool(value):
    return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}


def to_money(value, default='0.00'):
    try:
        if value is None or value == '':
            return default
        return f'{float(value):.2f}'
    except Exception:
        return default


def make_slug(value):
    text = (value or '').strip().lower()
    text = ''.join(ch if ch.isalnum() else '-' for ch in text)
    while '--' in text:
        text = text.replace('--', '-')
    return text.strip('-') or 'uncategorized'


def split_sizes(value):
    if not value:
        return []
    return [part.strip() for part in str(value).split(',') if part.strip()]


def parse_json_list(raw):
    if not raw:
        return []
    try:
        value = json.loads(raw)
        return value if isinstance(value, list) else []
    except Exception:
        return []


def get_base_url(handler):
    host = handler.headers.get('Host')
    if not host:
        return ''
    scheme = 'https' if handler.headers.get('X-Forwarded-Proto', '').lower() == 'https' else 'http'
    return f'{scheme}://{host}'


def safe_url(base_url, path):
    return f'{base_url}{path}' if base_url else path


def infer_top_category(category):
    if not category:
        return None
    lowered = category.lower()
    mapping = {
        'Dresses': ['dress', 'gown'],
        'Skirts': ['skirt'],
        'Shorts': ['shorts'],
        'Jeans': ['jean'],
        'Pants': ['pants', 'trouser', 'legging', 'jogger'],
        'Swimwear': ['swim', 'bikini', 'swimsuit'],
        'Lingerie': ['bra', 'panty', 'lingerie', 'sleepwear'],
        'Tops': ['top', 'tee', 'shirt', 'blouse', 'tank'],
        'Outerwear': ['jacket', 'coat', 'hoodie', 'sweatshirt', 'blazer', 'cardigan'],
        'Shoes': ['shoe', 'sneaker', 'heel', 'boot', 'sandal'],
        'Bags': ['bag', 'backpack', 'purse', 'wallet'],
        'Accessories': ['accessory', 'belt', 'hat', 'scarf', 'cap'],
        'Home': ['home', 'decor', 'furniture', 'kitchen', 'bathroom'],
    }
    for label, needles in mapping.items():
        if any(needle in lowered for needle in needles):
            return label
    return category.split(' / ')[0].split(' > ')[0].strip() or None


def iter_server_pids():
    current_pid = os.getpid()
    proc = Path('/proc')
    if not proc.exists():
        return
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        if pid == current_pid:
            continue
        try:
            raw = (entry / 'cmdline').read_bytes().replace(b'\x00', b' ').decode('utf-8', errors='ignore').strip()
        except Exception:
            continue
        if not raw:
            continue
        normalized = raw.lower()
        if 'fast-fashion-dashboard/server.py' in normalized or normalized.endswith('/server.py') or ('python' in normalized and 'server.py' in normalized):
            yield pid


def cleanup_previous_servers():
    pids = list(iter_server_pids() or [])
    if not pids:
        return
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
    deadline = time.time() + 3.0
    while time.time() < deadline:
        alive = False
        for pid in pids:
            try:
                os.kill(pid, 0)
                alive = True
                break
            except ProcessLookupError:
                continue
            except PermissionError:
                continue
        if not alive:
            return
        time.sleep(0.15)
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass


def normalize_goods_id(dataset_id, product_id):
    return f'{dataset_id}:{product_id}'


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def do_OPTIONS(self):
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header('Content-Length', '0')
        for key, value in CORS_HEADERS.items():
            self.send_header(key, value)
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        try:
            if parsed.path == '/docs':
                content = DOCS_PATH.read_bytes()
                return html_response(self, content)
            if parsed.path == '/api/openapi.json':
                return json_response(self, OPENAPI_SPEC)
            if parsed.path == '/api/datasets':
                return self.handle_datasets(parsed.query)
            if parsed.path == '/api/categories':
                return self.handle_categories(parsed.query)
            if parsed.path.startswith('/api/categories/'):
                slug = parsed.path.split('/api/categories/', 1)[1].strip('/')
                return self.handle_category(slug, parsed.query)
            if parsed.path == '/api/products':
                return self.handle_products(parsed.query)
            if parsed.path.startswith('/api/products/'):
                goods_id = parsed.path.split('/api/products/', 1)[1].strip('/')
                return self.handle_product(goods_id, parsed.query)
            if parsed.path == '/api/s3/jobs':
                return self.handle_s3_jobs_list()
            if parsed.path == '/api/s3/config':
                return self.handle_s3_config_get()
            return super().do_GET()
        except sqlite3.Error as exc:
            return error_response(self, f'Database error: {exc}', HTTPStatus.INTERNAL_SERVER_ERROR, code='database_error')
        except ValueError as exc:
            return error_response(self, str(exc), HTTPStatus.BAD_REQUEST, code='invalid_request')
        except Exception as exc:
            traceback.print_exc()
            return error_response(self, f'Internal server error: {exc}', HTTPStatus.INTERNAL_SERVER_ERROR, code='internal_error')

    def do_POST(self):
        parsed = urlparse(self.path)
        try:
            if parsed.path == '/api/s3/jobs':
                return self.handle_s3_jobs_create()
            if parsed.path.startswith('/api/s3/jobs/') and parsed.path.endswith('/cancel'):
                job_id = parsed.path.split('/api/s3/jobs/', 1)[1].rsplit('/cancel', 1)[0].strip('/')
                return self.handle_s3_job_cancel(job_id)
            if parsed.path == '/api/s3/config':
                return self.handle_s3_config_update()
            return error_response(self, 'Not found', HTTPStatus.NOT_FOUND, code='not_found')
        except sqlite3.Error as exc:
            return error_response(self, f'Database error: {exc}', HTTPStatus.INTERNAL_SERVER_ERROR, code='database_error')
        except ValueError as exc:
            return error_response(self, str(exc), HTTPStatus.BAD_REQUEST, code='invalid_request')
        except Exception as exc:
            traceback.print_exc()
            return error_response(self, f'Internal server error: {exc}', HTTPStatus.INTERNAL_SERVER_ERROR, code='internal_error')

    def _read_json_body(self):
        length = int(self.headers.get('Content-Length') or '0')
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        return json.loads(raw.decode('utf-8'))

    def _dataset_row(self, conn, dataset_id):
        row = conn.execute('SELECT * FROM datasets WHERE id = ?', (dataset_id,)).fetchone()
        if not row:
            raise ValueError(f'Dataset not found: {dataset_id}')
        return row

    def _category_rows(self, conn, dataset_id):
        return conn.execute(
            '''
            SELECT
                p.category AS name,
                COUNT(*) AS count,
                MAX(CASE WHEN p.image <> '' THEN p.image ELSE NULL END) AS image_url,
                MAX(CASE WHEN p.url <> '' THEN p.url ELSE NULL END) AS source_url
            FROM products p
            WHERE p.dataset_id = ? AND COALESCE(p.category, '') <> ''
            GROUP BY p.category
            ORDER BY count DESC, p.category COLLATE NOCASE ASC
            ''',
            (dataset_id,),
        ).fetchall()

    def _category_resource(self, row):
        name = (row['name'] or '').strip()
        return {
            'name': name,
            'slug': make_slug(name),
            'top_category_name': infer_top_category(name),
            'source_url': row['source_url'] or None,
            'image_url': row['image_url'] or None,
        }

    def _s3_object_for(self, dataset_id, product_id):
        load_s3_state()
        return S3_STATE.get('objects', {}).get(normalize_goods_id(dataset_id, product_id))

    def _product_resource(self, row, dataset_row, base_url):
        product_id = str(row['id']).strip()
        goods_id = normalize_goods_id(dataset_row['id'], product_id)
        name = (row['name'] or '').strip() or 'Sans nom'
        description = (row['description'] or '').strip() or 'Imported from scraper feed.'
        image_urls = [u for u in ([row['image']] + parse_json_list(row['image_urls_json'])) if isinstance(u, str) and u.strip()]
        image_urls = list(dict.fromkeys(image_urls))
        category_name = (row['category'] or '').strip() or None
        category_slug = make_slug(category_name) if category_name else None
        category_url = safe_url(base_url, f'/api/categories/{quote(category_slug)}?dataset={dataset_row["id"]}') if category_slug else None
        category_tree = []
        if category_name:
            top = infer_top_category(category_name)
            if top and top != category_name:
                category_tree.append({'name': top, 'url': category_url})
            category_tree.append({'name': category_name, 'url': category_url})
        s3_object = self._s3_object_for(dataset_row['id'], product_id)
        s3_url = s3_object.get('s3_url') if s3_object else None
        images = image_urls[:20]
        primary_image = s3_url or (images[0] if images else None)
        return {
            'goods_id': goods_id,
            'goods_sn': product_id,
            'spu': product_id,
            'category_id': None,
            'name': name,
            'brand': (row['brand'] or None) if row['brand'] else None,
            'color': (row['color'] or None) if row['color'] else None,
            'size': (row['size_text'] or None) if row['size_text'] else None,
            'description': description,
            'retail_price': to_money(row['price']),
            'sale_price': to_money(row['price']),
            'currency': 'USD',
            'in_stock': bool(images),
            'stock_quantity': 1 if images else 0,
            'images': [primary_image, *[u for u in images if u != primary_image]] if primary_image else images,
            'category_url': category_url,
            'product_url': row['url'] or None,
            'category_tree': category_tree or None,
            'country_code': 'US',
            'domain': urlparse(row['url']).netloc or None if row['url'] else None,
            'image_count': len(images),
            'offers': row['price_text'] or None,
            'attributes': [
                {'name': 'brand', 'value': (row['brand'] or None) if row['brand'] else None},
                {'name': 'color', 'value': (row['color'] or None) if row['color'] else None},
                {'name': 'size', 'value': (row['size_text'] or None) if row['size_text'] else None},
                {'name': 'source', 'value': row['source']},
                {'name': 'dataset_id', 'value': dataset_row['id']},
            ],
            'root_category': infer_top_category(category_name) if category_name else None,
            'related_products': None,
            'top_reviews': None,
            'store_name': dataset_row['label'],
            'rating': to_money(row['rating'], default='0.00'),
            'reviews_count': int(row['reviews_count'] or 0),
            'is_free_shipping': bool(images),
            'available_sizes': parse_json_list(row['sizes_json']) or split_sizes(row['size_text']) or None,
            'category_details': {
                'category_id': row['category'] or None,
                'goods_id': goods_id,
                'level': 1 if category_name else None,
                'name': category_name,
                'url': category_url,
            },
            'discount_price': to_money(row['price']),
            'discount_price_usd': to_money(row['price']),
            'colors': [(row['color'] or None)] if row['color'] else None,
            'store_details': {
                'code': dataset_row['id'],
                'followers': None,
                'items': None,
                'name': dataset_row['label'],
            },
            'shipping_details': None,
            'shipping_type': None,
            'tags': [t for t in [dataset_row['label'], row['source'], category_name, row['brand'], row['color']] if t],
            'model_data': None,
            'saved_on_s3': bool(s3_url),
            's3_url': s3_url,
        }

    def handle_datasets(self, query_string):
        params = parse_qs(query_string)
        dataset_id = (params.get('dataset', [''])[0] or '').strip().lower()
        conn = db_connect()
        if dataset_id:
            rows = conn.execute('SELECT * FROM datasets WHERE id = ?', (dataset_id,)).fetchall()
        else:
            rows = conn.execute('SELECT * FROM datasets ORDER BY id').fetchall()
        conn.close()
        json_response(self, {'datasets': [dict(row) for row in rows]})

    def handle_categories(self, query_string):
        params = parse_qs(query_string)
        dataset_id = (params.get('dataset', ['shein'])[0] or 'shein').strip().lower()
        if dataset_id not in ALLOWED_DATASETS:
            raise ValueError(f'Unknown dataset: {dataset_id}')
        page = parse_positive_int(params.get('page', ['1'])[0], 1)
        page_size = parse_positive_int(params.get('pageSize', ['100'])[0], 100, maximum=MAX_PAGE_SIZE)
        conn = db_connect()
        dataset_row = self._dataset_row(conn, dataset_id)
        rows = self._category_rows(conn, dataset_id)
        conn.close()
        total = len(rows)
        total_pages = max(1, math.ceil(total / page_size))
        page = min(page, total_pages)
        start = (page - 1) * page_size
        end = start + page_size
        base_url = get_base_url(self)
        data = [self._category_resource(row) | {'count': row['count']} for row in rows[start:end]]
        json_response(self, {'dataset': dict(dataset_row), 'data': data, 'pagination': {'page': page, 'pageSize': page_size, 'total': total, 'totalPages': total_pages, 'from': 0 if total == 0 else start + 1, 'to': min(end, total)}})

    def handle_category(self, slug, query_string):
        params = parse_qs(query_string)
        dataset_id = (params.get('dataset', ['shein'])[0] or 'shein').strip().lower()
        if dataset_id not in ALLOWED_DATASETS:
            raise ValueError(f'Unknown dataset: {dataset_id}')
        conn = db_connect()
        dataset_row = self._dataset_row(conn, dataset_id)
        rows = self._category_rows(conn, dataset_id)
        conn.close()
        target = None
        for row in rows:
            resource = self._category_resource(row)
            if resource['slug'] == slug:
                target = resource | {'count': row['count']}
                break
        if not target:
            return error_response(self, f'Category not found: {slug}', HTTPStatus.NOT_FOUND, code='not_found')
        json_response(self, {'dataset': dict(dataset_row), 'data': target})

    def handle_products(self, query_string):
        params = parse_qs(query_string)
        dataset_id = (params.get('dataset', ['shein'])[0] or 'shein').strip().lower()
        if dataset_id not in ALLOWED_DATASETS:
            raise ValueError(f'Unknown dataset: {dataset_id}')
        search = (params.get('search', [''])[0] or '').strip().lower()
        category = (params.get('category', [''])[0] or '').strip()
        sort = (params.get('sort', ['relevance'])[0] or 'relevance').strip()
        images_only = parse_bool(params.get('imagesOnly', ['false'])[0])
        page = parse_positive_int(params.get('page', ['1'])[0], 1)
        page_size = parse_positive_int(params.get('pageSize', [str(DEFAULT_PAGE_SIZE)])[0], DEFAULT_PAGE_SIZE, maximum=MAX_PAGE_SIZE)
        format_mode = (params.get('format', ['legacy'])[0] or 'legacy').strip().lower()
        if format_mode not in {'legacy', 'resource'}:
            raise ValueError('format must be legacy or resource')

        where = ['p.dataset_id = ?']
        values = [dataset_id]
        if search:
            where.append('p.search_text LIKE ?')
            values.append(f'%{search}%')
        if category:
            where.append('(p.category LIKE ? OR p.category_path LIKE ?)')
            values.extend([f'%{category}%', f'%{category}%'])
        if images_only:
            where.append("p.image <> ''")
        where_sql = ' AND '.join(where)
        order_sql = ALLOWED_SORTS.get(sort, ALLOWED_SORTS['relevance'])
        conn = db_connect()
        dataset_row = self._dataset_row(conn, dataset_id)
        total = conn.execute(f'SELECT COUNT(*) FROM products p WHERE {where_sql}', values).fetchone()[0]
        total_pages = max(1, math.ceil(total / page_size))
        page = min(page, total_pages)
        offset = (page - 1) * page_size
        product_rows = conn.execute(
            f'''
            SELECT p.*, COALESCE(s.ok, 0) AS image_ok
            FROM products p
            LEFT JOIN image_status s ON s.dataset_id = p.dataset_id AND s.product_id = p.id
            WHERE {where_sql}
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?
            ''',
            [*values, page_size, offset],
        ).fetchall()
        category_rows = self._category_rows(conn, dataset_id)
        conn.close()
        base_url = get_base_url(self)
        resource_products = [self._product_resource(row, dataset_row, base_url) for row in product_rows]

        if format_mode == 'resource':
            json_response(self, {'dataset': dict(dataset_row), 'data': resource_products, 'pagination': {'page': page, 'pageSize': page_size, 'total': total, 'totalPages': total_pages, 'from': 0 if total == 0 else offset + 1, 'to': min(offset + page_size, total)}, 'categories': [self._category_resource(row) | {'count': row['count']} for row in category_rows if row['name']]})
            return

        legacy_products = []
        for row, product in zip(product_rows, resource_products):
            legacy = dict(row)
            legacy['sizes'] = parse_json_list(legacy.pop('sizes_json') or '[]')
            legacy['imageUrls'] = parse_json_list(legacy.pop('image_urls_json') or '[]')
            legacy['image_ok'] = bool(legacy.get('image_ok'))
            legacy['goods_id'] = product['goods_id']
            legacy['saved_on_s3'] = product['saved_on_s3']
            legacy['s3_url'] = product['s3_url']
            legacy_products.append(legacy)

        json_response(self, {'dataset': dict(dataset_row), 'products': legacy_products, 'pagination': {'page': page, 'pageSize': page_size, 'total': total, 'totalPages': total_pages, 'from': 0 if total == 0 else offset + 1, 'to': min(offset + page_size, total)}, 'categories': [{'name': row['name'], 'count': row['count']} for row in category_rows if row['name']]})

    def handle_product(self, goods_id, query_string):
        params = parse_qs(query_string)
        dataset_id = (params.get('dataset', ['shein'])[0] or 'shein').strip().lower()
        if dataset_id not in ALLOWED_DATASETS:
            raise ValueError(f'Unknown dataset: {dataset_id}')
        conn = db_connect()
        dataset_row = self._dataset_row(conn, dataset_id)
        row = conn.execute('SELECT * FROM products WHERE dataset_id = ? AND id = ?', (dataset_id, goods_id)).fetchone()
        conn.close()
        if not row:
            return error_response(self, f'Product not found: {goods_id}', HTTPStatus.NOT_FOUND, code='not_found')
        json_response(self, {'dataset': dict(dataset_row), 'data': self._product_resource(row, dataset_row, get_base_url(self))})

    def handle_s3_config_get(self):
        load_s3_state()
        config = dict(S3_STATE.get('config', {}))
        config.pop('aws_access_key_id', None)
        config.pop('aws_secret_access_key', None)
        config.pop('aws_session_token', None)
        json_response(self, {'data': config})

    def handle_s3_config_update(self):
        payload = self._read_json_body()
        config = S3_STATE.setdefault('config', {})
        for key in ['region_name', 'bucket', 'prefix', 'endpoint_url']:
            if key in payload:
                config[key] = payload[key]
        # Never accept or persist secrets through the API.
        for secret_key in ('aws_access_key_id', 'aws_secret_access_key', 'aws_session_token'):
            config.pop(secret_key, None)
        save_s3_state()
        json_response(self, {'data': config})

    def handle_s3_jobs_list(self):
        config = dict(S3_STATE.get('config', {}))
        config.pop('aws_access_key_id', None)
        config.pop('aws_secret_access_key', None)
        config.pop('aws_session_token', None)
        json_response(self, {'data': S3_JOB_MANAGER.list_jobs(), 'config': config})

    def handle_s3_jobs_create(self):
        payload = self._read_json_body()
        dataset_id = (payload.get('dataset_id') or 'shein').strip().lower()
        source = (payload.get('source') or 'products').strip().lower()
        limit = parse_positive_int(payload.get('limit', 100), 100)
        concurrency = parse_positive_int(payload.get('concurrency', 4), 4, maximum=24)
        bucket = (payload.get('bucket') or S3_STATE.get('config', {}).get('bucket') or '').strip()
        prefix = (payload.get('prefix') or S3_STATE.get('config', {}).get('prefix') or '').strip()
        if dataset_id not in ALLOWED_DATASETS:
            raise ValueError(f'Unknown dataset: {dataset_id}')
        if not bucket:
            raise ValueError('Missing S3 bucket')
        conn = db_connect()
        rows = conn.execute('SELECT * FROM products WHERE dataset_id = ? ORDER BY id ASC', (dataset_id,)).fetchall()
        conn.close()
        selected = [dict(row) for row in rows[:limit]]
        job_id = f'{dataset_id}-{int(time.time())}'
        load_s3_state()
        config = dict(S3_STATE.get('config', {}))
        config.update({k: v for k, v in payload.items() if k in {'region_name', 'endpoint_url'}})
        config['bucket'] = bucket
        config['prefix'] = prefix
        for secret_key in ('aws_access_key_id', 'aws_secret_access_key', 'aws_session_token'):
            config.pop(secret_key, None)
        S3_STATE['config'] = config
        save_s3_state()

        def s3_client_factory():
            import boto3
            env_access_key = os.getenv('AWS_ACCESS_KEY_ID') or os.getenv('AWS_ACCESS_KEY')
            env_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY') or os.getenv('AWS_SECRET_KEY')
            env_session_token = os.getenv('AWS_SESSION_TOKEN')
            session = boto3.session.Session(
                aws_access_key_id=env_access_key or None,
                aws_secret_access_key=env_secret_key or None,
                aws_session_token=env_session_token or None,
                region_name=(config.get('region_name') or os.getenv('AWS_REGION') or os.getenv('AWS_DEFAULT_REGION') or None),
            )
            return session.client('s3', endpoint_url=config.get('endpoint_url') or None)

        def resolve_source_url(row):
            urls = [u for u in ([row.get('image')] + parse_json_list(row.get('image_urls_json'))) if isinstance(u, str) and u.strip()]
            return urls[0] if urls else row.get('url')

        def on_uploaded(row, source_url, key):
            load_s3_state()
            goods_id = normalize_goods_id(dataset_id, row.get('id'))
            s3_url = f's3://{bucket}/{key}'
            S3_STATE.setdefault('objects', {})[goods_id] = {
                'dataset_id': dataset_id,
                'product_id': str(row.get('id')),
                'goods_id': goods_id,
                'source_url': source_url,
                's3_url': s3_url,
                'bucket': bucket,
                'key': key,
                'saved_at': time.time(),
            }
            save_s3_state()

        future = S3_JOB_MANAGER.start_job(
            job_id=job_id,
            dataset_id=dataset_id,
            source=source,
            bucket=bucket,
            prefix=prefix,
            limit=limit,
            concurrency=concurrency,
            source_filter=payload.get('source_filter'),
            rows=selected,
            s3_client_factory=s3_client_factory,
            resolve_source_url=resolve_source_url,
            on_uploaded=on_uploaded,
        )
        json_response(self, {'data': S3_JOB_MANAGER.get_job(job_id), 'future': bool(future)}, status=HTTPStatus.ACCEPTED)

    def handle_s3_job_cancel(self, job_id):
        if not S3_JOB_MANAGER.cancel_job(job_id):
            return error_response(self, f'Job not found: {job_id}', HTTPStatus.NOT_FOUND, code='not_found')
        json_response(self, {'data': S3_JOB_MANAGER.get_job(job_id)}, status=HTTPStatus.ACCEPTED)


def find_available_port(host: str, preferred_port: int, attempts: int = 20) -> int:
    for port in range(preferred_port, preferred_port + attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind((host, port))
                return port
            except OSError:
                continue
    raise OSError(f'No free port found in range {preferred_port}-{preferred_port + attempts - 1}')


if __name__ == '__main__':
    host = os.getenv('FAST_FASHION_HOST', '127.0.0.1')
    preferred_port = int(os.getenv('FAST_FASHION_PORT', '8765'))
    cleanup_previous_servers()
    port = find_available_port(host, preferred_port)
    server = ThreadingHTTPServer((host, port), Handler)
    if port != preferred_port:
        print(f'Port {preferred_port} busy, using {port} instead.')
    print(f'Serving on http://{host}:{port}')
    server.serve_forever()

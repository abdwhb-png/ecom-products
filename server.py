#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import os
import socket
import sqlite3
import traceback
from collections import defaultdict
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, urlparse

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / 'catalog.db'
ALLOWED_DATASETS = {'shein', 'asos'}
DEFAULT_PAGE_SIZE = 24
MAX_PAGE_SIZE = 200
API_PREFIX = '/api'

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
    'Access-Control-Allow-Methods': 'GET, OPTIONS',
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
        'version': '1.0.0',
        'description': 'Stable read-only API exposing categories and products for external consumers.',
    },
    'servers': [{'url': '/'}],
    'paths': {
        '/api/categories': {
            'get': {
                'summary': 'List categories',
                'parameters': [
                    {'name': 'dataset', 'in': 'query', 'schema': {'type': 'string', 'enum': ['shein', 'asos']}},
                    {'name': 'page', 'in': 'query', 'schema': {'type': 'integer', 'minimum': 1, 'default': 1}},
                    {'name': 'pageSize', 'in': 'query', 'schema': {'type': 'integer', 'minimum': 1, 'maximum': 200, 'default': 100}},
                ],
                'responses': {'200': {'description': 'Category list'}},
            }
        },
        '/api/categories/{slug}': {
            'get': {
                'summary': 'Get a single category',
                'parameters': [
                    {'name': 'slug', 'in': 'path', 'required': True, 'schema': {'type': 'string'}},
                    {'name': 'dataset', 'in': 'query', 'schema': {'type': 'string', 'enum': ['shein', 'asos']}},
                ],
                'responses': {'200': {'description': 'Category resource'}},
            }
        },
        '/api/products': {
            'get': {
                'summary': 'List products',
                'parameters': [
                    {'name': 'dataset', 'in': 'query', 'schema': {'type': 'string', 'enum': ['shein', 'asos']}},
                    {'name': 'search', 'in': 'query', 'schema': {'type': 'string'}},
                    {'name': 'category', 'in': 'query', 'schema': {'type': 'string'}},
                    {'name': 'sort', 'in': 'query', 'schema': {'type': 'string', 'enum': list(ALLOWED_SORTS)}},
                    {'name': 'imagesOnly', 'in': 'query', 'schema': {'type': 'boolean', 'default': False}},
                    {'name': 'page', 'in': 'query', 'schema': {'type': 'integer', 'minimum': 1, 'default': 1}},
                    {'name': 'pageSize', 'in': 'query', 'schema': {'type': 'integer', 'minimum': 1, 'maximum': 200, 'default': 24}},
                    {'name': 'format', 'in': 'query', 'schema': {'type': 'string', 'enum': ['legacy', 'resource'], 'default': 'legacy'}},
                ],
                'responses': {'200': {'description': 'Product list'}},
            }
        },
        '/api/products/{goods_id}': {
            'get': {
                'summary': 'Get a single product',
                'parameters': [
                    {'name': 'goods_id', 'in': 'path', 'required': True, 'schema': {'type': 'string'}},
                    {'name': 'dataset', 'in': 'query', 'schema': {'type': 'string', 'enum': ['shein', 'asos']}},
                ],
                'responses': {'200': {'description': 'Product resource'}},
            }
        },
    },
    'components': {
        'schemas': {
            'CategoryResource': {
                'type': 'object',
                'additionalProperties': False,
                'required': ['name', 'slug'],
                'properties': {
                    'name': {'type': 'string', 'minLength': 1},
                    'slug': {'type': 'string', 'minLength': 1},
                    'top_category_name': {'type': ['string', 'null']},
                    'source_url': {'type': ['string', 'null'], 'format': 'uri'},
                    'image_url': {'type': ['string', 'null'], 'format': 'uri'},
                },
            },
            'CategoryTreeItem': {
                'type': 'object',
                'additionalProperties': True,
                'required': ['name'],
                'properties': {'name': {'type': 'string', 'minLength': 1}, 'url': {'type': ['string', 'null'], 'format': 'uri'}},
            },
            'ProductAttribute': {
                'type': 'object',
                'additionalProperties': True,
                'properties': {'name': {'type': ['string', 'null']}, 'value': {'oneOf': [{'type': 'string'}, {'type': 'number'}, {'type': 'integer'}, {'type': 'boolean'}, {'type': 'array'}, {'type': 'object'}, {'type': 'null'}]}},
            },
            'ProductCategoryDetails': {
                'type': 'object',
                'additionalProperties': True,
                'properties': {
                    'category_id': {'oneOf': [{'type': 'string'}, {'type': 'integer'}, {'type': 'null'}]},
                    'goods_id': {'oneOf': [{'type': 'string'}, {'type': 'integer'}, {'type': 'null'}]},
                    'level': {'oneOf': [{'type': 'integer'}, {'type': 'null'}]},
                    'name': {'type': ['string', 'null']},
                    'url': {'type': ['string', 'null'], 'format': 'uri'},
                },
            },
            'ProductStoreDetails': {
                'type': 'object',
                'additionalProperties': True,
                'properties': {
                    'code': {'oneOf': [{'type': 'string'}, {'type': 'integer'}, {'type': 'null'}]},
                    'followers': {'oneOf': [{'type': 'integer'}, {'type': 'number'}, {'type': 'null'}]},
                    'items': {'oneOf': [{'type': 'integer'}, {'type': 'number'}, {'type': 'null'}]},
                    'name': {'type': ['string', 'null']},
                },
            },
            'ProductShippingDetail': {
                'type': 'object',
                'additionalProperties': True,
                'properties': {
                    'currency': {'type': ['string', 'null']},
                    'free_shipping_price': {'oneOf': [{'type': 'number'}, {'type': 'integer'}, {'type': 'null'}]},
                    'method': {'type': ['string', 'null']},
                    'shipping_dates_range': {'oneOf': [{'type': 'string'}, {'type': 'array'}, {'type': 'object'}, {'type': 'null'}]},
                    'shipping_price': {'oneOf': [{'type': 'number'}, {'type': 'integer'}, {'type': 'null'}]},
                },
            },
            'ProductResource': {
                'type': 'object',
                'additionalProperties': False,
                'required': ['goods_id', 'goods_sn', 'spu', 'name', 'description', 'retail_price', 'sale_price', 'stock_quantity', 'images', 'attributes', 'store_name', 'rating', 'reviews_count', 'is_free_shipping'],
            },
        }
    },
}


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


def json_response(handler, payload, status=HTTPStatus.OK, headers=None):
    body = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    handler.send_response(status)
    handler.send_header('Content-Type', 'application/json; charset=utf-8')
    handler.send_header('Content-Length', str(len(body)))
    handler.send_header('Cache-Control', 'no-store')
    for key, value in CORS_HEADERS.items():
        handler.send_header(key, value)
    if headers:
        for key, value in headers.items():
            handler.send_header(key, value)
    handler.end_headers()
    handler.wfile.write(body)


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
        if value is None:
            return default
        return f'{float(value):.2f}'
    except Exception:
        return default


def format_currency_code(value):
    if not value:
        return None
    value = str(value).strip().upper()
    return value if value else None


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
    if host:
        scheme = 'https' if handler.headers.get('X-Forwarded-Proto', '').lower() == 'https' else 'http'
        return f'{scheme}://{host}'
    return ''


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


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def log_message(self, format, *args):
        super().log_message(format, *args)

    def end_headers(self):
        for key, value in CORS_HEADERS.items():
            self.send_header(key, value)
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header('Content-Length', '0')
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        try:
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
            return super().do_GET()
        except sqlite3.Error as exc:
            return error_response(self, f'Database error: {exc}', HTTPStatus.INTERNAL_SERVER_ERROR, code='database_error')
        except ValueError as exc:
            return error_response(self, str(exc), HTTPStatus.BAD_REQUEST, code='invalid_request')
        except Exception as exc:
            traceback.print_exc()
            return error_response(self, f'Internal server error: {exc}', HTTPStatus.INTERNAL_SERVER_ERROR, code='internal_error')

    def handle_datasets(self, query_string):
        params = parse_qs(query_string)
        requested = (params.get('dataset', [''])[0] or '').strip()
        conn = db_connect()
        if requested:
            rows = conn.execute('SELECT * FROM datasets WHERE id = ?', (requested,)).fetchall()
        else:
            rows = conn.execute('SELECT * FROM datasets ORDER BY id').fetchall()
        conn.close()
        json_response(self, {'datasets': [dict(row) for row in rows]})

    def _normalize_dataset(self, requested):
        dataset_id = (requested or 'shein').strip().lower()
        if dataset_id not in ALLOWED_DATASETS:
            raise ValueError(f'Unknown dataset: {dataset_id}')
        return dataset_id

    def _get_dataset_row(self, conn, dataset_id):
        row = conn.execute('SELECT * FROM datasets WHERE id = ?', (dataset_id,)).fetchone()
        if not row:
            raise ValueError(f'Dataset not found: {dataset_id}')
        return row

    def _category_rows(self, conn, dataset_id):
        return conn.execute(
            '''
            SELECT
                p.category AS name,
                MIN(p.id) AS first_product_id,
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

    def _category_resource(self, row, base_url, dataset_id):
        name = (row['name'] or '').strip()
        slug = make_slug(name)
        return {
            'name': name,
            'slug': slug,
            'top_category_name': infer_top_category(name),
            'source_url': row['source_url'] or None,
            'image_url': row['image_url'] or None,
        }

    def _product_resource(self, row, dataset_row, base_url):
        product_id = str(row['id']).strip()
        goods_id = f"{dataset_row['id']}:{product_id}"
        goods_sn = product_id
        spu = product_id
        name = (row['name'] or '').strip() or 'Sans nom'
        description = (row['description'] or '').strip() or 'Imported from scraper feed.'
        image_urls = [u for u in ([row['image']] + parse_json_list(row['image_urls_json'])) if isinstance(u, str) and u.strip()]
        image_urls = list(dict.fromkeys(image_urls))
        if not image_urls:
            image_urls = []
        category_name = (row['category'] or '').strip() or None
        category_slug = make_slug(category_name) if category_name else None
        category_url = safe_url(base_url, f'/api/categories/{quote(category_slug)}?dataset={dataset_row["id"]}') if category_slug else None
        category_tree = []
        if category_name:
            top = infer_top_category(category_name)
            if top and top != category_name:
                category_tree.append({'name': top, 'url': category_url})
            category_tree.append({'name': category_name, 'url': category_url})
        images = image_urls[:20]
        price = to_money(row['price'])
        rating = to_money(row['rating'], default='0.00')
        stock_quantity = 1 if images else 0
        in_stock = bool(stock_quantity)
        brand = (row['brand'] or None) if row['brand'] else None
        color = (row['color'] or None) if row['color'] else None
        size_text = (row['size_text'] or None) if row['size_text'] else None
        available_sizes = parse_json_list(row['sizes_json']) or split_sizes(size_text)
        tags = [t for t in [dataset_row['label'], row['source'], category_name, brand, color] if t]
        attributes = [
            {'name': 'brand', 'value': brand},
            {'name': 'color', 'value': color},
            {'name': 'size', 'value': size_text},
            {'name': 'source', 'value': row['source']},
            {'name': 'dataset_id', 'value': dataset_row['id']},
        ]
        store_details = {
            'code': dataset_row['id'],
            'followers': None,
            'items': None,
            'name': dataset_row['label'],
        }
        shipping_details = []
        category_details = {
            'category_id': row['category'] or None,
            'goods_id': goods_id,
            'level': 1 if category_name else None,
            'name': category_name,
            'url': category_url,
        }
        return {
            'goods_id': goods_id,
            'goods_sn': goods_sn,
            'spu': spu,
            'category_id': None,
            'name': name,
            'brand': brand,
            'color': color,
            'size': size_text,
            'description': description,
            'retail_price': price,
            'sale_price': price,
            'currency': 'USD',
            'in_stock': in_stock,
            'stock_quantity': stock_quantity,
            'images': images,
            'category_url': category_url,
            'product_url': row['url'] or None,
            'category_tree': category_tree or None,
            'country_code': 'US',
            'domain': urlparse(row['url']).netloc or None if row['url'] else None,
            'image_count': len(images),
            'offers': row['price_text'] or None,
            'attributes': attributes,
            'root_category': infer_top_category(category_name) if category_name else None,
            'related_products': None,
            'top_reviews': None,
            'store_name': dataset_row['label'],
            'rating': rating,
            'reviews_count': int(row['reviews_count'] or 0),
            'is_free_shipping': bool(images),
            'available_sizes': available_sizes or None,
            'category_details': category_details,
            'discount_price': price,
            'discount_price_usd': price,
            'colors': [color] if color else None,
            'store_details': store_details,
            'shipping_details': shipping_details,
            'shipping_type': None,
            'tags': tags or None,
            'model_data': None,
        }

    def handle_categories(self, query_string):
        params = parse_qs(query_string)
        dataset_id = self._normalize_dataset(params.get('dataset', ['shein'])[0])
        page = parse_positive_int(params.get('page', ['1'])[0], 1)
        page_size = parse_positive_int(params.get('pageSize', ['100'])[0], 100, maximum=MAX_PAGE_SIZE)
        conn = db_connect()
        dataset_row = self._get_dataset_row(conn, dataset_id)
        rows = self._category_rows(conn, dataset_id)
        conn.close()

        total = len(rows)
        total_pages = max(1, math.ceil(total / page_size))
        page = min(page, total_pages)
        start = (page - 1) * page_size
        end = start + page_size
        base_url = get_base_url(self)
        resources = [self._category_resource(row, base_url, dataset_id) for row in rows[start:end]]

        json_response(
            self,
            {
                'dataset': dict(dataset_row),
                'data': resources,
                'pagination': {
                    'page': page,
                    'pageSize': page_size,
                    'total': total,
                    'totalPages': total_pages,
                    'from': 0 if total == 0 else start + 1,
                    'to': min(end, total),
                },
            },
        )

    def handle_category(self, slug, query_string):
        params = parse_qs(query_string)
        dataset_id = self._normalize_dataset(params.get('dataset', ['shein'])[0])
        conn = db_connect()
        dataset_row = self._get_dataset_row(conn, dataset_id)
        rows = self._category_rows(conn, dataset_id)
        conn.close()

        target = None
        for row in rows:
            resource = self._category_resource(row, get_base_url(self), dataset_id)
            if resource['slug'] == slug:
                target = resource
                break
        if not target:
            return error_response(self, f'Category not found: {slug}', HTTPStatus.NOT_FOUND, code='not_found')

        json_response(self, {'dataset': dict(dataset_row), 'data': target})

    def _product_query(self, params):
        dataset_id = self._normalize_dataset(params.get('dataset', ['shein'])[0])
        search = (params.get('search', [''])[0] or '').strip().lower()
        category = (params.get('category', [''])[0] or '').strip()
        sort = (params.get('sort', ['relevance'])[0] or 'relevance').strip()
        images_only = parse_bool(params.get('imagesOnly', ['false'])[0])
        page = parse_positive_int(params.get('page', ['1'])[0], 1)
        page_size = parse_positive_int(params.get('pageSize', [str(DEFAULT_PAGE_SIZE)])[0], DEFAULT_PAGE_SIZE, maximum=MAX_PAGE_SIZE)
        format_mode = (params.get('format', ['legacy'])[0] or 'legacy').strip().lower()
        if format_mode not in {'legacy', 'resource'}:
            raise ValueError('format must be legacy or resource')
        return dataset_id, search, category, sort, images_only, page, page_size, format_mode

    def handle_products(self, query_string):
        params = parse_qs(query_string)
        dataset_id, search, category, sort, images_only, page, page_size, format_mode = self._product_query(params)
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
        dataset_row = self._get_dataset_row(conn, dataset_id)
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
        products = [self._product_resource(row, dataset_row, base_url) for row in product_rows]
        if format_mode == 'resource':
            json_response(
                self,
                {
                    'dataset': dict(dataset_row),
                    'data': products,
                    'pagination': {
                        'page': page,
                        'pageSize': page_size,
                        'total': total,
                        'totalPages': total_pages,
                        'from': 0 if total == 0 else offset + 1,
                        'to': min(offset + page_size, total),
                    },
                    'categories': [self._category_resource(row, base_url, dataset_id) | {'count': row['count']} for row in category_rows if row['name']],
                },
            )
            return

        legacy_products = []
        for row, product in zip(product_rows, products):
            legacy = dict(row)
            legacy['sizes'] = parse_json_list(legacy.pop('sizes_json') or '[]')
            legacy['imageUrls'] = parse_json_list(legacy.pop('image_urls_json') or '[]')
            legacy['image_ok'] = bool(legacy.get('image_ok'))
            legacy['goods_id'] = product['goods_id']
            legacy_products.append(legacy)

        json_response(
            self,
            {
                'dataset': dict(dataset_row),
                'products': legacy_products,
                'pagination': {
                    'page': page,
                    'pageSize': page_size,
                    'total': total,
                    'totalPages': total_pages,
                    'from': 0 if total == 0 else offset + 1,
                    'to': min(offset + page_size, total),
                },
                'categories': [{'name': row['name'], 'count': row['count']} for row in category_rows if row['name']],
            },
        )

    def handle_product(self, goods_id, query_string):
        params = parse_qs(query_string)
        dataset_id = self._normalize_dataset(params.get('dataset', ['shein'])[0])
        conn = db_connect()
        dataset_row = self._get_dataset_row(conn, dataset_id)
        row = conn.execute('SELECT * FROM products WHERE dataset_id = ? AND id = ?', (dataset_id, goods_id)).fetchone()
        conn.close()
        if not row:
            return error_response(self, f'Product not found: {goods_id}', HTTPStatus.NOT_FOUND, code='not_found')
        product = self._product_resource(row, dataset_row, get_base_url(self))
        json_response(self, {'dataset': dict(dataset_row), 'data': product})


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
    port = find_available_port(host, preferred_port)
    server = ThreadingHTTPServer((host, port), Handler)
    if port != preferred_port:
        print(f'Port {preferred_port} busy, using {port} instead.')
    print(f'Serving on http://{host}:{port}')
    server.serve_forever()

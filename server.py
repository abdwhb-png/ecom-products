#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import os
import socket
import sqlite3
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / 'catalog.db'

ALLOWED_SORTS = {
    'relevance': 'COALESCE(s.ok, 0) DESC, image_count DESC, rowid ASC',
    'price-asc': 'price ASC NULLS LAST',
    'price-desc': 'price DESC NULLS LAST',
    'rating-desc': 'rating DESC NULLS LAST',
    'reviews-desc': 'reviews_count DESC NULLS LAST',
    'name-asc': 'name COLLATE NOCASE ASC',
}


def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def end_json(self, payload, status=200):
        body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == '/api/datasets':
            return self.handle_datasets()
        if parsed.path == '/api/products':
            return self.handle_products(parsed.query)
        return super().do_GET()

    def handle_datasets(self):
        conn = db_connect()
        rows = conn.execute('SELECT * FROM datasets ORDER BY id').fetchall()
        conn.close()
        self.end_json({'datasets': [dict(row) for row in rows]})

    def handle_products(self, query_string):
        params = parse_qs(query_string)
        dataset_id = (params.get('dataset', ['shein'])[0] or 'shein').strip()
        search = (params.get('search', [''])[0] or '').strip().lower()
        category = (params.get('category', [''])[0] or '').strip()
        sort = (params.get('sort', ['relevance'])[0] or 'relevance').strip()
        images_only = (params.get('imagesOnly', ['false'])[0] or '').lower() in {'1', 'true', 'yes', 'on'}
        page = max(1, int(params.get('page', ['1'])[0] or '1'))
        page_size = max(1, min(200, int(params.get('pageSize', ['24'])[0] or '24')))

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

        category_rows = conn.execute(
            '''
            SELECT p.category, COUNT(*) AS count
            FROM products p
            WHERE p.dataset_id = ?
            GROUP BY p.category
            ORDER BY count DESC, p.category COLLATE NOCASE ASC
            ''',
            (dataset_id,),
        ).fetchall()

        dataset_row = conn.execute('SELECT * FROM datasets WHERE id = ?', (dataset_id,)).fetchone()
        conn.close()

        products = []
        for row in product_rows:
            item = dict(row)
            item['sizes'] = json.loads(item.pop('sizes_json') or '[]')
            item['imageUrls'] = json.loads(item.pop('image_urls_json') or '[]')
            item['image_ok'] = bool(item.get('image_ok'))
            products.append(item)

        self.end_json(
            {
                'dataset': dict(dataset_row) if dataset_row else None,
                'products': products,
                'pagination': {
                    'page': page,
                    'pageSize': page_size,
                    'total': total,
                    'totalPages': total_pages,
                    'from': 0 if total == 0 else offset + 1,
                    'to': min(offset + page_size, total),
                },
                'categories': [{'name': row['category'], 'count': row['count']} for row in category_rows if row['category']],
            }
        )


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

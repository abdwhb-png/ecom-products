from __future__ import annotations

import math
from http import HTTPStatus
from urllib.parse import parse_qs, quote, unquote, urlparse

from backend.http_utils import error_response, json_response
from backend.text_utils import (
    get_base_url,
    infer_top_category,
    make_slug,
    normalize_goods_id,
    normalize_search_text,
    parse_bool,
    parse_json_list,
    parse_positive_int,
    safe_url,
    split_sizes,
    to_money,
)


def dataset_row(conn, dataset_id):
    row = conn.execute('SELECT * FROM datasets WHERE id = ?', (dataset_id,)).fetchone()
    if not row:
        raise ValueError(f'Dataset not found: {dataset_id}')
    return row


def category_rows(conn, dataset_id, search: str | None = None, saved_on_s3_only: bool = False):
    sql = '''
        SELECT
            p.category AS name,
            COUNT(*) AS count,
            MAX(CASE WHEN p.image <> '' THEN p.image ELSE NULL END) AS source_image_url,
            MAX(CASE WHEN p.url <> '' THEN p.url ELSE NULL END) AS source_url,
            MAX(CASE WHEN o.saved_on_s3 = 1 AND COALESCE(o.s3_url, '') <> '' THEN o.s3_url ELSE NULL END) AS s3_image_url,
            MAX(CASE WHEN o.saved_on_s3 = 1 THEN 1 ELSE 0 END) AS saved_on_s3,
            COUNT(DISTINCT CASE WHEN o.saved_on_s3 = 1 THEN p.id ELSE NULL END) AS saved_products_count,
            COALESCE(SUM(CASE WHEN o.saved_on_s3 = 1 THEN COALESCE(o.s3_image_count, 0) ELSE 0 END), 0) AS s3_image_count
        FROM products p
        LEFT JOIN s3_objects o ON o.dataset_id = p.dataset_id AND o.product_id = p.id
        WHERE p.dataset_id = ? AND COALESCE(p.category, '') <> ''
    '''
    params = [dataset_id]
    if search:
        sql += " AND (unaccent(p.category) LIKE ? OR unaccent(p.category_path) LIKE ?)"
        ns = normalize_search_text(search)
        params.extend([f'%{ns}%', f'%{ns}%'])
    sql += '\n        GROUP BY p.category\n    '
    if saved_on_s3_only:
        sql += '\n        HAVING COUNT(DISTINCT CASE WHEN o.saved_on_s3 = 1 THEN p.id ELSE NULL END) > 0\n        '
    sql += '\n        ORDER BY count DESC, p.category COLLATE NOCASE ASC\n        '
    return conn.execute(sql, params).fetchall()


def category_resource(row):
    name = (row['name'] or '').strip()
    source_image_url = row['source_image_url'] or None
    s3_image_url = row['s3_image_url'] or None
    return {
        'name': name,
        'slug': make_slug(name),
        'top_category_name': infer_top_category(name),
        'source_url': row['source_url'] or None,
        'image_url': s3_image_url or source_image_url,
        'source_image_url': source_image_url,
        's3_image_url': s3_image_url,
        'saved_on_s3': bool(row['saved_on_s3']),
        'saved_products_count': int(row['saved_products_count'] or 0),
        's3_image_count': int(row['s3_image_count'] or 0),
    }


def s3_object_for(conn, dataset_id, product_id):
    goods_id = normalize_goods_id(dataset_id, product_id)
    row = conn.execute(
        '''
        SELECT goods_id, dataset_id, product_id, source_url, s3_url, bucket, object_key,
               source_image_urls_json, s3_image_urls_json, image_pairs_json,
               source_image_count, s3_image_count, failed_image_count, saved_on_s3, saved_at
        FROM s3_objects
        WHERE goods_id = ?
        ''',
        (goods_id,),
    ).fetchone()
    if not row:
        return None
    return {
        'goods_id': row['goods_id'],
        'dataset_id': row['dataset_id'],
        'product_id': row['product_id'],
        'source_url': row['source_url'],
        's3_url': row['s3_url'],
        'bucket': row['bucket'],
        'key': row['object_key'],
        'source_image_urls': parse_json_list(row['source_image_urls_json']),
        's3_image_urls': parse_json_list(row['s3_image_urls_json']),
        'image_pairs': parse_json_list(row['image_pairs_json']),
        'source_image_count': int(row['source_image_count'] or 0),
        's3_image_count': int(row['s3_image_count'] or 0),
        'failed_image_count': int(row['failed_image_count'] or 0),
        'saved_on_s3': bool(row['saved_on_s3']),
        'saved_at': float(row['saved_at'] or 0),
    }


def source_images_for_row(row):
    urls = []
    for value in [row['image']] + parse_json_list(row['image_urls_json']):
        if not isinstance(value, str):
            continue
        cleaned = value.strip()
        if not cleaned or not cleaned.lower().startswith(('http://', 'https://')):
            continue
        urls.append(cleaned)
    return list(dict.fromkeys(urls))[:20]


def s3_images_for_product(s3_object, source_images):
    if not s3_object:
        return [], []
    image_pairs = []
    raw_pairs = s3_object.get('image_pairs') or []
    if isinstance(raw_pairs, list):
        for pair in raw_pairs:
            if not isinstance(pair, dict):
                continue
            source_url = str(pair.get('source_url') or '').strip()
            s3_url = str(pair.get('s3_url') or '').strip()
            if not source_url or not s3_url:
                continue
            image_pairs.append({
                'source_url': source_url,
                's3_url': s3_url,
                'key': pair.get('key'),
                'status': pair.get('status'),
            })
    if not image_pairs:
        source_url = str(s3_object.get('source_url') or '').strip()
        s3_url = str(s3_object.get('s3_url') or '').strip()
        if source_url and s3_url:
            image_pairs = [{
                'source_url': source_url,
                's3_url': s3_url,
                'key': s3_object.get('key'),
                'status': 'uploaded',
            }]
    by_source = {pair['source_url']: pair for pair in image_pairs}
    ordered_pairs = [by_source[url] for url in source_images if url in by_source]
    if not ordered_pairs and image_pairs:
        ordered_pairs = image_pairs
    s3_urls = [pair['s3_url'] for pair in ordered_pairs if pair.get('s3_url')]
    return ordered_pairs, s3_urls


def product_resource(row, dataset_row, base_url, *, db_connect_fn, conn=None):
    product_id = str(row['id']).strip()
    goods_id = normalize_goods_id(dataset_row['id'], product_id)
    name = (row['name'] or '').strip() or 'Sans nom'
    description = (row['description'] or '').strip() or 'Imported from scraper feed.'
    image_urls = source_images_for_row(row)
    category_name = (row['category'] or '').strip() or None
    category_slug = make_slug(category_name) if category_name else None
    category_url = safe_url(base_url, f'/api/categories/{quote(category_slug)}?dataset={dataset_row["id"]}') if category_slug else None
    category_tree = []
    if category_name:
        top = infer_top_category(category_name)
        if top and top != category_name:
            category_tree.append({'name': top, 'url': category_url})
        category_tree.append({'name': category_name, 'url': category_url})
    owns_conn = conn is None
    conn = conn or db_connect_fn()
    try:
        s3_object = s3_object_for(conn, dataset_row['id'], product_id)
    finally:
        if owns_conn:
            conn.close()
    image_pairs, s3_image_urls = s3_images_for_product(s3_object, image_urls)
    saved_on_s3 = bool(s3_object and s3_object.get('saved_on_s3'))
    s3_image_count = int((s3_object or {}).get('s3_image_count') or len(s3_image_urls))
    images = s3_image_urls if saved_on_s3 and s3_image_urls else image_urls
    primary_image = images[0] if images else None
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
        'image_count': len(image_urls),
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
        'source_image_urls': image_urls,
        's3_image_urls': s3_image_urls,
        'image_pairs': image_pairs,
        'saved_on_s3': saved_on_s3,
        's3_url': s3_image_urls[0] if s3_image_urls else None,
        's3_image_count': s3_image_count,
    }


def legacy_product_payload(row, dataset_row, base_url, *, db_connect_fn, resource_product=None):
    product = resource_product or product_resource(row, dataset_row, base_url, db_connect_fn=db_connect_fn)
    legacy = dict(row)
    legacy['sizes'] = parse_json_list(legacy.pop('sizes_json') or '[]')
    legacy['imageUrls'] = product['source_image_urls']
    legacy['sourceImageUrls'] = product['source_image_urls']
    legacy['s3ImageUrls'] = product['s3_image_urls']
    legacy['imagePairs'] = product['image_pairs']
    legacy['image_ok'] = bool(legacy.get('image_ok'))
    legacy['goods_id'] = product['goods_id']
    legacy['saved_on_s3'] = product['saved_on_s3']
    legacy['s3_url'] = product['s3_url']
    legacy['s3_image_count'] = product['s3_image_count']
    return legacy


def handle_datasets(handler, query_string, *, db_connect_fn):
    params = parse_qs(query_string)
    dataset_id = (params.get('dataset', [''])[0] or '').strip().lower()
    conn = db_connect_fn()
    try:
        if dataset_id:
            rows = conn.execute('SELECT * FROM datasets WHERE id = ?', (dataset_id,)).fetchall()
        else:
            rows = conn.execute('SELECT * FROM datasets ORDER BY id').fetchall()
    finally:
        conn.close()
    json_response(handler, {'datasets': [dict(row) for row in rows]})


def handle_categories(handler, query_string, *, db_connect_fn, allowed_datasets, max_page_size):
    params = parse_qs(query_string)
    dataset_id = (params.get('dataset', ['shein'])[0] or 'shein').strip().lower()
    if dataset_id not in allowed_datasets:
        raise ValueError(f'Unknown dataset: {dataset_id}')
    page = parse_positive_int(params.get('page', ['1'])[0], 1)
    page_size = parse_positive_int(params.get('pageSize', ['100'])[0], 100, maximum=max_page_size)
    search = (params.get('search', [''])[0] or '').strip()
    saved_on_s3_only = parse_bool(params.get('savedOnS3', ['false'])[0])
    conn = db_connect_fn()
    try:
        ds_row = dataset_row(conn, dataset_id)
        rows = category_rows(conn, dataset_id, search, saved_on_s3_only=saved_on_s3_only)
    finally:
        conn.close()
    total = len(rows)
    total_pages = max(1, math.ceil(total / page_size))
    page = min(page, total_pages)
    start = (page - 1) * page_size
    end = start + page_size
    data = [category_resource(row) | {'count': row['count']} for row in rows[start:end]]
    json_response(handler, {'dataset': dict(ds_row), 'data': data, 'pagination': {'page': page, 'pageSize': page_size, 'total': total, 'totalPages': total_pages, 'from': 0 if total == 0 else start + 1, 'to': min(end, total)}})


def handle_category(handler, slug, query_string, *, db_connect_fn, allowed_datasets):
    params = parse_qs(query_string)
    dataset_id = (params.get('dataset', ['shein'])[0] or 'shein').strip().lower()
    if dataset_id not in allowed_datasets:
        raise ValueError(f'Unknown dataset: {dataset_id}')
    saved_on_s3_only = parse_bool(params.get('savedOnS3', ['false'])[0])
    conn = db_connect_fn()
    try:
        ds_row = dataset_row(conn, dataset_id)
        rows = category_rows(conn, dataset_id, saved_on_s3_only=saved_on_s3_only)
    finally:
        conn.close()
    target = None
    for row in rows:
        resource = category_resource(row)
        if resource['slug'] == slug:
            target = resource | {'count': row['count']}
            break
    if not target:
        return error_response(handler, f'Category not found: {slug}', HTTPStatus.NOT_FOUND, code='not_found')
    json_response(handler, {'dataset': dict(ds_row), 'data': target})


def handle_products(handler, query_string, *, db_connect_fn, allowed_datasets, default_page_size, max_page_size, allowed_sorts):
    params = parse_qs(query_string)
    dataset_id = (params.get('dataset', ['shein'])[0] or 'shein').strip().lower()
    if dataset_id not in allowed_datasets:
        raise ValueError(f'Unknown dataset: {dataset_id}')
    search = (params.get('search', [''])[0] or '').strip().lower()
    category = (params.get('category', [''])[0] or '').strip()
    sort = (params.get('sort', ['relevance'])[0] or 'relevance').strip()
    images_only = parse_bool(params.get('imagesOnly', ['false'])[0])
    saved_on_s3_only = parse_bool(params.get('savedOnS3', ['false'])[0])
    page = parse_positive_int(params.get('page', ['1'])[0], 1)
    page_size = parse_positive_int(params.get('pageSize', [str(default_page_size)])[0], default_page_size, maximum=max_page_size)
    format_mode = (params.get('format', ['legacy'])[0] or 'legacy').strip().lower()
    if format_mode not in {'legacy', 'resource'}:
        raise ValueError('format must be legacy or resource')

    where = ['p.dataset_id = ?']
    values = [dataset_id]
    if search:
        where.append('p.search_text LIKE ?')
        values.append(f'%{search}%')
    if category:
        where.append('(unaccent(p.category) LIKE ? OR unaccent(p.category_path) LIKE ?)')
        nc = normalize_search_text(category)
        values.extend([f'%{nc}%', f'%{nc}%'])
    if images_only:
        where.append("p.image <> ''")
    if saved_on_s3_only:
        where.append("EXISTS (SELECT 1 FROM s3_objects o WHERE o.dataset_id = p.dataset_id AND o.product_id = p.id AND o.saved_on_s3 = 1)")
    where_sql = ' AND '.join(where)
    order_sql = allowed_sorts.get(sort, allowed_sorts['relevance'])
    conn = db_connect_fn()
    try:
        ds_row = dataset_row(conn, dataset_id)
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
        base_url = get_base_url(handler)
        resource_products = [product_resource(row, ds_row, base_url, db_connect_fn=db_connect_fn, conn=conn) for row in product_rows]
    finally:
        conn.close()

    if format_mode == 'resource':
        json_response(handler, {'dataset': dict(ds_row), 'data': resource_products, 'pagination': {'page': page, 'pageSize': page_size, 'total': total, 'totalPages': total_pages, 'from': 0 if total == 0 else offset + 1, 'to': min(offset + page_size, total)}})
        return

    legacy_products = [legacy_product_payload(row, ds_row, base_url, db_connect_fn=db_connect_fn, resource_product=product) for row, product in zip(product_rows, resource_products)]
    json_response(handler, {'dataset': dict(ds_row), 'products': legacy_products, 'pagination': {'page': page, 'pageSize': page_size, 'total': total, 'totalPages': total_pages, 'from': 0 if total == 0 else offset + 1, 'to': min(offset + page_size, total)}})


def handle_product(handler, goods_id, query_string, *, db_connect_fn, allowed_datasets):
    params = parse_qs(query_string)
    dataset_id = (params.get('dataset', ['shein'])[0] or 'shein').strip().lower()
    if dataset_id not in allowed_datasets:
        raise ValueError(f'Unknown dataset: {dataset_id}')
    requested_id = unquote(str(goods_id or '')).strip()
    product_id = requested_id.split(':', 1)[1] if ':' in requested_id else requested_id
    conn = db_connect_fn()
    try:
        ds_row = dataset_row(conn, dataset_id)
        row = conn.execute('SELECT * FROM products WHERE dataset_id = ? AND id = ?', (dataset_id, product_id)).fetchone()
        if not row:
            return error_response(handler, f'Product not found: {goods_id}', HTTPStatus.NOT_FOUND, code='not_found')
        base_url = get_base_url(handler)
        product_res = product_resource(row, ds_row, base_url, db_connect_fn=db_connect_fn, conn=conn)
        product_display = legacy_product_payload(row, ds_row, base_url, db_connect_fn=db_connect_fn, resource_product=product_res)
    finally:
        conn.close()
    json_response(handler, {'dataset': dict(ds_row), 'display': product_display, 'api': product_res, 'data': product_res})


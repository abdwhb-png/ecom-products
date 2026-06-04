from __future__ import annotations

import json
import unicodedata


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


def strip_accents(value: str) -> str:
    if value is None:
        return ''
    normalized = unicodedata.normalize('NFKD', str(value))
    return ''.join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_search_text(value: str) -> str:
    if value is None:
        return ''
    return strip_accents(str(value)).lower().strip()


def sql_unaccent(value):
    try:
        return normalize_search_text(value or '')
    except Exception:
        return ''


def make_slug(value):
    text = strip_accents((value or '').strip()).lower()
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


def normalize_goods_id(dataset_id, product_id):
    return f'{dataset_id}:{product_id}'


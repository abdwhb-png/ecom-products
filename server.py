#!/usr/bin/env python3
from __future__ import annotations

import json
import math
import os
import socket
import sqlite3
import time
import traceback
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlparse

from backend import catalog_routes, s3_routes
from backend.auth import (
    API_BEARER_TOKEN,
    S3_ADMIN_PASSWORD,
    S3_AUTH_TOKENS,
    S3_AUTH_TTL_SECONDS,
    api_token_is_valid,
    api_unauthorized_response,
    auth_required,
    get_bearer_token,
    handle_s3_auth as auth_handle_s3_auth,
    issue_s3_token,
    s3_access_is_valid,
    s3_admin_required_response,
    token_is_valid,
)
from backend.http_utils import error_response, html_response, json_response
from backend.public_assets import DOCS_PATH, PUBLIC_DIR, S3_PAGE_PATH
from backend.runtime_db import db_connect as db_connect_impl, health_status as health_status_impl
from backend.security import (
    CORS_HEADERS,
    MAX_REQUEST_BODY_BYTES,
    SECURITY_HEADERS,
    reject_directory_listing,
)
from backend.text_utils import (
    get_base_url,
    infer_top_category,
    make_slug,
    normalize_goods_id,
    normalize_search_text as _normalize_search_text,
    parse_bool,
    parse_json_list,
    parse_positive_int,
    safe_url,
    split_sizes,
    sql_unaccent as _sql_unaccent,
    strip_accents as _strip_accents,
    to_money,
)
from dataset_service import load_dotenv
import network_proxy
from s3_job_operations import (
    JOB_DEFINITIONS,
    STATE_CLEANUP_JOB_FAMILY,
    UPLOAD_JOB_FAMILY,
    URL_MIGRATION_JOB_FAMILY,
    build_job_id,
    normalize_job_metadata,
)
from s3_jobs import ACTIVE_JOB_STATUSES, S3JobManager
from scripts.cleanup_stale_s3_objects import apply_cleanup as s3_cleanup_apply, collect_stale_rows as s3_cleanup_collect, write_backup as s3_cleanup_write_backup
from scripts.migrate_aws_public_urls import apply_changes as migrate_apply_changes, collect_changes as migrate_collect_changes, write_backup as migrate_write_backup

ROOT = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv('FAST_FASHION_DB_PATH', str(ROOT / 'catalog.db')))
S3_STATE_PATH = ROOT / 's3_state.json'
S3_JOBS_STATE_PATH = ROOT / 's3_jobs_state.json'
ALLOWED_DATASETS = {'shein', 'asos'}
DEFAULT_PAGE_SIZE = 24
MAX_PAGE_SIZE = 200
S3_STATE: dict = {'config': {}, 'objects': {}}
S3_STATE_MTIME = 0.0

load_dotenv()

ALLOWED_SORTS = {
    'relevance': 'COALESCE(s.ok, 0) DESC, p.image_count DESC, p.id ASC',
    'price-asc': 'p.price ASC NULLS LAST, p.id ASC',
    'price-desc': 'p.price DESC NULLS LAST, p.id ASC',
    'rating-desc': 'p.rating DESC NULLS LAST, p.id ASC',
    'reviews-desc': 'p.reviews_count DESC NULLS LAST, p.id ASC',
    'name-asc': 'p.name COLLATE NOCASE ASC, p.id ASC',
}

def _env_nonempty(*names: str) -> str:
    for name in names:
        value = os.getenv(name, '').strip()
        if value:
            return value
    return ''


def resolve_aws_public_url() -> str:
    return _env_nonempty('AWS_URL')


def resolve_aws_endpoint_url() -> str:
    return _env_nonempty('AWS_ENDPOINT_URL')


def resolve_aws_bucket() -> str:
    return _env_nonempty('AWS_BUCKET')


def resolve_aws_prefix() -> str:
    return _env_nonempty('AWS_PREFIX')


def resolve_s3_region(endpoint_url: str | None = None, explicit_region: str | None = None) -> str | None:
    region = (explicit_region or '').strip()
    if region:
        return region
    endpoint = (endpoint_url or '').strip().lower()
    if 'r2.cloudflarestorage.com' in endpoint:
        return 'auto'
    env_region = _env_nonempty('AWS_REGION', 'AWS_DEFAULT_REGION')
    if env_region:
        return env_region
    return 'us-east-1' if endpoint else None


def effective_s3_config() -> dict:
    endpoint_url = resolve_aws_endpoint_url()
    proxy_state = network_proxy.public_proxy_state()
    proxy_enabled = bool(proxy_state.get('proxy_enabled'))
    return {
        'region_name': resolve_s3_region(endpoint_url, None),
        'bucket': resolve_aws_bucket() or None,
        'prefix': resolve_aws_prefix() or '',
        'endpoint_url': endpoint_url or None,
        'public_url': resolve_aws_public_url() or None,
        'config_source': 'env',
        'config_source_map': {
            'region_name': 'env',
            'bucket': 'env',
            'prefix': 'env',
            'endpoint_url': 'env',
            'public_url': 'env',
        },
        'proxy_enabled': proxy_enabled,
        'egress_proxy_mode': proxy_state.get('egress_proxy_mode', 'direct'),
        'asos_max_concurrency': int(proxy_state.get('asos_max_concurrency') or 2),
        'asos_timeout_plan_seconds': [10, 20, 30],
        'asos_retry_backoff_seconds': [1, 3],
    }


def s3_to_public_url(s3_url: str | None, endpoint_url: str | None = None, region: str | None = None) -> str | None:
    """Convert an s3://bucket/key URL to a public HTTPS URL.

    Heuristics used:
    - If endpoint_url is provided, prefer endpoint-based construction. For typical S3-compatible endpoints
      that expose objects at {endpoint}/{bucket}/{key}, return endpoint + / + bucket + / + key.
    - If endpoint_url looks like an AWS S3 host (contains "amazonaws"), construct virtual-hosted style
      URL: https://{bucket}.s3.{region}.amazonaws.com/{key} (region-aware, falls back to us-east-1 pattern).
    - Otherwise fall back to https://{bucket}.s3.amazonaws.com/{key}.

    This covers AWS S3, Cloudflare R2, MinIO, and other S3-compatible endpoints in most setups. If you use
    a custom public CDN or domain, prefer storing that URL directly when saving s3 state.
    """
    if not s3_url or not isinstance(s3_url, str):
        return None
    s3_url = s3_url.strip()
    if not s3_url.startswith('s3://'):
        return s3_url
    try:
        _, rest = s3_url.split('s3://', 1)
        bucket, key = rest.split('/', 1)
    except Exception:
        return s3_url
    public_base = resolve_aws_public_url()
    if public_base:
        public_base = public_base.rstrip('/')
        return f'{public_base}/{key}'
    ep = (endpoint_url or '').strip()
    if ep:
        ep = ep.rstrip('/')
        # If the endpoint already includes the bucket in host (rare), try to detect and avoid duplicate
        # If endpoint contains amazonaws, prefer virtual-hosted style below
        if 'amazonaws' in ep:
            reg = region or resolve_s3_region(ep, None) or 'us-east-1'
            if reg == 'us-east-1':
                return f'https://{bucket}.s3.amazonaws.com/{key}'
            return f'https://{bucket}.s3.{reg}.amazonaws.com/{key}'
        # Generic endpoint: assume path-style public access: endpoint/{bucket}/{key}
        return f'{ep}/{bucket}/{key}'
    # No endpoint provided: default to AWS public URL
    reg = region or resolve_s3_region(None, None) or 'us-east-1'
    if reg == 'us-east-1':
        return f'https://{bucket}.s3.amazonaws.com/{key}'
    return f'https://{bucket}.s3.{reg}.amazonaws.com/{key}'

OPENAPI_SPEC = {
    'openapi': '3.1.0',
    'info': {
        'title': 'Fast Fashion Dashboard API',
        'version': '1.3.0',
        'description': 'Read-only API for categories/products plus protected S3 job families with dry-run previews.',
    },
    'servers': [{'url': '/'}],
    'tags': [
        {'name': 'health', 'description': 'Public deployment and readiness endpoints.'},
        {'name': 'meta', 'description': 'Public API description endpoints.'},
        {'name': 'datasets', 'description': 'Dataset metadata endpoints.'},
        {'name': 'categories', 'description': 'Category listing and detail endpoints.'},
        {'name': 'products', 'description': 'Product listing and detail endpoints.'},
        {'name': 's3', 'description': 'Protected S3 sync and background job endpoints.'},
    ],
    'components': {
        'securitySchemes': {
            'bearerAuth': {
                'type': 'http',
                'scheme': 'bearer',
                'bearerFormat': 'API token',
                'description': 'Send the deployment token in the Authorization header: Bearer <token>.',
            }
        },
        'parameters': {
            'dataset': {
                'name': 'dataset',
                'in': 'query',
                'description': 'Dataset identifier.',
                'schema': {'type': 'string', 'enum': ['shein', 'asos'], 'default': 'shein'},
            },
            'page': {
                'name': 'page',
                'in': 'query',
                'description': '1-based page number.',
                'schema': {'type': 'integer', 'minimum': 1, 'default': 1},
            },
            'pageSize': {
                'name': 'pageSize',
                'in': 'query',
                'description': 'Page size.',
                'schema': {'type': 'integer', 'minimum': 1, 'maximum': 200},
            },
            'categorySlug': {
                'name': 'slug',
                'in': 'path',
                'required': True,
                'description': 'Stable slug of the category.',
                'schema': {'type': 'string'},
            },
            'goodsId': {
                'name': 'goods_id',
                'in': 'path',
                'required': True,
                'description': 'Stable goods id, optionally prefixed with the dataset (example: shein:123).',
                'schema': {'type': 'string'},
            },
            'jobId': {
                'name': 'job_id',
                'in': 'path',
                'required': True,
                'description': 'Background S3 job identifier.',
                'schema': {'type': 'string'},
            },
            'search': {
                'name': 'search',
                'in': 'query',
                'description': 'Full-text search term applied to the indexed catalog text.',
                'schema': {'type': 'string'},
            },
            'categoryFilter': {
                'name': 'category',
                'in': 'query',
                'description': 'Category name or fragment used to filter products.',
                'schema': {'type': 'string'},
            },
            'sort': {
                'name': 'sort',
                'in': 'query',
                'description': 'Sort mode.',
                'schema': {
                    'type': 'string',
                    'enum': ['relevance', 'price-asc', 'price-desc', 'rating-desc', 'reviews-desc', 'name-asc'],
                    'default': 'relevance',
                },
            },
            'imagesOnly': {
                'name': 'imagesOnly',
                'in': 'query',
                'description': 'When true, returns only products that have at least one source image in the imported catalog.',
                'schema': {'type': 'boolean', 'default': False},
            },
            'savedOnS3': {
                'name': 'savedOnS3',
                'in': 'query',
                'description': 'When true, product endpoints return only rows whose runtime S3 state is marked as saved in SQLite; category endpoints return only categories with at least one saved product and prefer a representative S3 image URL when available.',
                'schema': {'type': 'boolean', 'default': False},
            },
            'format': {
                'name': 'format',
                'in': 'query',
                'description': 'Response shape. legacy matches the dashboard payload, resource exposes the stable resource schema.',
                'schema': {'type': 'string', 'enum': ['legacy', 'resource'], 'default': 'legacy'},
            },
            'page_size_jobs': {
                'name': 'page_size',
                'in': 'query',
                'description': 'Number of S3 job items returned per page.',
                'schema': {'type': 'integer', 'minimum': 1, 'maximum': 50, 'default': 12},
            },
            'pageSizeS3JobsHistory': {
                'name': 'pageSize',
                'in': 'query',
                'description': 'Number of S3 jobs returned per history page.',
                'schema': {'type': 'integer', 'minimum': 1, 'maximum': 100, 'default': 20},
            },
        },
        'schemas': {
            'OpenApiDocument': {'type': 'object', 'additionalProperties': True},
            'ErrorObject': {
                'type': 'object',
                'required': ['code', 'message'],
                'properties': {
                    'code': {'type': 'string'},
                    'message': {'type': 'string'},
                },
            },
            'ErrorResponse': {
                'type': 'object',
                'required': ['error'],
                'properties': {
                    'error': {'$ref': '#/components/schemas/ErrorObject'},
                },
            },
            'HealthStatus': {
                'type': 'object',
                'required': ['ok', 'db_path', 'db_exists', 'datasets_count'],
                'properties': {
                    'ok': {'type': 'boolean'},
                    'db_path': {'type': 'string'},
                    'db_exists': {'type': 'boolean'},
                    'has_datasets_table': {'type': 'boolean'},
                    'datasets_count': {'type': 'integer'},
                    'error': {'type': 'string'},
                },
            },
            'HealthResponse': {
                'type': 'object',
                'required': ['data'],
                'properties': {
                    'data': {'$ref': '#/components/schemas/HealthStatus'},
                },
            },
            'DatasetMeta': {
                'type': 'object',
                'required': ['id', 'label', 'source', 'total_count', 'with_images_count', 'with_reviews_count', 'local_path', 'provider'],
                'properties': {
                    'id': {'type': 'string', 'enum': ['shein', 'asos']},
                    'label': {'type': 'string'},
                    'source': {'type': 'string'},
                    'total_count': {'type': 'integer'},
                    'with_images_count': {'type': 'integer'},
                    'with_reviews_count': {'type': 'integer'},
                    'local_path': {'type': 'string'},
                    'provider': {'type': 'string'},
                },
            },
            'DatasetsResponse': {
                'type': 'object',
                'required': ['datasets'],
                'properties': {
                    'datasets': {
                        'type': 'array',
                        'items': {'$ref': '#/components/schemas/DatasetMeta'},
                    },
                },
            },
            'Pagination': {
                'type': 'object',
                'required': ['page', 'pageSize', 'total', 'totalPages', 'from', 'to'],
                'properties': {
                    'page': {'type': 'integer'},
                    'pageSize': {'type': 'integer'},
                    'total': {'type': 'integer'},
                    'totalPages': {'type': 'integer'},
                    'from': {'type': 'integer'},
                    'to': {'type': 'integer'},
                },
            },
            'CategoryResource': {
                'type': 'object',
                'required': ['name', 'slug', 'top_category_name', 'source_url', 'image_url', 'source_image_url', 's3_image_url', 'saved_on_s3', 'saved_products_count', 's3_image_count'],
                'properties': {
                    'name': {'type': 'string'},
                    'slug': {'type': 'string'},
                    'top_category_name': {'type': ['string', 'null']},
                    'source_url': {'type': ['string', 'null']},
                    'image_url': {'type': ['string', 'null']},
                    'source_image_url': {'type': ['string', 'null']},
                    's3_image_url': {'type': ['string', 'null']},
                    'saved_on_s3': {'type': 'boolean'},
                    'saved_products_count': {'type': 'integer'},
                    's3_image_count': {'type': 'integer'},
                    'count': {'type': 'integer'},
                },
            },
            'CategoriesListResponse': {
                'type': 'object',
                'required': ['dataset', 'data', 'pagination'],
                'properties': {
                    'dataset': {'$ref': '#/components/schemas/DatasetMeta'},
                    'data': {
                        'type': 'array',
                        'items': {'$ref': '#/components/schemas/CategoryResource'},
                    },
                    'pagination': {'$ref': '#/components/schemas/Pagination'},
                },
            },
            'CategoryDetailResponse': {
                'type': 'object',
                'required': ['dataset', 'data'],
                'properties': {
                    'dataset': {'$ref': '#/components/schemas/DatasetMeta'},
                    'data': {'$ref': '#/components/schemas/CategoryResource'},
                },
            },
            'Attribute': {
                'type': 'object',
                'required': ['name', 'value'],
                'properties': {
                    'name': {'type': 'string'},
                    'value': {'type': ['string', 'number', 'boolean', 'null']},
                },
            },
            'CategoryTreeItem': {
                'type': 'object',
                'required': ['name', 'url'],
                'properties': {
                    'name': {'type': 'string'},
                    'url': {'type': ['string', 'null']},
                },
            },
            'CategoryDetails': {
                'type': 'object',
                'properties': {
                    'category_id': {'type': ['string', 'null']},
                    'goods_id': {'type': 'string'},
                    'level': {'type': ['integer', 'null']},
                    'name': {'type': ['string', 'null']},
                    'url': {'type': ['string', 'null']},
                },
            },
            'StoreDetails': {
                'type': 'object',
                'properties': {
                    'code': {'type': ['string', 'null']},
                    'followers': {'type': ['integer', 'null']},
                    'items': {'type': ['integer', 'null']},
                    'name': {'type': ['string', 'null']},
                },
            },
            'ImagePair': {
                'type': 'object',
                'properties': {
                    'source_url': {'type': 'string'},
                    's3_url': {'type': 'string'},
                    'key': {'type': ['string', 'null']},
                    'status': {'type': ['string', 'null']},
                },
            },
            'ProductResource': {
                'type': 'object',
                'required': ['goods_id', 'goods_sn', 'spu', 'name', 'description', 'retail_price', 'sale_price', 'currency', 'images', 'attributes', 'store_name', 'rating', 'reviews_count', 'saved_on_s3', 's3_image_count'],
                'properties': {
                    'goods_id': {'type': 'string'},
                    'goods_sn': {'type': 'string'},
                    'spu': {'type': 'string'},
                    'category_id': {'type': ['string', 'null']},
                    'name': {'type': 'string'},
                    'brand': {'type': ['string', 'null']},
                    'color': {'type': ['string', 'null']},
                    'size': {'type': ['string', 'null']},
                    'description': {'type': 'string'},
                    'retail_price': {'type': 'string'},
                    'sale_price': {'type': 'string'},
                    'currency': {'type': 'string'},
                    'in_stock': {'type': 'boolean'},
                    'stock_quantity': {'type': 'integer'},
                    'images': {'type': 'array', 'items': {'type': 'string'}},
                    'category_url': {'type': ['string', 'null']},
                    'product_url': {'type': ['string', 'null']},
                    'category_tree': {
                        'oneOf': [
                            {'type': 'null'},
                            {'type': 'array', 'items': {'$ref': '#/components/schemas/CategoryTreeItem'}},
                        ]
                    },
                    'country_code': {'type': ['string', 'null']},
                    'domain': {'type': ['string', 'null']},
                    'image_count': {'type': 'integer'},
                    'offers': {'type': ['string', 'null']},
                    'attributes': {'type': 'array', 'items': {'$ref': '#/components/schemas/Attribute'}},
                    'root_category': {'type': ['string', 'null']},
                    'related_products': {'type': ['array', 'null']},
                    'top_reviews': {'type': ['array', 'null']},
                    'store_name': {'type': ['string', 'null']},
                    'rating': {'type': 'string'},
                    'reviews_count': {'type': 'integer'},
                    'is_free_shipping': {'type': 'boolean'},
                    'available_sizes': {
                        'oneOf': [
                            {'type': 'null'},
                            {'type': 'array', 'items': {'type': 'string'}},
                        ]
                    },
                    'category_details': {'$ref': '#/components/schemas/CategoryDetails'},
                    'discount_price': {'type': 'string'},
                    'discount_price_usd': {'type': 'string'},
                    'colors': {
                        'oneOf': [
                            {'type': 'null'},
                            {'type': 'array', 'items': {'type': ['string', 'null']}},
                        ]
                    },
                    'store_details': {'$ref': '#/components/schemas/StoreDetails'},
                    'shipping_details': {'type': ['object', 'null'], 'additionalProperties': True},
                    'shipping_type': {'type': ['string', 'null']},
                    'tags': {'type': 'array', 'items': {'type': 'string'}},
                    'model_data': {'type': ['object', 'null'], 'additionalProperties': True},
                    'source_image_urls': {'type': 'array', 'items': {'type': 'string'}},
                    's3_image_urls': {'type': 'array', 'items': {'type': 'string'}},
                    'image_pairs': {'type': 'array', 'items': {'$ref': '#/components/schemas/ImagePair'}},
                    'saved_on_s3': {'type': 'boolean'},
                    's3_url': {'type': ['string', 'null']},
                    's3_image_count': {'type': 'integer'},
                },
            },
            'LegacyProduct': {
                'type': 'object',
                'description': 'Dashboard-oriented legacy payload. Shape can include the original imported row plus derived display fields.',
                'properties': {
                    'goods_id': {'type': 'string'},
                    'saved_on_s3': {'type': 'boolean'},
                    's3_url': {'type': ['string', 'null']},
                    's3_image_count': {'type': 'integer'},
                    'sourceImageUrls': {'type': 'array', 'items': {'type': 'string'}},
                    's3ImageUrls': {'type': 'array', 'items': {'type': 'string'}},
                    'imagePairs': {'type': 'array', 'items': {'$ref': '#/components/schemas/ImagePair'}},
                },
                'additionalProperties': True,
            },
            'ProductsLegacyResponse': {
                'type': 'object',
                'required': ['dataset', 'products', 'pagination'],
                'properties': {
                    'dataset': {'$ref': '#/components/schemas/DatasetMeta'},
                    'products': {'type': 'array', 'items': {'$ref': '#/components/schemas/LegacyProduct'}},
                    'pagination': {'$ref': '#/components/schemas/Pagination'},
                },
            },
            'ProductsResourceResponse': {
                'type': 'object',
                'required': ['dataset', 'data', 'pagination'],
                'properties': {
                    'dataset': {'$ref': '#/components/schemas/DatasetMeta'},
                    'data': {'type': 'array', 'items': {'$ref': '#/components/schemas/ProductResource'}},
                    'pagination': {'$ref': '#/components/schemas/Pagination'},
                },
            },
            'ProductDetailResponse': {
                'type': 'object',
                'required': ['dataset', 'display', 'api', 'data'],
                'properties': {
                    'dataset': {'$ref': '#/components/schemas/DatasetMeta'},
                    'display': {'$ref': '#/components/schemas/LegacyProduct'},
                    'api': {'$ref': '#/components/schemas/ProductResource'},
                    'data': {'$ref': '#/components/schemas/ProductResource'},
                },
            },
            'S3Config': {
                'type': 'object',
                'properties': {
                    'region_name': {'type': ['string', 'null'], 'readOnly': True},
                    'bucket': {'type': ['string', 'null'], 'readOnly': True},
                    'prefix': {'type': ['string', 'null'], 'readOnly': True},
                    'endpoint_url': {'type': ['string', 'null'], 'readOnly': True},
                    'public_url': {'type': ['string', 'null'], 'readOnly': True},
                    'config_source': {'type': 'string', 'enum': ['env'], 'readOnly': True},
                    'config_source_map': {
                        'type': 'object',
                        'readOnly': True,
                        'additionalProperties': {'type': 'string'},
                    },
                    'proxy_enabled': {'type': 'boolean', 'readOnly': True},
                    'egress_proxy_mode': {'type': 'string', 'enum': ['direct', 'proxy'], 'readOnly': True},
                    'asos_max_concurrency': {'type': 'integer', 'readOnly': True},
                    'asos_timeout_plan_seconds': {'type': 'array', 'items': {'type': 'integer'}, 'readOnly': True},
                    'asos_retry_backoff_seconds': {'type': 'array', 'items': {'type': 'integer'}, 'readOnly': True},
                },
            },
            'S3ConfigResponse': {
                'type': 'object',
                'required': ['data'],
                'properties': {
                    'data': {'$ref': '#/components/schemas/S3Config'},
                },
            },
            'S3JobItem': {
                'type': 'object',
                'additionalProperties': True,
                'properties': {
                    'status': {'type': ['string', 'null']},
                    'message': {'type': ['string', 'null']},
                    'goods_id': {'type': ['string', 'null']},
                    'product_id': {'type': ['string', 'null']},
                    'source_url': {'type': ['string', 'null']},
                    's3_url': {'type': ['string', 'null']},
                    'download_stats': {'type': ['object', 'null'], 'additionalProperties': True},
                },
            },
            'S3JobState': {
                'type': 'object',
                'additionalProperties': True,
                'properties': {
                    'job_id': {'type': 'string'},
                    'dataset_id': {'type': 'string'},
                    'source': {'type': 'string'},
                    'limit': {'type': 'integer'},
                    'status': {'type': 'string'},
                    'processed': {'type': 'integer'},
                    'uploaded': {'type': 'integer'},
                    'skipped': {'type': 'integer'},
                    'failed': {'type': 'integer'},
                    'total': {'type': 'integer'},
                    'started_at': {'type': ['number', 'null']},
                    'ended_at': {'type': ['number', 'null']},
                    'error': {'type': ['string', 'null']},
                    'cancel_requested': {'type': 'boolean'},
                    'bucket': {'type': ['string', 'null']},
                    'prefix': {'type': ['string', 'null']},
                    'concurrency': {'type': 'integer'},
                    'source_filter': {'type': ['string', 'null']},
                    'selection_mode': {'type': 'string', 'enum': ['pending', 'pending_only', 'all', 'partial']},
                    'excluded_complete_count': {'type': 'integer', 'minimum': 0},
                    'last_message': {'type': ['string', 'null']},
                    'job_family': {'type': 'string', 'enum': ['upload', 'url_migration', 'state_cleanup']},
                    'dry_run': {'type': 'boolean'},
                    'kind': {'type': 'string'},
                    'download_stats': {'type': ['object', 'null'], 'additionalProperties': True},
                    'items': {'type': ['array', 'null'], 'items': {'$ref': '#/components/schemas/S3JobItem'}},
                },
            },
            'S3JobsListResponse': {
                'type': 'object',
                'required': ['data', 'config', 'pagination'],
                'properties': {
                    'data': {'type': 'array', 'items': {'$ref': '#/components/schemas/S3JobState'}},
                    'config': {'$ref': '#/components/schemas/S3Config'},
                    'pagination': {'$ref': '#/components/schemas/Pagination'},
                },
            },
            'S3JobDetailPayload': {
                'type': 'object',
                'required': ['job', 'items', 'page', 'page_size', 'total_items', 'total_pages'],
                'properties': {
                    'job': {'$ref': '#/components/schemas/S3JobState'},
                    'items': {'type': 'array', 'items': {'$ref': '#/components/schemas/S3JobItem'}},
                    'page': {'type': 'integer'},
                    'page_size': {'type': 'integer'},
                    'total_items': {'type': 'integer'},
                    'total_pages': {'type': 'integer'},
                },
            },
            'S3JobDetailResponse': {
                'type': 'object',
                'required': ['data'],
                'properties': {
                    'data': {'$ref': '#/components/schemas/S3JobDetailPayload'},
                },
            },
            'S3AuthStatusResponse': {
                'type': 'object',
                'required': ['data'],
                'properties': {
                    'data': {
                        'type': 'object',
                        'required': ['authenticated'],
                        'properties': {
                            'authenticated': {'type': 'boolean'},
                            'expires_in_seconds': {'type': 'integer'},
                        },
                    },
                },
            },
            'S3AuthRequest': {
                'type': 'object',
                'properties': {
                    'password': {'type': 'string'},
                },
            },
            'S3UploadJobCreateRequest': {
                'type': 'object',
                'properties': {
                    'dataset_id': {'type': 'string', 'enum': ['shein', 'asos'], 'default': 'shein'},
                    'source': {'type': 'string', 'default': 'products'},
                    'limit': {'type': 'integer', 'minimum': 1, 'default': 100},
                    'concurrency': {'type': 'integer', 'minimum': 1, 'maximum': 24, 'default': 4},
                    'source_filter': {'type': 'string'},
                    'selection_mode': {'type': 'string', 'enum': ['pending', 'pending_only', 'all', 'partial'], 'default': 'pending'},
                    'dry_run': {'type': 'boolean', 'default': False},
                },
            },
            'S3UrlMigrationJobCreateRequest': {
                'type': 'object',
                'properties': {
                    'dry_run': {'type': 'boolean', 'default': False},
                    'sample_limit': {'type': 'integer', 'minimum': 1, 'maximum': 200, 'default': 25},
                },
            },
            'S3UrlMigrationSummary': {
                'type': 'object',
                'required': ['total', 'sample_limit', 'sample', 'public_url'],
                'properties': {
                    'total': {'type': 'integer'},
                    'sample_limit': {'type': 'integer'},
                    'public_url': {'type': 'string'},
                    'sample': {
                        'type': 'array',
                        'items': {'type': 'object', 'additionalProperties': True},
                    },
                },
            },
            'S3StateCleanupJobCreateRequest': {
                'type': 'object',
                'properties': {
                    'dry_run': {'type': 'boolean', 'default': False},
                    'sample_limit': {'type': 'integer', 'minimum': 1, 'maximum': 200, 'default': 25},
                },
            },
            'S3StateCleanupSummary': {
                'type': 'object',
                'required': ['total', 'sample_limit', 'sample', 'current_bucket'],
                'properties': {
                    'total': {'type': 'integer'},
                    'sample_limit': {'type': 'integer'},
                    'current_bucket': {'type': ['string', 'null']},
                    'sample': {
                        'type': 'array',
                        'items': {'type': 'object', 'additionalProperties': True},
                    },
                },
            },
            'S3JobAcceptedResponse': {
                'type': 'object',
                'required': ['data', 'future'],
                'properties': {
                    'data': {'$ref': '#/components/schemas/S3JobState'},
                    'future': {'type': 'boolean'},
                },
            },
        },
        'responses': {
            'Unauthorized': {
                'description': 'Missing or invalid bearer token.',
                'content': {
                    'application/json': {
                        'schema': {'$ref': '#/components/schemas/ErrorResponse'},
                    }
                },
            },
            'NotFound': {
                'description': 'Requested resource was not found.',
                'content': {
                    'application/json': {
                        'schema': {'$ref': '#/components/schemas/ErrorResponse'},
                    }
                },
            },
            'BadRequest': {
                'description': 'Request validation failed.',
                'content': {
                    'application/json': {
                        'schema': {'$ref': '#/components/schemas/ErrorResponse'},
                    }
                },
            },
            'InternalError': {
                'description': 'Internal server error.',
                'content': {
                    'application/json': {
                        'schema': {'$ref': '#/components/schemas/ErrorResponse'},
                    }
                },
            },
        },
    },
    'paths': {
        '/healthz': {
            'get': {
                'tags': ['health'],
                'operationId': 'getHealthz',
                'summary': 'Deployment health endpoint',
                'description': 'Public readiness endpoint for Dokploy and reverse-proxy health checks.',
                'responses': {
                    '200': {
                        'description': 'Service is ready.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/HealthResponse'}}},
                    },
                    '503': {
                        'description': 'Service is not ready yet.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/HealthResponse'}}},
                    },
                },
            }
        },
        '/openapi.json': {
            'get': {
                'tags': ['meta'],
                'operationId': 'getOpenApiDocument',
                'summary': 'OpenAPI document',
                'description': 'Public OpenAPI document for client integration and tooling.',
                'responses': {
                    '200': {
                        'description': 'OpenAPI document.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/OpenApiDocument'}}},
                    }
                },
            }
        },
        '/api/openapi.json': {
            'get': {
                'tags': ['meta'],
                'operationId': 'getOpenApiDocumentAlias',
                'summary': 'OpenAPI document alias',
                'description': 'Backward-compatible alias of /openapi.json.',
                'responses': {
                    '200': {
                        'description': 'OpenAPI document.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/OpenApiDocument'}}},
                    }
                },
            }
        },
        '/api/datasets': {
            'get': {
                'tags': ['datasets'],
                'operationId': 'listDatasets',
                'summary': 'List datasets',
                'description': 'Returns metadata for all retained datasets, or a single dataset when the dataset query parameter is provided.',
                'security': [{'bearerAuth': []}],
                'parameters': [
                    {
                        'name': 'dataset',
                        'in': 'query',
                        'description': 'Optional dataset filter.',
                        'schema': {'type': 'string', 'enum': ['shein', 'asos']},
                    }
                ],
                'responses': {
                    '200': {
                        'description': 'Dataset metadata list.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/DatasetsResponse'}}},
                    },
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                    '500': {'$ref': '#/components/responses/InternalError'},
                },
            }
        },
        '/api/categories': {
            'get': {
                'tags': ['categories'],
                'operationId': 'listCategories',
                'summary': 'List categories',
                'description': 'Returns stable category resources for a dataset. When savedOnS3=true, only categories with at least one S3-saved product are returned and image_url prefers a representative S3 image.',
                'security': [{'bearerAuth': []}],
                'parameters': [
                    {'$ref': '#/components/parameters/dataset'},
                    {'$ref': '#/components/parameters/search'},
                    {'$ref': '#/components/parameters/savedOnS3'},
                    {'$ref': '#/components/parameters/page'},
                    {'$ref': '#/components/parameters/pageSize'},
                ],
                'responses': {
                    '200': {
                        'description': 'Category list.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/CategoriesListResponse'}}},
                    },
                    '400': {'$ref': '#/components/responses/BadRequest'},
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                    '500': {'$ref': '#/components/responses/InternalError'},
                },
            }
        },
        '/api/categories/{slug}': {
            'get': {
                'tags': ['categories'],
                'operationId': 'getCategory',
                'summary': 'Get category',
                'description': 'Returns a single category resource by slug. When savedOnS3=true, the lookup is restricted to categories with at least one S3-saved product and image_url prefers a representative S3 image.',
                'security': [{'bearerAuth': []}],
                'parameters': [
                    {'$ref': '#/components/parameters/categorySlug'},
                    {'$ref': '#/components/parameters/dataset'},
                    {'$ref': '#/components/parameters/savedOnS3'},
                ],
                'responses': {
                    '200': {
                        'description': 'Category detail.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/CategoryDetailResponse'}}},
                    },
                    '400': {'$ref': '#/components/responses/BadRequest'},
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                    '404': {'$ref': '#/components/responses/NotFound'},
                    '500': {'$ref': '#/components/responses/InternalError'},
                },
            }
        },
        '/api/products': {
            'get': {
                'tags': ['products'],
                'operationId': 'listProducts',
                'summary': 'List products',
                'description': 'Returns either the legacy dashboard payload or the stable resource payload, depending on the format query parameter.',
                'security': [{'bearerAuth': []}],
                'parameters': [
                    {'$ref': '#/components/parameters/dataset'},
                    {'$ref': '#/components/parameters/search'},
                    {'$ref': '#/components/parameters/categoryFilter'},
                    {'$ref': '#/components/parameters/sort'},
                    {'$ref': '#/components/parameters/imagesOnly'},
                    {'$ref': '#/components/parameters/savedOnS3'},
                    {'$ref': '#/components/parameters/page'},
                    {'$ref': '#/components/parameters/pageSize'},
                    {'$ref': '#/components/parameters/format'},
                ],
                'responses': {
                    '200': {
                        'description': 'Product list.',
                        'content': {
                            'application/json': {
                                'schema': {
                                    'oneOf': [
                                        {'$ref': '#/components/schemas/ProductsLegacyResponse'},
                                        {'$ref': '#/components/schemas/ProductsResourceResponse'},
                                    ]
                                }
                            }
                        },
                    },
                    '400': {'$ref': '#/components/responses/BadRequest'},
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                    '500': {'$ref': '#/components/responses/InternalError'},
                },
            }
        },
        '/api/products/{goods_id}': {
            'get': {
                'tags': ['products'],
                'operationId': 'getProduct',
                'summary': 'Get product',
                'description': 'Returns a single product in both the legacy dashboard display shape and the stable resource shape.',
                'security': [{'bearerAuth': []}],
                'parameters': [
                    {'$ref': '#/components/parameters/goodsId'},
                    {'$ref': '#/components/parameters/dataset'},
                ],
                'responses': {
                    '200': {
                        'description': 'Product detail.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/ProductDetailResponse'}}},
                    },
                    '400': {'$ref': '#/components/responses/BadRequest'},
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                    '404': {'$ref': '#/components/responses/NotFound'},
                    '500': {'$ref': '#/components/responses/InternalError'},
                },
            }
        },
        '/api/s3/auth-check': {
            'get': {
                'tags': ['s3'],
                'operationId': 'getS3AuthCheck',
                'summary': 'Check S3 auth session',
                'description': 'Returns whether the current browser session already holds a valid S3 auth cookie.',
                'security': [{'bearerAuth': []}],
                'responses': {
                    '200': {
                        'description': 'S3 auth status.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/S3AuthStatusResponse'}}},
                    },
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                },
            }
        },
        '/api/s3/auth': {
            'post': {
                'tags': ['s3'],
                'operationId': 'postS3Auth',
                'summary': 'Authenticate S3 control session',
                'description': 'Validates the S3 password and returns an HttpOnly cookie used by the S3 control page.',
                'security': [{'bearerAuth': []}],
                'requestBody': {
                    'required': False,
                    'content': {
                        'application/json': {
                            'schema': {'$ref': '#/components/schemas/S3AuthRequest'},
                        }
                    },
                },
                'responses': {
                    '200': {
                        'description': 'Authenticated.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/S3AuthStatusResponse'}}},
                    },
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                },
            }
        },
        '/api/s3/config': {
            'get': {
                'tags': ['s3'],
                'operationId': 'getS3Config',
                'summary': 'Get effective S3 config',
                'description': 'Returns the non-secret effective S3 configuration resolved from environment variables.',
                'security': [{'bearerAuth': []}],
                'responses': {
                    '200': {
                        'description': 'S3 config.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/S3ConfigResponse'}}},
                    },
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                },
            },
        },
        '/api/s3/upload-jobs': {
            'get': {
                'tags': ['s3'],
                'operationId': 'listS3UploadJobs',
                'summary': 'List upload jobs',
                'description': 'Returns only paginated S3 upload job history plus the effective non-secret config. Default history page size is 20.',
                'security': [{'bearerAuth': []}],
                'parameters': [
                    {'$ref': '#/components/parameters/page'},
                    {'$ref': '#/components/parameters/pageSizeS3JobsHistory'},
                ],
                'responses': {
                    '200': {
                        'description': 'S3 upload jobs list.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/S3JobsListResponse'}}},
                    },
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                },
            },
            'post': {
                'tags': ['s3'],
                'operationId': 'createS3UploadJob',
                'summary': 'Create upload job',
                'description': 'Starts a new upload job, or a dry-run preview when dry_run=true. By default only pending or partially synced products are selected; selection_mode can widen or narrow that candidate set.',
                'security': [{'bearerAuth': []}],
                'requestBody': {
                    'required': True,
                    'content': {
                        'application/json': {
                            'schema': {'$ref': '#/components/schemas/S3UploadJobCreateRequest'},
                        }
                    },
                },
                'responses': {
                    '202': {
                        'description': 'Upload job created.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/S3JobAcceptedResponse'}}},
                    },
                    '400': {'$ref': '#/components/responses/BadRequest'},
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                    '409': {'$ref': '#/components/responses/BadRequest'},
                },
            },
        },
        '/api/s3/jobs/{job_id}': {
            'get': {
                'tags': ['s3'],
                'operationId': 'getS3Job',
                'summary': 'Get job detail',
                'description': 'Returns one job plus a paginated slice of its item history.',
                'security': [{'bearerAuth': []}],
                'parameters': [
                    {'$ref': '#/components/parameters/jobId'},
                    {'$ref': '#/components/parameters/page'},
                    {'$ref': '#/components/parameters/page_size_jobs'},
                ],
                'responses': {
                    '200': {
                        'description': 'S3 job detail.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/S3JobDetailResponse'}}},
                    },
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                    '404': {'$ref': '#/components/responses/NotFound'},
                },
            }
        },
        '/api/s3/jobs/{job_id}/cancel': {
            'post': {
                'tags': ['s3'],
                'operationId': 'cancelS3Job',
                'summary': 'Cancel job',
                'description': 'Requests cancellation of an active S3 background job.',
                'security': [{'bearerAuth': []}],
                'parameters': [
                    {'$ref': '#/components/parameters/jobId'},
                ],
                'responses': {
                    '202': {
                        'description': 'Cancellation accepted.',
                        'content': {'application/json': {'schema': {'type': 'object', 'properties': {'data': {'$ref': '#/components/schemas/S3JobState'}}}}},
                    },
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                    '404': {'$ref': '#/components/responses/NotFound'},
                },
            }
        },
        '/api/s3/migration-summary': {
            'get': {
                'tags': ['s3'],
                'operationId': 'getS3MigrationSummary',
                'summary': 'Preview migration impact [deprecated]',
                'description': 'Deprecated compatibility summary endpoint. Use POST /api/s3/url-migration-jobs with dry_run=true as the canonical preview flow.',
                'deprecated': True,
                'security': [{'bearerAuth': []}],
                'responses': {
                    '200': {
                        'description': 'Migration impact summary.',
                        'content': {'application/json': {'schema': {'type': 'object', 'properties': {'data': {'$ref': '#/components/schemas/S3UrlMigrationSummary'}}}}},
                    },
                    '400': {'$ref': '#/components/responses/BadRequest'},
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                },
            }
        },
        '/api/s3/url-migration-jobs': {
            'get': {
                'tags': ['s3'],
                'operationId': 'listS3UrlMigrationJobs',
                'summary': 'List URL migration jobs',
                'description': 'Returns only paginated URL migration job history plus the effective non-secret config. Default history page size is 20.',
                'security': [{'bearerAuth': []}],
                'parameters': [
                    {'$ref': '#/components/parameters/page'},
                    {'$ref': '#/components/parameters/pageSizeS3JobsHistory'},
                ],
                'responses': {
                    '200': {
                        'description': 'URL migration jobs list.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/S3JobsListResponse'}}},
                    },
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                },
            },
            'post': {
                'tags': ['s3'],
                'operationId': 'createS3UrlMigrationJob',
                'summary': 'Create URL migration job',
                'description': 'Starts a URL migration job, or a dry-run preview when dry_run=true.',
                'security': [{'bearerAuth': []}],
                'requestBody': {
                    'required': False,
                    'content': {
                        'application/json': {
                            'schema': {'$ref': '#/components/schemas/S3UrlMigrationJobCreateRequest'},
                        }
                    },
                },
                'responses': {
                    '202': {
                        'description': 'URL migration job created.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/S3JobAcceptedResponse'}}},
                    },
                    '400': {'$ref': '#/components/responses/BadRequest'},
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                },
            }
        },
        '/api/s3/cleanup-summary': {
            'get': {
                'tags': ['s3'],
                'operationId': 'getS3CleanupSummary',
                'summary': 'Preview stale S3-state cleanup impact [deprecated]',
                'description': 'Deprecated compatibility summary endpoint. Use POST /api/s3/state-cleanup-jobs with dry_run=true as the canonical preview flow.',
                'deprecated': True,
                'security': [{'bearerAuth': []}],
                'responses': {
                    '200': {
                        'description': 'Cleanup impact summary.',
                        'content': {'application/json': {'schema': {'type': 'object', 'properties': {'data': {'$ref': '#/components/schemas/S3StateCleanupSummary'}}}}},
                    },
                    '400': {'$ref': '#/components/responses/BadRequest'},
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                },
            }
        },
        '/api/s3/state-cleanup-jobs': {
            'get': {
                'tags': ['s3'],
                'operationId': 'listS3StateCleanupJobs',
                'summary': 'List stale-state cleanup jobs',
                'description': 'Returns only paginated stale-state cleanup job history plus the effective non-secret config. Default history page size is 20.',
                'security': [{'bearerAuth': []}],
                'parameters': [
                    {'$ref': '#/components/parameters/page'},
                    {'$ref': '#/components/parameters/pageSizeS3JobsHistory'},
                ],
                'responses': {
                    '200': {
                        'description': 'Cleanup jobs list.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/S3JobsListResponse'}}},
                    },
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                },
            },
            'post': {
                'tags': ['s3'],
                'operationId': 'createS3StateCleanupJob',
                'summary': 'Create stale-state cleanup job',
                'description': 'Starts a stale-state cleanup job, or a dry-run preview when dry_run=true.',
                'security': [{'bearerAuth': []}],
                'requestBody': {
                    'required': False,
                    'content': {
                        'application/json': {
                            'schema': {'$ref': '#/components/schemas/S3StateCleanupJobCreateRequest'},
                        }
                    },
                },
                'responses': {
                    '202': {
                        'description': 'Cleanup job created.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/S3JobAcceptedResponse'}}},
                    },
                    '400': {'$ref': '#/components/responses/BadRequest'},
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                },
            }
        },
    },
}


def _sanitize_s3_config(config: dict | None) -> dict:
    cleaned = dict(config or {})
    for secret_key in ('aws_access_key_id', 'aws_secret_access_key', 'aws_session_token'):
        cleaned.pop(secret_key, None)
    for managed_key in ('region_name', 'bucket', 'prefix', 'endpoint_url', 'public_url', 'config_source', 'config_source_map'):
        cleaned.pop(managed_key, None)
    return cleaned


def _normalize_s3_object(goods_id: str, payload: dict | None) -> dict | None:
    if not isinstance(payload, dict):
        return None
    source_urls = [str(url).strip() for url in (payload.get('source_image_urls') or []) if isinstance(url, str) and str(url).strip()]
    s3_urls = [str(url).strip() for url in (payload.get('s3_image_urls') or []) if isinstance(url, str) and str(url).strip()]
    image_pairs = []
    for pair in payload.get('image_pairs') or []:
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
    return {
        'dataset_id': str(payload.get('dataset_id') or '').strip() or goods_id.split(':', 1)[0],
        'product_id': str(payload.get('product_id') or '').strip() or goods_id.split(':', 1)[1],
        'goods_id': goods_id,
        'source_url': str(payload.get('source_url') or '').strip() or (source_urls[0] if source_urls else None),
        's3_url': str(payload.get('s3_url') or '').strip() or (s3_urls[0] if s3_urls else None),
        'bucket': str(payload.get('bucket') or '').strip() or None,
        'key': payload.get('key'),
        'source_image_urls': source_urls,
        's3_image_urls': s3_urls,
        'image_pairs': image_pairs,
        'source_image_count': int(payload.get('source_image_count') or len(source_urls)),
        's3_image_count': int(payload.get('s3_image_count') or len(s3_urls)),
        'failed_image_count': int(payload.get('failed_image_count') or 0),
        'saved_on_s3': bool(payload.get('saved_on_s3')),
        'saved_at': float(payload.get('saved_at') or time.time()),
    }


def _load_legacy_s3_state_payload() -> dict:
    if not S3_STATE_PATH.exists():
        return {'config': {}, 'objects': {}}
    try:
        loaded = json.loads(S3_STATE_PATH.read_text(encoding='utf-8'))
    except Exception:
        return {'config': {}, 'objects': {}}
    return {
        'config': _sanitize_s3_config(loaded.get('config', {})),
        'objects': loaded.get('objects', {}) if isinstance(loaded.get('objects'), dict) else {},
    }


def _load_legacy_s3_jobs_payload() -> list[dict]:
    if not S3_JOBS_STATE_PATH.exists():
        return []
    try:
        payload = json.loads(S3_JOBS_STATE_PATH.read_text(encoding='utf-8'))
    except Exception:
        return []
    jobs = payload.get('jobs') if isinstance(payload, dict) else payload
    return jobs if isinstance(jobs, list) else []


def _publicize_job_item(item: dict | None) -> dict | None:
    if not isinstance(item, dict):
        return item
    out = dict(item)
    if isinstance(out.get('s3_urls'), list):
        out['s3_urls'] = [s3_to_public_url(url) for url in out.get('s3_urls')]
    if isinstance(out.get('image_pairs'), list):
        pairs = []
        for pair in out.get('image_pairs') or []:
            if not isinstance(pair, dict):
                pairs.append(pair)
                continue
            pair = dict(pair)
            pair['s3_url'] = s3_to_public_url(pair.get('s3_url'))
            pairs.append(pair)
        out['image_pairs'] = pairs
    if out.get('s3_url'):
        out['s3_url'] = s3_to_public_url(out.get('s3_url'))
    if out.get('old_s3_url'):
        out['old_s3_url'] = s3_to_public_url(out.get('old_s3_url')) if str(out.get('old_s3_url')).startswith('s3://') else out.get('old_s3_url')
    if out.get('new_s3_url'):
        out['new_s3_url'] = s3_to_public_url(out.get('new_s3_url')) if str(out.get('new_s3_url')).startswith('s3://') else out.get('new_s3_url')
    return out


def _publicize_job_payload(job: dict | None, *, include_items: bool = True) -> dict | None:
    if not isinstance(job, dict):
        return job
    out = dict(job)
    if include_items:
        if isinstance(out.get('items'), list):
            out['items'] = [_publicize_job_item(item) for item in out.get('items')]
    else:
        out.pop('items', None)
    metadata = normalize_job_metadata(
        kind=out.get('kind'),
        job_family=out.get('job_family'),
        dry_run=out.get('dry_run'),
    )
    out['job_family'] = metadata['job_family']
    out['dry_run'] = metadata['dry_run']
    out['kind'] = metadata['kind']
    return out


def persist_upload_job_item(*, dataset_id: str, bucket: str, row: dict[str, Any], item: dict[str, Any]) -> None:
    state_conn = db_connect()
    try:
        load_s3_state(conn=state_conn, force=True)
        goods_id = normalize_goods_id(dataset_id, row.get('id'))
        source_urls = [str(url).strip() for url in (item.get('source_urls') or []) if isinstance(url, str) and str(url).strip()]
        image_pairs = []
        cfg = effective_s3_config()
        ep = cfg.get('endpoint_url')
        rg = cfg.get('region_name')
        for pair in item.get('image_pairs') or []:
            if not isinstance(pair, dict):
                continue
            source_url = str(pair.get('source_url') or '').strip()
            raw_s3_url = str(pair.get('s3_url') or '').strip()
            if not source_url or not raw_s3_url:
                continue
            public_s3 = s3_to_public_url(raw_s3_url, ep, rg)
            image_pairs.append({
                'source_url': source_url,
                's3_url': public_s3,
                'key': pair.get('key'),
                'status': pair.get('status'),
            })
        S3_STATE.setdefault('objects', {})[goods_id] = {
            'dataset_id': dataset_id,
            'product_id': str(row.get('id')),
            'goods_id': goods_id,
            'source_url': source_urls[0] if source_urls else None,
            's3_url': image_pairs[0]['s3_url'] if image_pairs else None,
            'bucket': bucket,
            'key': image_pairs[0].get('key') if image_pairs else None,
            'source_image_urls': source_urls,
            's3_image_urls': [pair['s3_url'] for pair in image_pairs],
            'image_pairs': image_pairs,
            'source_image_count': len(source_urls),
            's3_image_count': len(image_pairs),
            'failed_image_count': int(item.get('image_failed') or 0),
            'saved_on_s3': bool(item.get('saved_on_s3')),
            'saved_at': time.time(),
        }
        save_s3_state(conn=state_conn)
    finally:
        state_conn.close()


def make_s3_job_context(db_conn=None) -> dict:
    return {
        'root': ROOT,
        'allowed_datasets': ALLOWED_DATASETS,
        'db_connect': db_connect,
        'db_conn': db_conn,
        'effective_s3_config': effective_s3_config,
        'resolve_aws_public_url': resolve_aws_public_url,
        'resolve_aws_bucket': resolve_aws_bucket,
        'resolve_aws_prefix': resolve_aws_prefix,
        'resolve_s3_region': resolve_s3_region,
        'load_s3_state': load_s3_state,
        'parse_json_list': parse_json_list,
        'persist_upload_item': persist_upload_job_item,
        'update_job': S3_JOB_MANAGER.update_job,
    }


def _maybe_migrate_legacy_s3_state(conn):
    # Import legacy file-based S3 state only when the SQL tables are still empty.
    # Otherwise the legacy file would keep overwriting newer DB-backed state.
    existing_count = conn.execute('SELECT COUNT(*) FROM s3_objects').fetchone()[0]
    if int(existing_count or 0) > 0:
        return
    legacy = _load_legacy_s3_state_payload()
    config = legacy.get('config', {})
    objects = legacy.get('objects', {})
    if config:
        conn.execute(
            '''
            INSERT INTO s3_config (config_key, config_value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(config_key) DO UPDATE SET
                config_value = excluded.config_value,
                updated_at = excluded.updated_at
            ''',
            ('config', json.dumps(config, ensure_ascii=False), time.time()),
        )
    for goods_id, payload in objects.items():
        normalized = _normalize_s3_object(goods_id, payload)
        if not normalized:
            continue
        conn.execute(
            '''
            INSERT INTO s3_objects (
                goods_id, dataset_id, product_id, source_url, s3_url, bucket, object_key,
                source_image_urls_json, s3_image_urls_json, image_pairs_json,
                source_image_count, s3_image_count, failed_image_count, saved_on_s3, saved_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(goods_id) DO UPDATE SET
                dataset_id = excluded.dataset_id,
                product_id = excluded.product_id,
                source_url = excluded.source_url,
                s3_url = excluded.s3_url,
                bucket = excluded.bucket,
                object_key = excluded.object_key,
                source_image_urls_json = excluded.source_image_urls_json,
                s3_image_urls_json = excluded.s3_image_urls_json,
                image_pairs_json = excluded.image_pairs_json,
                source_image_count = excluded.source_image_count,
                s3_image_count = excluded.s3_image_count,
                failed_image_count = excluded.failed_image_count,
                saved_on_s3 = excluded.saved_on_s3,
                saved_at = excluded.saved_at,
                updated_at = excluded.updated_at
            ''',
            (
                normalized['goods_id'],
                normalized['dataset_id'],
                normalized['product_id'],
                normalized['source_url'],
                normalized['s3_url'],
                normalized['bucket'],
                normalized['key'],
                json.dumps(normalized['source_image_urls'], ensure_ascii=False),
                json.dumps(normalized['s3_image_urls'], ensure_ascii=False),
                json.dumps(normalized['image_pairs'], ensure_ascii=False),
                normalized['source_image_count'],
                normalized['s3_image_count'],
                normalized['failed_image_count'],
                1 if normalized['saved_on_s3'] else 0,
                normalized['saved_at'],
                time.time(),
            ),
        )


def _maybe_migrate_legacy_s3_jobs(conn):
    for raw_job in _load_legacy_s3_jobs_payload():
        if not isinstance(raw_job, dict) or not raw_job.get('job_id'):
            continue
        normalized = _publicize_job_payload(raw_job) or raw_job
        conn.execute(
            '''
            INSERT INTO s3_jobs (job_id, payload_json, started_at, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                payload_json = excluded.payload_json,
                started_at = excluded.started_at,
                updated_at = excluded.updated_at
            ''',
            (
                str(raw_job.get('job_id')),
                json.dumps(normalized, ensure_ascii=False, separators=(',', ':')),
                float(raw_job.get('started_at') or time.time()),
                time.time(),
            ),
        )


def load_s3_state(conn=None, force=False):
    global S3_STATE, S3_STATE_MTIME
    if not force and S3_STATE_MTIME and (time.time() - S3_STATE_MTIME) < 2:
        return S3_STATE
    owns_conn = conn is None
    conn = conn or db_connect()
    try:
        _maybe_migrate_legacy_s3_state(conn)
        row = conn.execute('SELECT config_value FROM s3_config WHERE config_key = ?', ('config',)).fetchone()
        config = {}
        if row and row['config_value']:
            try:
                config = _sanitize_s3_config(json.loads(row['config_value']))
            except Exception:
                config = {}
        objects = {}
        rows = conn.execute(
            '''
            SELECT goods_id, dataset_id, product_id, source_url, s3_url, bucket, object_key,
                   source_image_urls_json, s3_image_urls_json, image_pairs_json,
                   source_image_count, s3_image_count, failed_image_count, saved_on_s3, saved_at
            FROM s3_objects
            '''
        ).fetchall()
        for row in rows:
            goods_id = str(row['goods_id'])
            objects[goods_id] = {
                'dataset_id': row['dataset_id'],
                'product_id': row['product_id'],
                'goods_id': goods_id,
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
        S3_STATE = {'config': config, 'objects': objects}
        S3_STATE_MTIME = time.time()
        return S3_STATE
    finally:
        if owns_conn:
            conn.close()


def save_s3_state(conn=None):
    global S3_STATE_MTIME
    owns_conn = conn is None
    conn = conn or db_connect()
    try:
        config = _sanitize_s3_config(S3_STATE.get('config', {}))
        conn.execute(
            '''
            INSERT INTO s3_config (config_key, config_value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(config_key) DO UPDATE SET
                config_value = excluded.config_value,
                updated_at = excluded.updated_at
            ''',
            ('config', json.dumps(config, ensure_ascii=False), time.time()),
        )
        for goods_id, payload in (S3_STATE.get('objects', {}) or {}).items():
            normalized = _normalize_s3_object(goods_id, payload)
            if not normalized:
                continue
            conn.execute(
                '''
                INSERT INTO s3_objects (
                    goods_id, dataset_id, product_id, source_url, s3_url, bucket, object_key,
                    source_image_urls_json, s3_image_urls_json, image_pairs_json,
                    source_image_count, s3_image_count, failed_image_count, saved_on_s3, saved_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(goods_id) DO UPDATE SET
                    dataset_id = excluded.dataset_id,
                    product_id = excluded.product_id,
                    source_url = excluded.source_url,
                    s3_url = excluded.s3_url,
                    bucket = excluded.bucket,
                    object_key = excluded.object_key,
                    source_image_urls_json = excluded.source_image_urls_json,
                    s3_image_urls_json = excluded.s3_image_urls_json,
                    image_pairs_json = excluded.image_pairs_json,
                    source_image_count = excluded.source_image_count,
                    s3_image_count = excluded.s3_image_count,
                    failed_image_count = excluded.failed_image_count,
                    saved_on_s3 = excluded.saved_on_s3,
                    saved_at = excluded.saved_at,
                    updated_at = excluded.updated_at
                ''',
                (
                    normalized['goods_id'],
                    normalized['dataset_id'],
                    normalized['product_id'],
                    normalized['source_url'],
                    normalized['s3_url'],
                    normalized['bucket'],
                    normalized['key'],
                    json.dumps(normalized['source_image_urls'], ensure_ascii=False),
                    json.dumps(normalized['s3_image_urls'], ensure_ascii=False),
                    json.dumps(normalized['image_pairs'], ensure_ascii=False),
                    normalized['source_image_count'],
                    normalized['s3_image_count'],
                    normalized['failed_image_count'],
                    1 if normalized['saved_on_s3'] else 0,
                    normalized['saved_at'],
                    time.time(),
                ),
            )
        conn.commit()
        S3_STATE_MTIME = time.time()
    finally:
        if owns_conn:
            conn.close()


def db_connect():
    return db_connect_impl(
        DB_PATH,
        sql_unaccent_fn=_sql_unaccent,
        migrate_legacy_state=_maybe_migrate_legacy_s3_state,
        migrate_legacy_jobs=_maybe_migrate_legacy_s3_jobs,
    )


def health_status():
    return health_status_impl(DB_PATH)


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
    # Intentionally disabled in stable dev mode.
    # Keep old servers alive unless the user explicitly stops them.
    return


def normalize_goods_id(dataset_id, product_id):
    return f'{dataset_id}:{product_id}'


def _load_s3_jobs_from_db() -> list[dict]:
    conn = db_connect()
    try:
        _maybe_migrate_legacy_s3_jobs(conn)
        rows = conn.execute('SELECT payload_json FROM s3_jobs ORDER BY COALESCE(started_at, 0) DESC, job_id DESC').fetchall()
        payloads = []
        for row in rows:
            try:
                payloads.append(_publicize_job_payload(json.loads(row['payload_json'])))
            except Exception:
                continue
        return payloads
    finally:
        conn.close()


def _save_s3_jobs_to_db(jobs: list[dict]) -> None:
    attempts = 5
    last_exc = None
    for attempt in range(attempts):
        conn = db_connect()
        try:
            seen = set()
            now = time.time()
            for raw_job in jobs:
                if not isinstance(raw_job, dict) or not raw_job.get('job_id'):
                    continue
                job_id = str(raw_job.get('job_id'))
                seen.add(job_id)
                normalized = _publicize_job_payload(raw_job) or raw_job
                conn.execute(
                    '''
                    INSERT INTO s3_jobs (job_id, payload_json, started_at, updated_at)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(job_id) DO UPDATE SET
                        payload_json = excluded.payload_json,
                        started_at = excluded.started_at,
                        updated_at = excluded.updated_at
                    ''',
                    (
                        job_id,
                        json.dumps(normalized, ensure_ascii=False, separators=(',', ':')),
                        float(raw_job.get('started_at') or now),
                        now,
                    ),
                )
            if seen:
                placeholders = ','.join('?' for _ in seen)
                conn.execute(f'DELETE FROM s3_jobs WHERE job_id NOT IN ({placeholders})', tuple(seen))
            else:
                conn.execute('DELETE FROM s3_jobs')
            conn.commit()
            return
        except sqlite3.OperationalError as exc:
            last_exc = exc
            message = str(exc).lower()
            if 'database is locked' not in message or attempt == attempts - 1:
                raise
            time.sleep(0.15 * (attempt + 1))
        finally:
            conn.close()
    if last_exc:
        raise last_exc


S3_JOB_MANAGER = S3JobManager(load_jobs_fn=_load_s3_jobs_from_db, save_jobs_fn=_save_s3_jobs_to_db, db_connect_fn=db_connect)


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self._sent_cache_control = False
        super().__init__(*args, directory=str(PUBLIC_DIR), **kwargs)

    def send_header(self, keyword, value):
        if str(keyword).lower() == 'cache-control':
            self._sent_cache_control = True
        return super().send_header(keyword, value)

    def end_headers(self):
        if not self._sent_cache_control:
            super().send_header('Cache-Control', 'no-store')
        for key, value in SECURITY_HEADERS.items():
            super().send_header(key, value)
        self._sent_cache_control = False
        super().end_headers()

    def list_directory(self, path):
        return reject_directory_listing(self)

    def do_OPTIONS(self):
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header('Content-Length', '0')
        for key, value in CORS_HEADERS.items():
            self.send_header(key, value)
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        try:
            if parsed.path == '/':
                return html_response(self, (PUBLIC_DIR / 'index.html').read_bytes())
            if parsed.path == '/healthz':
                status = health_status()
                return json_response(self, {'data': status}, status=HTTPStatus.OK if status.get('ok') else HTTPStatus.SERVICE_UNAVAILABLE)
            if parsed.path in {'/openapi.json', '/api/openapi.json'}:
                return json_response(self, OPENAPI_SPEC)
            if parsed.path.startswith('/api/') and not api_token_is_valid(self):
                return api_unauthorized_response(self)
            if parsed.path == '/docs':
                content = DOCS_PATH.read_bytes()
                return html_response(self, content)
            if parsed.path == '/s3':
                content = S3_PAGE_PATH.read_bytes()
                return html_response(self, content)
            if parsed.path == '/api/s3/auth-check':
                return json_response(self, {'data': {'authenticated': auth_required(self)}})
            if parsed.path == '/api/s3/config' and not s3_access_is_valid(self):
                return s3_admin_required_response(self)
            if parsed.path.startswith('/api/s3/') and parsed.path != '/api/s3/auth' and not s3_access_is_valid(self):
                return s3_admin_required_response(self)
            if parsed.path == '/api/s3/auth':
                return self.handle_s3_auth()
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
            if parsed.path == '/api/s3/config':
                return self.handle_s3_config_get()
            if parsed.path == '/api/s3/upload-jobs':
                return self.handle_s3_family_jobs_list(UPLOAD_JOB_FAMILY)
            if parsed.path == '/api/s3/url-migration-jobs':
                return self.handle_s3_family_jobs_list(URL_MIGRATION_JOB_FAMILY)
            if parsed.path == '/api/s3/state-cleanup-jobs':
                return self.handle_s3_family_jobs_list(STATE_CLEANUP_JOB_FAMILY)
            if parsed.path == '/api/s3/migration-summary':
                return self.handle_s3_family_summary(URL_MIGRATION_JOB_FAMILY)
            if parsed.path == '/api/s3/cleanup-summary':
                return self.handle_s3_family_summary(STATE_CLEANUP_JOB_FAMILY)
            if parsed.path.startswith('/api/s3/jobs/'):
                job_id = parsed.path.split('/api/s3/jobs/', 1)[1].strip('/')
                return self.handle_s3_job_detail(job_id)
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
            if parsed.path == '/api/s3/auth':
                if not api_token_is_valid(self):
                    return api_unauthorized_response(self)
                return self.handle_s3_auth()
            if parsed.path == '/api/s3/upload-jobs':
                if not api_token_is_valid(self):
                    return api_unauthorized_response(self)
                if not s3_access_is_valid(self):
                    return s3_admin_required_response(self)
                return self.handle_s3_family_job_create(UPLOAD_JOB_FAMILY)
            if parsed.path == '/api/s3/url-migration-jobs':
                if not api_token_is_valid(self):
                    return api_unauthorized_response(self)
                if not s3_access_is_valid(self):
                    return s3_admin_required_response(self)
                return self.handle_s3_family_job_create(URL_MIGRATION_JOB_FAMILY)
            if parsed.path == '/api/s3/state-cleanup-jobs':
                if not api_token_is_valid(self):
                    return api_unauthorized_response(self)
                if not s3_access_is_valid(self):
                    return s3_admin_required_response(self)
                return self.handle_s3_family_job_create(STATE_CLEANUP_JOB_FAMILY)
            if parsed.path.startswith('/api/s3/jobs/') and parsed.path.endswith('/cancel'):
                if not api_token_is_valid(self):
                    return api_unauthorized_response(self)
                if not s3_access_is_valid(self):
                    return s3_admin_required_response(self)
                job_id = parsed.path.split('/api/s3/jobs/', 1)[1].rsplit('/cancel', 1)[0].strip('/')
                return self.handle_s3_job_cancel(job_id)
            if parsed.path.startswith('/api/') and not api_token_is_valid(self):
                return api_unauthorized_response(self)
            return error_response(self, 'Not found', HTTPStatus.NOT_FOUND, code='not_found')
        except sqlite3.Error as exc:
            return error_response(self, f'Database error: {exc}', HTTPStatus.INTERNAL_SERVER_ERROR, code='database_error')
        except ValueError as exc:
            return error_response(self, str(exc), HTTPStatus.BAD_REQUEST, code='invalid_request')
        except Exception as exc:
            traceback.print_exc()
            return error_response(self, f'Internal server error: {exc}', HTTPStatus.INTERNAL_SERVER_ERROR, code='internal_error')

    def _read_json_body(self):
        try:
            length = int(self.headers.get('Content-Length') or '0')
        except ValueError as exc:
            raise ValueError('Invalid Content-Length header') from exc
        if length <= 0:
            return {}
        if length > MAX_REQUEST_BODY_BYTES:
            raise ValueError(f'Request body too large (max {MAX_REQUEST_BODY_BYTES} bytes)')
        raw = self.rfile.read(length)
        if not raw:
            return {}
        return json.loads(raw.decode('utf-8'))

    def handle_datasets(self, query_string):
        return catalog_routes.handle_datasets(self, query_string, db_connect_fn=db_connect)

    def handle_categories(self, query_string):
        return catalog_routes.handle_categories(
            self,
            query_string,
            db_connect_fn=db_connect,
            allowed_datasets=ALLOWED_DATASETS,
            max_page_size=MAX_PAGE_SIZE,
        )

    def handle_category(self, slug, query_string):
        return catalog_routes.handle_category(
            self,
            slug,
            query_string,
            db_connect_fn=db_connect,
            allowed_datasets=ALLOWED_DATASETS,
        )

    def handle_products(self, query_string):
        return catalog_routes.handle_products(
            self,
            query_string,
            db_connect_fn=db_connect,
            allowed_datasets=ALLOWED_DATASETS,
            default_page_size=DEFAULT_PAGE_SIZE,
            max_page_size=MAX_PAGE_SIZE,
            allowed_sorts=ALLOWED_SORTS,
        )

    def handle_product(self, goods_id, query_string):
        return catalog_routes.handle_product(
            self,
            goods_id,
            query_string,
            db_connect_fn=db_connect,
            allowed_datasets=ALLOWED_DATASETS,
        )

    def handle_s3_auth(self):
        return auth_handle_s3_auth(self, self._read_json_body)

    def handle_s3_config_get(self):
        return s3_routes.handle_s3_config_get(self, effective_s3_config_fn=effective_s3_config)

    def handle_s3_family_jobs_list(self, job_family: str):
        return s3_routes.handle_s3_family_jobs_list(
            self,
            job_family,
            manager=S3_JOB_MANAGER,
            load_s3_state_fn=load_s3_state,
            effective_s3_config_fn=effective_s3_config,
            publicize_job_payload=_publicize_job_payload,
        )

    def handle_s3_family_summary(self, job_family: str):
        return s3_routes.handle_s3_family_summary(
            self,
            job_family,
            make_context_fn=make_s3_job_context,
        )

    def handle_s3_family_job_create(self, job_family: str):
        return s3_routes.handle_s3_family_job_create(
            self,
            job_family,
            manager=S3_JOB_MANAGER,
            make_context_fn=make_s3_job_context,
            publicize_job_payload=_publicize_job_payload,
        )

    def handle_s3_jobs_list(self):
        return self.handle_s3_family_jobs_list(UPLOAD_JOB_FAMILY)

    def handle_s3_jobs_create(self):
        return self.handle_s3_family_job_create(UPLOAD_JOB_FAMILY)

    def handle_s3_migration_summary(self):
        return self.handle_s3_family_summary(URL_MIGRATION_JOB_FAMILY)

    def handle_s3_cleanup_summary(self):
        return self.handle_s3_family_summary(STATE_CLEANUP_JOB_FAMILY)

    def handle_s3_job_cancel(self, job_id):
        return s3_routes.handle_s3_job_cancel(
            self,
            job_id,
            manager=S3_JOB_MANAGER,
            publicize_job_payload=_publicize_job_payload,
        )

    def handle_s3_job_detail(self, job_id):
        return s3_routes.handle_s3_job_detail(
            self,
            job_id,
            manager=S3_JOB_MANAGER,
            load_s3_state_fn=load_s3_state,
            publicize_job_item=_publicize_job_item,
            publicize_job_payload=_publicize_job_payload,
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

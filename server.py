#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import hmac
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
from urllib.parse import parse_qs, quote, unquote, urlparse

from dataset_service import load_dotenv
from s3_jobs import S3JobManager

ROOT = Path(__file__).resolve().parent
DB_PATH = Path(os.getenv('FAST_FASHION_DB_PATH', str(ROOT / 'catalog.db')))
DOCS_PATH = ROOT / 'docs.html'
S3_PAGE_PATH = ROOT / 's3.html'
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

CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
    'Access-Control-Max-Age': '86400',
    'X-Content-Type-Options': 'nosniff',
    'Referrer-Policy': 'no-referrer',
    'X-Frame-Options': 'DENY',
}
API_BEARER_TOKEN = os.getenv('FAST_FASHION_API_TOKEN', '').strip()
S3_PASSWORD = os.getenv('FAST_FASHION_S3_PASSWORD', '').strip()
S3_AUTH_TOKENS: dict[str, float] = {}
S3_AUTH_TTL_SECONDS = 60 * 60


def _env_nonempty(*names: str) -> str:
    for name in names:
        value = os.getenv(name, '').strip()
        if value:
            return value
    return ''


def resolve_s3_region(endpoint_url: str | None = None, explicit_region: str | None = None) -> str | None:
    region = (explicit_region or '').strip()
    if region:
        return region
    endpoint = (endpoint_url or '').strip().lower()
    if 'r2.cloudflarestorage.com' in endpoint:
        return 'auto'
    env_region = _env_nonempty('AWS_REGION', 'AWS_DEFAULT_REGION', 'S3_REGION')
    if env_region:
        return env_region
    return 'us-east-1' if endpoint else None

OPENAPI_SPEC = {
    'openapi': '3.1.0',
    'info': {
        'title': 'Fast Fashion Dashboard API',
        'version': '1.1.0',
        'description': 'Read-only API for categories/products plus background S3 jobs.',
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
                'description': 'When true, returns only products whose runtime S3 state is marked as saved in SQLite.',
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
                'required': ['name', 'slug', 'top_category_name', 'source_url', 'image_url'],
                'properties': {
                    'name': {'type': 'string'},
                    'slug': {'type': 'string'},
                    'top_category_name': {'type': ['string', 'null']},
                    'source_url': {'type': ['string', 'null']},
                    'image_url': {'type': ['string', 'null']},
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
                    'region_name': {'type': ['string', 'null']},
                    'bucket': {'type': ['string', 'null']},
                    'prefix': {'type': ['string', 'null']},
                    'endpoint_url': {'type': ['string', 'null']},
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
                    'last_message': {'type': ['string', 'null']},
                    'items': {'type': ['array', 'null'], 'items': {'$ref': '#/components/schemas/S3JobItem'}},
                },
            },
            'S3JobsListResponse': {
                'type': 'object',
                'required': ['data', 'config'],
                'properties': {
                    'data': {'type': 'array', 'items': {'$ref': '#/components/schemas/S3JobState'}},
                    'config': {'$ref': '#/components/schemas/S3Config'},
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
            'S3JobCreateRequest': {
                'type': 'object',
                'properties': {
                    'dataset_id': {'type': 'string', 'enum': ['shein', 'asos'], 'default': 'shein'},
                    'source': {'type': 'string', 'default': 'products'},
                    'limit': {'type': 'integer', 'minimum': 1, 'default': 100},
                    'concurrency': {'type': 'integer', 'minimum': 1, 'maximum': 24, 'default': 4},
                    'bucket': {'type': 'string'},
                    'prefix': {'type': 'string'},
                    'region_name': {'type': 'string'},
                    'endpoint_url': {'type': 'string'},
                    'source_filter': {'type': 'string'},
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
                'description': 'Returns stable category resources for a dataset.',
                'security': [{'bearerAuth': []}],
                'parameters': [
                    {'$ref': '#/components/parameters/dataset'},
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
                'description': 'Returns a single category resource by slug.',
                'security': [{'bearerAuth': []}],
                'parameters': [
                    {'$ref': '#/components/parameters/categorySlug'},
                    {'$ref': '#/components/parameters/dataset'},
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
                'summary': 'Get S3 config',
                'description': 'Returns the non-secret S3 configuration visible to the UI.',
                'security': [{'bearerAuth': []}],
                'responses': {
                    '200': {
                        'description': 'S3 config.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/S3ConfigResponse'}}},
                    },
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                },
            },
            'post': {
                'tags': ['s3'],
                'operationId': 'updateS3Config',
                'summary': 'Update S3 config',
                'description': 'Updates the non-secret S3 configuration persisted on disk.',
                'security': [{'bearerAuth': []}],
                'requestBody': {
                    'required': True,
                    'content': {
                        'application/json': {
                            'schema': {'$ref': '#/components/schemas/S3Config'},
                        }
                    },
                },
                'responses': {
                    '200': {
                        'description': 'Updated S3 config.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/S3ConfigResponse'}}},
                    },
                    '400': {'$ref': '#/components/responses/BadRequest'},
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                },
            },
        },
        '/api/s3/jobs': {
            'get': {
                'tags': ['s3'],
                'operationId': 'listS3Jobs',
                'summary': 'List jobs',
                'description': 'Returns S3 job history plus the effective non-secret config.',
                'security': [{'bearerAuth': []}],
                'responses': {
                    '200': {
                        'description': 'S3 jobs list.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/S3JobsListResponse'}}},
                    },
                    '401': {'$ref': '#/components/responses/Unauthorized'},
                },
            },
            'post': {
                'tags': ['s3'],
                'operationId': 'createS3Job',
                'summary': 'Create job',
                'description': 'Starts a new S3 background sync job for a dataset.',
                'security': [{'bearerAuth': []}],
                'requestBody': {
                    'required': True,
                    'content': {
                        'application/json': {
                            'schema': {'$ref': '#/components/schemas/S3JobCreateRequest'},
                        }
                    },
                },
                'responses': {
                    '202': {
                        'description': 'Job created.',
                        'content': {'application/json': {'schema': {'$ref': '#/components/schemas/S3JobAcceptedResponse'}}},
                    },
                    '400': {'$ref': '#/components/responses/BadRequest'},
                    '401': {'$ref': '#/components/responses/Unauthorized'},
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
    },
}


def _sanitize_s3_config(config: dict | None) -> dict:
    cleaned = dict(config or {})
    for secret_key in ('aws_access_key_id', 'aws_secret_access_key', 'aws_session_token'):
        cleaned.pop(secret_key, None)
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


def _maybe_migrate_legacy_s3_state(conn):
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
                json.dumps(raw_job, ensure_ascii=False, separators=(',', ':')),
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
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS s3_config (
            config_key TEXT PRIMARY KEY,
            config_value TEXT NOT NULL,
            updated_at REAL NOT NULL
        )
        '''
    )
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS s3_objects (
            goods_id TEXT PRIMARY KEY,
            dataset_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            source_url TEXT,
            s3_url TEXT,
            bucket TEXT,
            object_key TEXT,
            source_image_urls_json TEXT NOT NULL DEFAULT '[]',
            s3_image_urls_json TEXT NOT NULL DEFAULT '[]',
            image_pairs_json TEXT NOT NULL DEFAULT '[]',
            source_image_count INTEGER NOT NULL DEFAULT 0,
            s3_image_count INTEGER NOT NULL DEFAULT 0,
            failed_image_count INTEGER NOT NULL DEFAULT 0,
            saved_on_s3 INTEGER NOT NULL DEFAULT 0,
            saved_at REAL,
            updated_at REAL NOT NULL
        )
        '''
    )
    conn.execute('CREATE INDEX IF NOT EXISTS idx_s3_objects_dataset_product ON s3_objects(dataset_id, product_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_s3_objects_saved ON s3_objects(saved_on_s3)')
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS s3_jobs (
            job_id TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            started_at REAL,
            updated_at REAL NOT NULL
        )
        '''
    )
    conn.execute('CREATE INDEX IF NOT EXISTS idx_s3_jobs_started_at ON s3_jobs(started_at DESC)')
    _maybe_migrate_legacy_s3_state(conn)
    _maybe_migrate_legacy_s3_jobs(conn)
    conn.commit()
    return conn


def health_status():
    status = {
        'ok': False,
        'db_path': str(DB_PATH),
        'db_exists': DB_PATH.exists(),
        'datasets_count': 0,
    }
    if not status['db_exists']:
        return status
    try:
        conn = sqlite3.connect(DB_PATH)
        try:
            row = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='datasets'").fetchone()
            has_datasets_table = bool(row and row[0])
            status['has_datasets_table'] = has_datasets_table
            if not has_datasets_table:
                return status
            count_row = conn.execute('SELECT COUNT(*) FROM datasets').fetchone()
            status['datasets_count'] = int(count_row[0] or 0) if count_row else 0
            status['ok'] = status['datasets_count'] > 0
            return status
        finally:
            conn.close()
    except Exception as exc:
        status['error'] = str(exc)
        return status


def json_response(handler, payload, status=HTTPStatus.OK, extra_headers=None):
    body = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    handler.send_response(status)
    handler.send_header('Content-Type', 'application/json; charset=utf-8')
    handler.send_header('Content-Length', str(len(body)))
    handler.send_header('Cache-Control', 'no-store')
    for key, value in CORS_HEADERS.items():
        handler.send_header(key, value)
    for key, value in (extra_headers or {}).items():
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


def error_response(handler, message, status=HTTPStatus.BAD_REQUEST, code='bad_request', extra_headers=None):
    json_response(handler, {'error': {'code': code, 'message': message}}, status=status, extra_headers=extra_headers)


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


def issue_s3_token() -> str:
    raw = f'{os.getpid()}:{time.time()}:{os.urandom(24).hex()}'
    token = hashlib.sha256(raw.encode('utf-8')).hexdigest()
    S3_AUTH_TOKENS[token] = time.time() + S3_AUTH_TTL_SECONDS
    return token


def token_is_valid(token: str | None) -> bool:
    if not token:
        return False
    expiry = S3_AUTH_TOKENS.get(token)
    if not expiry:
        return False
    if expiry < time.time():
        S3_AUTH_TOKENS.pop(token, None)
        return False
    return True


def auth_required(handler) -> bool:
    cookie = handler.headers.get('Cookie', '') or ''
    token = None
    for chunk in cookie.split(';'):
        chunk = chunk.strip()
        if chunk.startswith('ff_s3_auth='):
            token = chunk.split('=', 1)[1]
            break
    if token_is_valid(token):
        return True
    return False


def get_bearer_token(handler) -> str:
    header = (handler.headers.get('Authorization', '') or '').strip()
    if not header:
        return ''
    scheme, _, token = header.partition(' ')
    if scheme.lower() != 'bearer':
        return ''
    return token.strip()


def api_token_is_valid(handler) -> bool:
    if not API_BEARER_TOKEN:
        return True
    token = get_bearer_token(handler)
    if not token:
        return False
    return hmac.compare_digest(token, API_BEARER_TOKEN)


def api_unauthorized_response(handler):
    return error_response(
        handler,
        'Authorization token required',
        HTTPStatus.UNAUTHORIZED,
        code='unauthorized',
        extra_headers={
            'WWW-Authenticate': 'Bearer realm="fast-fashion-dashboard"',
            'X-Fast-Fashion-Auth-Required': 'true',
        },
    )


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
                payloads.append(json.loads(row['payload_json']))
            except Exception:
                continue
        return payloads
    finally:
        conn.close()


def _save_s3_jobs_to_db(jobs: list[dict]) -> None:
    conn = db_connect()
    try:
        seen = set()
        now = time.time()
        for raw_job in jobs:
            if not isinstance(raw_job, dict) or not raw_job.get('job_id'):
                continue
            job_id = str(raw_job.get('job_id'))
            seen.add(job_id)
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
                    json.dumps(raw_job, ensure_ascii=False, separators=(',', ':')),
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
    finally:
        conn.close()


S3_JOB_MANAGER = S3JobManager(load_jobs_fn=_load_s3_jobs_from_db, save_jobs_fn=_save_s3_jobs_to_db)


class Handler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self._sent_cache_control = False
        super().__init__(*args, directory=str(ROOT), **kwargs)

    def send_header(self, keyword, value):
        if str(keyword).lower() == 'cache-control':
            self._sent_cache_control = True
        return super().send_header(keyword, value)

    def end_headers(self):
        if not self._sent_cache_control:
            super().send_header('Cache-Control', 'no-store')
        self._sent_cache_control = False
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(HTTPStatus.NO_CONTENT)
        self.send_header('Content-Length', '0')
        for key, value in CORS_HEADERS.items():
            self.send_header(key, value)
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        try:
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
            if parsed.path == '/api/s3/config' and not auth_required(self):
                return error_response(self, 'Authentication required', HTTPStatus.UNAUTHORIZED, code='unauthorized')
            if parsed.path == '/api/s3/jobs' and not auth_required(self):
                return error_response(self, 'Authentication required', HTTPStatus.UNAUTHORIZED, code='unauthorized')
            if parsed.path.startswith('/api/s3/jobs/') and not auth_required(self):
                return error_response(self, 'Authentication required', HTTPStatus.UNAUTHORIZED, code='unauthorized')
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
            if parsed.path == '/api/s3/jobs':
                return self.handle_s3_jobs_list()
            if parsed.path.startswith('/api/s3/jobs/'):
                job_id = parsed.path.split('/api/s3/jobs/', 1)[1].strip('/')
                return self.handle_s3_job_detail(job_id)
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
            if parsed.path.startswith('/api/') and not api_token_is_valid(self):
                return api_unauthorized_response(self)
            if parsed.path == '/api/s3/jobs':
                return self.handle_s3_jobs_create()
            if parsed.path == '/api/s3/auth':
                return self.handle_s3_auth()
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

    def _s3_object_for(self, conn, dataset_id, product_id):
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

    def _source_images_for_row(self, row):
        urls = []
        for value in [row['image']] + parse_json_list(row['image_urls_json']):
            if not isinstance(value, str):
                continue
            cleaned = value.strip()
            if not cleaned or not cleaned.lower().startswith(('http://', 'https://')):
                continue
            urls.append(cleaned)
        return list(dict.fromkeys(urls))[:20]

    def _s3_images_for_product(self, s3_object, source_images):
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

    def _product_resource(self, row, dataset_row, base_url, conn=None):
        product_id = str(row['id']).strip()
        goods_id = normalize_goods_id(dataset_row['id'], product_id)
        name = (row['name'] or '').strip() or 'Sans nom'
        description = (row['description'] or '').strip() or 'Imported from scraper feed.'
        image_urls = self._source_images_for_row(row)
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
        conn = conn or db_connect()
        try:
            s3_object = self._s3_object_for(conn, dataset_row['id'], product_id)
        finally:
            if owns_conn:
                conn.close()
        image_pairs, s3_image_urls = self._s3_images_for_product(s3_object, image_urls)
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

    def _legacy_product_payload(self, row, dataset_row, base_url, resource_product=None):
        product = resource_product or self._product_resource(row, dataset_row, base_url)
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
        saved_on_s3_only = parse_bool(params.get('savedOnS3', ['false'])[0])
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
        if saved_on_s3_only:
            where.append("EXISTS (SELECT 1 FROM s3_objects o WHERE o.dataset_id = p.dataset_id AND o.product_id = p.id AND o.saved_on_s3 = 1)")
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
        base_url = get_base_url(self)
        resource_products = [self._product_resource(row, dataset_row, base_url, conn) for row in product_rows]
        conn.close()

        if format_mode == 'resource':
            json_response(self, {'dataset': dict(dataset_row), 'data': resource_products, 'pagination': {'page': page, 'pageSize': page_size, 'total': total, 'totalPages': total_pages, 'from': 0 if total == 0 else offset + 1, 'to': min(offset + page_size, total)}})
            return

        legacy_products = [self._legacy_product_payload(row, dataset_row, base_url, product) for row, product in zip(product_rows, resource_products)]

        json_response(self, {'dataset': dict(dataset_row), 'products': legacy_products, 'pagination': {'page': page, 'pageSize': page_size, 'total': total, 'totalPages': total_pages, 'from': 0 if total == 0 else offset + 1, 'to': min(offset + page_size, total)}})

    def handle_product(self, goods_id, query_string):
        params = parse_qs(query_string)
        dataset_id = (params.get('dataset', ['shein'])[0] or 'shein').strip().lower()
        if dataset_id not in ALLOWED_DATASETS:
            raise ValueError(f'Unknown dataset: {dataset_id}')
        requested_id = unquote(str(goods_id or '')).strip()
        product_id = requested_id.split(':', 1)[1] if ':' in requested_id else requested_id
        conn = db_connect()
        dataset_row = self._dataset_row(conn, dataset_id)
        row = conn.execute('SELECT * FROM products WHERE dataset_id = ? AND id = ?', (dataset_id, product_id)).fetchone()
        if not row:
            conn.close()
            return error_response(self, f'Product not found: {goods_id}', HTTPStatus.NOT_FOUND, code='not_found')
        base_url = get_base_url(self)
        product_resource = self._product_resource(row, dataset_row, base_url, conn)
        product_display = self._legacy_product_payload(row, dataset_row, base_url, product_resource)
        conn.close()
        json_response(self, {
            'dataset': dict(dataset_row),
            'display': product_display,
            'api': product_resource,
            'data': product_resource,
        })

    def handle_s3_auth(self):
        payload = self._read_json_body()
        if not S3_PASSWORD:
            token = issue_s3_token()
            body = json.dumps({'data': {'authenticated': True, 'expires_in_seconds': S3_AUTH_TTL_SECONDS}}, ensure_ascii=False).encode('utf-8')
            self.send_response(HTTPStatus.OK)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Set-Cookie', f'ff_s3_auth={token}; Max-Age={S3_AUTH_TTL_SECONDS}; Path=/; HttpOnly; SameSite=Strict')
            self.send_header('Content-Length', str(len(body)))
            for key, value in CORS_HEADERS.items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(body)
            return
        password = str(payload.get('password') or '')
        if not hmac.compare_digest(hash_password(password), hash_password(S3_PASSWORD)):
            return error_response(self, 'Invalid password', HTTPStatus.UNAUTHORIZED, code='unauthorized')
        token = issue_s3_token()
        body = json.dumps({'data': {'authenticated': True, 'expires_in_seconds': S3_AUTH_TTL_SECONDS}}, ensure_ascii=False).encode('utf-8')
        self.send_response(HTTPStatus.OK)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Set-Cookie', f'ff_s3_auth={token}; Max-Age={S3_AUTH_TTL_SECONDS}; Path=/; HttpOnly; SameSite=Strict')
        self.send_header('Content-Length', str(len(body)))
        for key, value in CORS_HEADERS.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def handle_s3_config_get(self):
        load_s3_state(force=True)
        config = dict(S3_STATE.get('config', {}))
        config.pop('aws_access_key_id', None)
        config.pop('aws_secret_access_key', None)
        config.pop('aws_session_token', None)
        if not config.get('bucket'):
            config['bucket'] = _env_nonempty('S3_BUCKET')
        if not config.get('endpoint_url'):
            config['endpoint_url'] = _env_nonempty('S3_ENDPOINT_URL')
        config['region_name'] = resolve_s3_region(config.get('endpoint_url'), config.get('region_name'))
        json_response(self, {'data': config})

    def handle_s3_config_update(self):
        payload = self._read_json_body()
        conn = db_connect()
        try:
            load_s3_state(conn=conn, force=True)
            config = S3_STATE.setdefault('config', {})
            for key in ['region_name', 'bucket', 'prefix', 'endpoint_url']:
                if key in payload:
                    config[key] = payload[key]
            for secret_key in ('aws_access_key_id', 'aws_secret_access_key', 'aws_session_token'):
                config.pop(secret_key, None)
            save_s3_state(conn=conn)
            json_response(self, {'data': dict(config)})
        finally:
            conn.close()

    def handle_s3_jobs_list(self):
        load_s3_state(force=True)
        config = dict(S3_STATE.get('config', {}))
        config.pop('aws_access_key_id', None)
        config.pop('aws_secret_access_key', None)
        config.pop('aws_session_token', None)
        if not config.get('bucket'):
            config['bucket'] = _env_nonempty('S3_BUCKET')
        if not config.get('endpoint_url'):
            config['endpoint_url'] = _env_nonempty('S3_ENDPOINT_URL')
        config['region_name'] = resolve_s3_region(config.get('endpoint_url'), config.get('region_name'))
        json_response(self, {'data': S3_JOB_MANAGER.list_jobs(), 'config': config})

    def handle_s3_jobs_create(self):
        payload = self._read_json_body()
        dataset_id = (payload.get('dataset_id') or 'shein').strip().lower()
        source = (payload.get('source') or 'products').strip().lower()
        limit = parse_positive_int(payload.get('limit', 100), 100)
        concurrency = parse_positive_int(payload.get('concurrency', 4), 4, maximum=24)
        bucket = (payload.get('bucket') or S3_STATE.get('config', {}).get('bucket') or _env_nonempty('S3_BUCKET') or '').strip()
        prefix = (payload.get('prefix') or S3_STATE.get('config', {}).get('prefix') or '').strip()
        if dataset_id not in ALLOWED_DATASETS:
            raise ValueError(f'Unknown dataset: {dataset_id}')
        if not bucket:
            raise ValueError('Missing S3 bucket')
        conn = db_connect()
        rows = conn.execute('SELECT * FROM products WHERE dataset_id = ? ORDER BY id ASC', (dataset_id,)).fetchall()
        selected = [dict(row) for row in rows[:limit]]
        job_id = f'{dataset_id}-{int(time.time())}'
        load_s3_state(conn=conn, force=True)
        config = dict(S3_STATE.get('config', {}))
        config.update({k: v for k, v in payload.items() if k in {'region_name', 'endpoint_url'}})
        if not config.get('endpoint_url'):
            config['endpoint_url'] = _env_nonempty('S3_ENDPOINT_URL')
        config['region_name'] = resolve_s3_region(config.get('endpoint_url'), config.get('region_name'))
        config['bucket'] = bucket
        config['prefix'] = prefix
        for secret_key in ('aws_access_key_id', 'aws_secret_access_key', 'aws_session_token'):
            config.pop(secret_key, None)
        S3_STATE['config'] = config
        save_s3_state(conn=conn)

        def s3_client_factory():
            import boto3
            from botocore.config import Config

            env_access_key = os.getenv('AWS_ACCESS_KEY_ID') or os.getenv('AWS_ACCESS_KEY')
            env_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY') or os.getenv('AWS_SECRET_KEY')
            endpoint_url = config.get('endpoint_url') or None
            env_session_token = os.getenv('AWS_SESSION_TOKEN')
            if endpoint_url and 'r2.cloudflarestorage.com' in endpoint_url.lower():
                env_session_token = None
            session = boto3.session.Session(
                aws_access_key_id=env_access_key or None,
                aws_secret_access_key=env_secret_key or None,
                aws_session_token=env_session_token or None,
                region_name=resolve_s3_region(endpoint_url, config.get('region_name')),
            )
            client_kwargs = {
                'endpoint_url': config.get('endpoint_url') or None,
            }
            if config.get('endpoint_url'):
                client_kwargs['config'] = Config(s3={'addressing_style': 'path'})
            return session.client('s3', **client_kwargs)

        def resolve_source_url(row):
            candidates = []
            for url in [row.get('image')] + parse_json_list(row.get('image_urls_json')):
                if not isinstance(url, str):
                    continue
                cleaned = url.strip()
                if not cleaned:
                    continue
                lowered = cleaned.lower()
                if lowered.startswith(('http://', 'https://')):
                    candidates.append(cleaned)
            return candidates

        def on_uploaded(row, item):
            state_conn = db_connect()
            try:
                load_s3_state(conn=state_conn, force=True)
                goods_id = normalize_goods_id(dataset_id, row.get('id'))
                source_urls = [str(url).strip() for url in (item.get('source_urls') or []) if isinstance(url, str) and str(url).strip()]
                image_pairs = []
                for pair in item.get('image_pairs') or []:
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

        conn.close()
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

    def handle_s3_job_detail(self, job_id):
        load_s3_state(force=True)
        job = S3_JOB_MANAGER.get_job(job_id)
        if not job:
            return error_response(self, f'Job not found: {job_id}', HTTPStatus.NOT_FOUND, code='not_found')
        query = parse_qs(urlparse(self.path).query)
        page = parse_positive_int((query.get('page') or ['1'])[0], 1)
        page_size = parse_positive_int((query.get('page_size') or ['12'])[0], 12, maximum=50)
        items = list(job.get('items') or [])
        total_items = len(items)
        start = (page - 1) * page_size
        end = start + page_size
        json_response(self, {
            'data': {
                'job': job,
                'items': items[start:end],
                'page': page,
                'page_size': page_size,
                'total_items': total_items,
                'total_pages': max(1, math.ceil(total_items / page_size)) if total_items else 1,
            }
        })


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

from __future__ import annotations

import hmac
import json
import os
from http import HTTPStatus

from backend.http_utils import error_response
from backend.security import (
    CORS_HEADERS,
    build_auth_cookie,
    extract_bearer_token,
    extract_cookie_value,
    hash_password,
    issue_ephemeral_token,
    token_is_valid as ephemeral_token_is_valid,
)


API_BEARER_TOKEN = os.getenv('FAST_FASHION_API_TOKEN', '').strip()
S3_ADMIN_PASSWORD = os.getenv('FAST_FASHION_S3_ADMIN_PASSWORD', '').strip()
S3_AUTH_TOKENS: dict[str, float] = {}
S3_AUTH_TTL_SECONDS = 60 * 60


def issue_s3_token() -> str:
    return issue_ephemeral_token(S3_AUTH_TOKENS, S3_AUTH_TTL_SECONDS)


def token_is_valid(token: str | None) -> bool:
    return ephemeral_token_is_valid(S3_AUTH_TOKENS, token)


def auth_required(handler) -> bool:
    token = extract_cookie_value(handler.headers.get('Cookie', ''), 'ff_s3_auth')
    return token_is_valid(token)


def get_bearer_token(handler) -> str:
    return extract_bearer_token(handler.headers.get('Authorization', ''))


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


def s3_access_is_valid(handler) -> bool:
    return auth_required(handler)


def s3_admin_required_response(handler):
    return error_response(
        handler,
        'S3 admin authentication required',
        HTTPStatus.UNAUTHORIZED,
        code='s3_admin_auth_required',
    )


def handle_s3_auth(handler, read_json_body):
    payload = read_json_body()
    if not S3_ADMIN_PASSWORD:
        token = issue_s3_token()
        body = json.dumps({'data': {'authenticated': True, 'expires_in_seconds': S3_AUTH_TTL_SECONDS}}, ensure_ascii=False).encode('utf-8')
        handler.send_response(HTTPStatus.OK)
        handler.send_header('Content-Type', 'application/json; charset=utf-8')
        handler.send_header('Set-Cookie', build_auth_cookie(handler, name='ff_s3_auth', token=token, max_age=S3_AUTH_TTL_SECONDS))
        handler.send_header('Content-Length', str(len(body)))
        for key, value in CORS_HEADERS.items():
            handler.send_header(key, value)
        handler.end_headers()
        handler.wfile.write(body)
        return
    password = str(payload.get('password') or '')
    if not hmac.compare_digest(hash_password(password), hash_password(S3_ADMIN_PASSWORD)):
        return error_response(handler, 'Invalid S3 admin password', HTTPStatus.UNAUTHORIZED, code='unauthorized')
    token = issue_s3_token()
    body = json.dumps({'data': {'authenticated': True, 'expires_in_seconds': S3_AUTH_TTL_SECONDS}}, ensure_ascii=False).encode('utf-8')
    handler.send_response(HTTPStatus.OK)
    handler.send_header('Content-Type', 'application/json; charset=utf-8')
    handler.send_header('Set-Cookie', build_auth_cookie(handler, name='ff_s3_auth', token=token, max_age=S3_AUTH_TTL_SECONDS))
    handler.send_header('Content-Length', str(len(body)))
    for key, value in CORS_HEADERS.items():
        handler.send_header(key, value)
    handler.end_headers()
    handler.wfile.write(body)


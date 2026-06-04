from __future__ import annotations

import hashlib
import secrets
import time
from http import HTTPStatus


MAX_REQUEST_BODY_BYTES = 1_048_576

CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With',
    'Access-Control-Max-Age': '86400',
}

SECURITY_HEADERS = {
    'X-Content-Type-Options': 'nosniff',
    'Referrer-Policy': 'no-referrer',
    'X-Frame-Options': 'DENY',
    'Cross-Origin-Opener-Policy': 'same-origin',
    'Permissions-Policy': 'camera=(), geolocation=(), microphone=()',
    'Content-Security-Policy': (
        "default-src 'self'; "
        "base-uri 'self'; "
        "connect-src 'self'; "
        "frame-ancestors 'none'; "
        "img-src 'self' data: https: http:; "
        "object-src 'none'; "
        "script-src 'self'; "
        "style-src 'self' 'unsafe-inline'"
    ),
}


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


def issue_ephemeral_token(token_store: dict[str, float], ttl_seconds: int) -> str:
    token = secrets.token_hex(32)
    token_store[token] = time.time() + ttl_seconds
    return token


def token_is_valid(token_store: dict[str, float], token: str | None) -> bool:
    if not token:
        return False
    expiry = token_store.get(token)
    if not expiry:
        return False
    if expiry < time.time():
        token_store.pop(token, None)
        return False
    return True


def extract_cookie_value(cookie_header: str | None, cookie_name: str) -> str:
    raw_cookie = cookie_header or ''
    for chunk in raw_cookie.split(';'):
        chunk = chunk.strip()
        if chunk.startswith(f'{cookie_name}='):
            return chunk.split('=', 1)[1].strip()
    return ''


def extract_bearer_token(header_value: str | None) -> str:
    header = (header_value or '').strip()
    if not header:
        return ''
    scheme, _, token = header.partition(' ')
    if scheme.lower() != 'bearer':
        return ''
    return token.strip()


def secure_request(handler) -> bool:
    forwarded_proto = (handler.headers.get('X-Forwarded-Proto', '') or '').strip().lower()
    if forwarded_proto == 'https':
        return True
    return bool(getattr(handler, 'request_version', '').lower().startswith('https/'))


def build_auth_cookie(handler, *, name: str, token: str, max_age: int) -> str:
    parts = [
        f'{name}={token}',
        f'Max-Age={max_age}',
        'Path=/',
        'HttpOnly',
        'SameSite=Strict',
    ]
    if secure_request(handler):
        parts.append('Secure')
    return '; '.join(parts)


def reject_directory_listing(handler):
    handler.send_error(HTTPStatus.NOT_FOUND, 'Not found')
    return None

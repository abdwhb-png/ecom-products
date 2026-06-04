from __future__ import annotations

import json
from http import HTTPStatus

from backend.security import CORS_HEADERS


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


#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import dataset_service  # noqa: E402
import network_proxy  # noqa: E402


PROXY_KEYS = [
    'FAST_FASHION_PROXY_HOST',
    'FAST_FASHION_PROXY_PORT',
    'FAST_FASHION_PROXY_LOGIN',
    'FAST_FASHION_PROXY_PASSWORD',
]


class FakeResponse:
    def __init__(self, payload: bytes):
        self.payload = payload
        self._offset = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = len(self.payload) - self._offset
        chunk = self.payload[self._offset:self._offset + size]
        self._offset += len(chunk)
        return chunk


class FakeOpener:
    def __init__(self, calls: list[dict]):
        self.calls = calls

    def open(self, request, timeout=0):
        self.calls.append({
            'url': request.full_url,
            'timeout': timeout,
            'headers': dict(request.header_items()),
        })
        return FakeResponse(b'downloaded-via-opener')


def restore_env(previous: dict[str, str | None]) -> None:
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def set_proxy_env(enabled: bool) -> None:
    for key in PROXY_KEYS:
        os.environ.pop(key, None)
    if enabled:
        os.environ['FAST_FASHION_PROXY_HOST'] = 'gw.dataimpulse.com'
        os.environ['FAST_FASHION_PROXY_PORT'] = '823'
        os.environ['FAST_FASHION_PROXY_LOGIN'] = 'proxy-user'
        os.environ['FAST_FASHION_PROXY_PASSWORD'] = 'proxy-pass'


def main() -> int:
    previous = {key: os.environ.get(key) for key in PROXY_KEYS}
    original_urlopen = dataset_service.urllib.request.urlopen
    original_build_urllib_opener = getattr(network_proxy, 'build_urllib_opener', None)
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / 'payload.bin'
            calls: list[dict] = []

            def fail_direct_urlopen(*args, **kwargs):
                raise AssertionError('direct urllib.request.urlopen should not be used for dataset downloads')

            def fake_build_urllib_opener(*, use_proxy: bool | None = None):
                calls.append({'build': True, 'use_proxy': use_proxy})
                return FakeOpener(calls)

            dataset_service.urllib.request.urlopen = fail_direct_urlopen  # type: ignore[assignment]
            dataset_service.network_proxy = network_proxy  # type: ignore[attr-defined]
            network_proxy.build_urllib_opener = fake_build_urllib_opener  # type: ignore[attr-defined]

            set_proxy_env(False)
            dataset_service._write_response_to_file('https://example.test/direct.bin', target)
            assert target.read_bytes() == b'downloaded-via-opener'
            assert any(call.get('build') for call in calls), calls
            assert any(call.get('url') == 'https://example.test/direct.bin' for call in calls), calls

            calls.clear()
            set_proxy_env(True)
            dataset_service._write_response_to_file('https://example.test/proxied.bin', target, headers={'User-Agent': 'Mozilla/5.0', 'Authorization': 'Bearer hf'})
            assert target.read_bytes() == b'downloaded-via-opener'
            assert any(call.get('build') for call in calls), calls
            assert any(call.get('use_proxy') is True for call in calls if 'use_proxy' in call), calls
            proxied_call = next(call for call in calls if call.get('url') == 'https://example.test/proxied.bin')
            assert proxied_call['timeout'] == 120, proxied_call
            header_dump = str(proxied_call['headers'])
            assert 'Bearer hf' in header_dump, header_dump
    finally:
        dataset_service.urllib.request.urlopen = original_urlopen  # type: ignore[assignment]
        if original_build_urllib_opener is None:
            try:
                delattr(network_proxy, 'build_urllib_opener')
            except Exception:
                pass
        else:
            network_proxy.build_urllib_opener = original_build_urllib_opener  # type: ignore[attr-defined]
        restore_env(previous)

    print('OK: dataset downloads use the shared proxy-aware opener instead of direct urlopen')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

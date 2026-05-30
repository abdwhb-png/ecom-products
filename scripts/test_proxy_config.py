#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import network_proxy  # noqa: E402


ENV_KEYS = [
    'FAST_FASHION_PROXY_HOST',
    'FAST_FASHION_PROXY_PORT',
    'FAST_FASHION_PROXY_LOGIN',
    'FAST_FASHION_PROXY_PASSWORD',
    'FAST_FASHION_ASOS_PROXY_MAX_CONCURRENCY',
]


def restore_env(previous: dict[str, str | None]) -> None:
    for key, value in previous.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def set_env(values: dict[str, str]) -> None:
    for key in ENV_KEYS:
        os.environ.pop(key, None)
    for key, value in values.items():
        os.environ[key] = value


def main() -> int:
    previous = {key: os.environ.get(key) for key in ENV_KEYS}
    try:
        set_env({})
        assert network_proxy.resolve_proxy_config() is None
        state = network_proxy.public_proxy_state()
        assert state['proxy_enabled'] is False, state
        assert state['egress_proxy_mode'] == 'direct', state
        assert state['asos_max_concurrency'] == 2, state

        set_env({'FAST_FASHION_PROXY_HOST': 'proxy.example.test'})
        try:
            network_proxy.resolve_proxy_config()
        except RuntimeError as exc:
            message = str(exc)
            assert 'FAST_FASHION_PROXY_PORT' in message, message
            assert 'FAST_FASHION_PROXY_LOGIN' in message, message
            assert 'FAST_FASHION_PROXY_PASSWORD' in message, message
            assert 'proxy.example.test' not in message, message
        else:
            raise AssertionError('Expected incomplete proxy config to fail')

        set_env({
            'FAST_FASHION_PROXY_HOST': 'proxy.example.test',
            'FAST_FASHION_PROXY_PORT': '823',
            'FAST_FASHION_PROXY_LOGIN': 'user@example',
            'FAST_FASHION_PROXY_PASSWORD': 'p@ss word',
        })
        config = network_proxy.resolve_proxy_config()
        assert config is not None
        assert config.host == 'proxy.example.test', config
        assert config.port == 823, config
        assert config.login == 'user@example', config
        assert config.password == 'p@ss word', config
        proxy_url = network_proxy.build_proxy_url(config)
        assert proxy_url.startswith('http://'), proxy_url
        assert 'user%40example' in proxy_url, proxy_url
        assert 'p%40ss%20word' in proxy_url, proxy_url
        state = network_proxy.public_proxy_state()
        assert state['proxy_enabled'] is True, state
        assert state['egress_proxy_mode'] == 'proxy', state
        assert state['asos_max_concurrency'] == 8, state

        os.environ['FAST_FASHION_ASOS_PROXY_MAX_CONCURRENCY'] = '12'
        state = network_proxy.public_proxy_state()
        assert state['asos_max_concurrency'] == 12, state

        serialized = str(state)
        assert 'proxy.example.test' not in serialized, serialized
        assert 'user@example' not in serialized, serialized
        assert 'p@ss word' not in serialized, serialized
    finally:
        restore_env(previous)

    print('OK: proxy config resolves safely and rejects partial configuration')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

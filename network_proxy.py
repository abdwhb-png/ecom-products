from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote
from urllib.request import ProxyHandler, build_opener


PROXY_ENV_KEYS = (
    'FAST_FASHION_PROXY_HOST',
    'FAST_FASHION_PROXY_PORT',
    'FAST_FASHION_PROXY_LOGIN',
    'FAST_FASHION_PROXY_PASSWORD',
)


@dataclass(frozen=True)
class ProxyConfig:
    host: str
    port: int
    login: str
    password: str


def _read_env(key: str) -> str:
    return os.getenv(key, '').strip()


def _present_env_values() -> dict[str, str]:
    return {key: value for key in PROXY_ENV_KEYS if (value := _read_env(key))}


def _missing_keys(values: dict[str, str]) -> list[str]:
    if not values:
        return []
    return [key for key in PROXY_ENV_KEYS if key not in values]


def validate_proxy_config() -> ProxyConfig | None:
    values = _present_env_values()
    if not values:
        return None

    missing = _missing_keys(values)
    if missing:
        raise RuntimeError(f'Incomplete proxy configuration. Missing required env: {", ".join(missing)}')

    try:
        port = int(values['FAST_FASHION_PROXY_PORT'])
    except Exception as exc:
        raise RuntimeError('Invalid proxy configuration. FAST_FASHION_PROXY_PORT must be an integer.') from exc

    if port < 1 or port > 65535:
        raise RuntimeError('Invalid proxy configuration. FAST_FASHION_PROXY_PORT must be between 1 and 65535.')

    return ProxyConfig(
        host=values['FAST_FASHION_PROXY_HOST'],
        port=port,
        login=values['FAST_FASHION_PROXY_LOGIN'],
        password=values['FAST_FASHION_PROXY_PASSWORD'],
    )


def resolve_proxy_config() -> ProxyConfig | None:
    return validate_proxy_config()


def proxy_enabled() -> bool:
    return resolve_proxy_config() is not None


def build_proxy_url(config: ProxyConfig | None = None) -> str | None:
    proxy = config or resolve_proxy_config()
    if proxy is None:
        return None
    login = quote(proxy.login, safe='')
    password = quote(proxy.password, safe='')
    return f'http://{login}:{password}@{proxy.host}:{proxy.port}'


def public_proxy_state() -> dict[str, Any]:
    enabled = proxy_enabled()
    return {
        'proxy_enabled': enabled,
        'egress_proxy_mode': 'proxy' if enabled else 'direct',
    }


def build_urllib_proxy_handler(config: ProxyConfig | None = None) -> ProxyHandler:
    proxy_url = build_proxy_url(config)
    if not proxy_url:
        return ProxyHandler({})
    return ProxyHandler({'http': proxy_url, 'https': proxy_url})


def build_urllib_opener(*, use_proxy: bool | None = None):
    enabled = proxy_enabled() if use_proxy is None else bool(use_proxy)
    if not enabled:
        return build_opener(ProxyHandler({}))
    return build_opener(build_urllib_proxy_handler())

#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
S3_HTML = REPO / 's3.html'
S3_JS = REPO / 's3.js'


def main() -> int:
    html = S3_HTML.read_text(encoding='utf-8')
    js = S3_JS.read_text(encoding='utf-8')

    required_html = [
        'id="storedApiTokenState"',
        'id="useStoredTokenBtn"',
        'id="resetStoredTokenBtn"',
    ]
    required_js = [
        'storedApiTokenLabel: document.getElementById(\'storedApiTokenState\')',
        'useStoredTokenBtn: document.getElementById(\'useStoredTokenBtn\')',
        'resetStoredTokenBtn: document.getElementById(\'resetStoredTokenBtn\')',
        'function maskToken(value) {',
        'function refreshStoredTokenUi() {',
        "els.storedApiTokenLabel.textContent = `Token stocké détecté (${maskToken(state.apiToken)})`;",
        "els.useStoredTokenBtn?.addEventListener('click', () => {",
        "els.resetStoredTokenBtn?.addEventListener('click', () => {",
        'window.localStorage.removeItem(API_TOKEN_STORAGE_KEY);',
    ]

    missing_html = [snippet for snippet in required_html if snippet not in html]
    missing_js = [snippet for snippet in required_js if snippet not in js]
    assert not missing_html, f'Missing HTML snippets: {missing_html}'
    assert not missing_js, f'Missing JS snippets: {missing_js}'

    print('OK: /s3 auth gate exposes stored-token status plus use/reset controls')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

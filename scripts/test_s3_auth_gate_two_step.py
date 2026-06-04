#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
PUBLIC = REPO / 'public'
S3_HTML = PUBLIC / 's3.html'
S3_JS = PUBLIC / 's3.js'


def main() -> int:
    html = S3_HTML.read_text(encoding='utf-8')
    js = S3_JS.read_text(encoding='utf-8')

    required_html = [
        'id="apiTokenInput"',
        'id="unlockApiBtn"',
        'id="apiTokenHint"',
        'id="s3AdminPasswordGroup"',
    ]
    required_js = [
        "if (els.apiTokenInput) {\n    els.apiTokenInput.value = state.apiToken;\n  }",
        "async function unlockApiTokenForS3() {",
        "state.apiToken = window.FastFashionAuth.writeToken(candidate);",
        "els.authStateLabel.textContent = 'API OK · S3 verrouillé';",
        "els.apiTokenHint.textContent = 'Token API valide. Tu peux maintenant entrer le mot de passe admin S3.';",
        "els.s3AdminPasswordGroup?.classList.remove('hidden');",
        "if (response.status === 401 && payload?.error?.code === 'unauthorized' && !state.apiToken) {",
    ]

    missing_html = [snippet for snippet in required_html if snippet not in html]
    missing_js = [snippet for snippet in required_js if snippet not in js]
    assert not missing_html, f'Missing HTML snippets: {missing_html}'
    assert not missing_js, f'Missing JS snippets: {missing_js}'

    print('OK: /s3 auth gate supports API token first, then S3 admin password')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

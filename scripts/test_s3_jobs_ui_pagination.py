#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
PUBLIC = REPO / 'public'
S3_JS = PUBLIC / 's3.js'
S3_HTML = PUBLIC / 's3.html'


def main() -> int:
    js = S3_JS.read_text(encoding='utf-8')
    html = S3_HTML.read_text(encoding='utf-8')
    required_js = [
        'const JOB_HISTORY_PAGE_SIZE = 20;',
        'familyJobPagination:',
        "fetch(`${config.listEndpoint}?page=${currentPage}&pageSize=${historyPageSize}`",
        'function getFamilyPaginationEls(family) {',
        'function goToFamilyJobsPage(family, nextPage) {',
        'uploadJobsPrevBtn',
        'migrationJobsPrevBtn',
        'cleanupJobsPrevBtn',
    ]
    required_html = [
        'id="uploadJobsPrevBtn"',
        'id="uploadJobsNextBtn"',
        'id="uploadJobsPageLabel"',
        'id="migrationJobsPrevBtn"',
        'id="migrationJobsNextBtn"',
        'id="migrationJobsPageLabel"',
        'id="cleanupJobsPrevBtn"',
        'id="cleanupJobsNextBtn"',
        'id="cleanupJobsPageLabel"',
    ]
    missing_js = [snippet for snippet in required_js if snippet not in js]
    missing_html = [snippet for snippet in required_html if snippet not in html]
    assert not missing_js, missing_js
    assert not missing_html, missing_html
    print('OK: S3 admin UI wires paginated job history controls at 20 jobs per page')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

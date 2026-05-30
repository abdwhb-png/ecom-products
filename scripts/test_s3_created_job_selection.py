#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
S3_JS = REPO / 's3.js'


def main() -> int:
    text = S3_JS.read_text(encoding='utf-8')
    required = [
        "const createdJobId = createdJob?.job_id || null;",
        "state.pendingJobIdByFamily[family] = createdJobId;",
        "state.selectedJobId = createdJobId;",
        "state.selectedJobDetail = null;",
        "state.detailPage = 1;",
        "state.activeFamily = family;",
        "openModalShell();",
        "renderJobDetailsLoading(createdJobId, 1, createdJob);",
        "await openJobDetails(createdJobId, 1, { job: createdJob });",
    ]
    missing = [snippet for snippet in required if snippet not in text]
    assert not missing, missing
    print('OK: created upload job stays selected and opens its own detail modal immediately')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

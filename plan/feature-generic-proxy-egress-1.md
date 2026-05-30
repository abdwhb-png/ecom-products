---
goal: Add a generic outbound proxy layer for production web fetches, with ASOS-specific upload resilience, dynamic concurrency caps, shorter timeouts, controlled retries, and safe observability
version: 1.0
date_created: 2026-05-30
last_updated: 2026-05-30
owner: Fast Fashion Dashboard agent
status: 'Planned'
tags: [feature, proxy, egress, asos, s3, uploads, backend, frontend, observability]
---

# Introduction

![Status: Planned](https://img.shields.io/badge/status-Planned-blue)

This plan introduces a generic authenticated outbound proxy as a shared egress layer for production HTTP(S) fetches initiated by the application, without hard-coding assumptions about proxy class such as residential or datacenter/SEM. The current codebase performs external web requests in at least two runtime paths: dataset downloads in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/dataset_service.py` and source-image downloads for S3 upload jobs in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py`. The current upload implementation already clamps ASOS upload concurrency to `2` in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_job_operations.py` lines 185-191 and uses browser-like headers plus long ASOS timeouts `(25, 45, 60)` in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` lines 548-585. The goal of this plan is to add a reusable proxy configuration based on separate environment fields (`host`, `port`, `login`, `password`), apply it across outbound third-party fetches made by the app, keep ASOS direct uploads capped at `2`, allow ASOS uploads up to `8` only when the proxy is configured, reduce ASOS timeouts, add controlled retry/backoff behavior for `403` and timeout-class failures, and expose only safe proxy-state/diagnostic information in the admin UI and docs.

## 1. Requirements & Constraints

- **REQ-001**: Introduce a shared authenticated proxy configuration for outbound third-party HTTP(S) fetches initiated by the application runtime; do not model the proxy as an image-only feature and do not couple the design to a specific proxy family such as residential.
- **REQ-002**: The proxy configuration must be environment-authoritative and must use four explicit environment variables: `FAST_FASHION_PROXY_HOST`, `FAST_FASHION_PROXY_PORT`, `FAST_FASHION_PROXY_LOGIN`, and `FAST_FASHION_PROXY_PASSWORD`.
- **REQ-003**: Proxy enablement must be derived from the four dedicated fields above. If any one of the four fields is set, all four must be present and valid; partial configuration must fail deterministically with a clear configuration error instead of silently falling back.
- **REQ-004**: The implementation must assemble the proxy credentials internally from the four dedicated fields rather than requiring a single prebuilt proxy URL environment variable.
- **REQ-005**: The proxy must be reusable by all applicable outbound web-fetch paths in the repository, including dataset downloads in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/dataset_service.py` and source-image downloads in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py`.
- **REQ-006**: The proxy must be treated as an application egress mechanism for third-party web fetch traffic so production requests do not expose the server IP directly to those remote sites.
- **REQ-007**: The implementation must not require a system-wide OS proxy or shell-global `HTTP_PROXY`/`HTTPS_PROXY` variables; the application must control proxy usage explicitly through shared code.
- **REQ-008**: Preserve the current ASOS default concurrency policy: when the proxy is not configured, ASOS upload jobs must remain capped at `2` concurrent workers server-side.
- **REQ-009**: When the proxy is configured, ASOS upload jobs must allow up to `8` concurrent workers server-side.
- **REQ-010**: Non-ASOS upload jobs must keep the existing generic upper bound of `24` concurrent workers unless future requirements explicitly change that behavior.
- **REQ-011**: The backend must remain authoritative for concurrency caps. The frontend may guide or clamp input values, but the backend must always enforce the effective maximum.
- **REQ-012**: Replace the current ASOS timeout plan `(25, 45, 60)` in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` with the exact shorter plan `(10, 20, 30)` seconds.
- **REQ-013**: Add controlled retry/backoff handling for ASOS source-image downloads. Retries must use the exact backoff sequence `(1, 3)` seconds between attempts and must stop after the timeout plan is exhausted.
- **REQ-014**: Treat `HTTPError` with status `403`, `TimeoutError`, `socket.timeout`, and transient `URLError` network failures as retry-eligible for ASOS source-image downloads.
- **REQ-015**: Preserve existing browser-like request headers and ASOS `Referer: https://www.asos.com/` behavior while routing the request through the proxy when the proxy is enabled.
- **REQ-016**: Persist observable download diagnostics for upload jobs. Each upload job must expose compact aggregate metrics at minimum by `hostname`, `result_status`, and `proxy_mode` so operators can distinguish success, timeout, and `403` patterns.
- **REQ-017**: Final failed item payloads for upload jobs must include richer failure metadata for the last attempt: `hostname`, `attempt_count`, `proxy_used`, `timeout_seconds`, and normalized error class/status when available.
- **REQ-018**: The protected `/s3` upload UI must show the effective proxy state clearly and must communicate the ASOS concurrency policy as `max 2` without proxy and `max 8` with proxy.
- **REQ-019**: The protected API surface and static docs must be updated in the same change: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/server.py`, `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/README.md`, and `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/docs.html`.
- **REQ-020**: The existing S3 family architecture from `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/plan/refactor-s3-job-workflows-1.md` must remain intact. This work must extend the existing `upload` family rather than create a parallel upload system.
- **SEC-001**: Never expose the raw values of `FAST_FASHION_PROXY_HOST`, `FAST_FASHION_PROXY_PORT`, `FAST_FASHION_PROXY_LOGIN`, or `FAST_FASHION_PROXY_PASSWORD` in API responses, job payloads, UI-rendered logs, memory files, or persisted job history.
- **SEC-002**: Only safe derived proxy state may be exposed to the frontend/API, for example `proxy_enabled=true` and `proxy_mode=authenticated`.
- **SEC-003**: Proxy credentials must remain server-side only and must never be interpolated into exception messages returned to the browser.
- **SEC-004**: Proxy support must not weaken the current Bearer-token and S3-admin-cookie protection of the `/api/s3/*` admin routes.
- **CON-001**: No database refresh/reset command may be introduced or executed as part of this feature.
- **CON-002**: The implementation must preserve the current JSON-backed `s3_jobs` history model and restart recovery behavior in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py`.
- **CON-003**: The implementation must remain compatible with the current upload dry-run lifecycle; proxy-aware behavior must work for both `dry_run=true` and `dry_run=false` jobs.
- **CON-004**: The implementation must preserve direct S3 API behavior for boto3-based uploads unless a later specification explicitly decides to proxy S3-compatible API traffic too.
- **GUD-001**: Proxy behavior must be centralized in one shared helper module instead of being duplicated in `dataset_service.py` and `s3_jobs.py`.
- **GUD-002**: Retry/backoff rules must be host-aware and deterministic; avoid unbounded loops, random jitter, or opaque “try until it works” behavior.
- **GUD-003**: Observability data must be compact enough to keep job-history payload sizes manageable on large ASOS runs.
- **PAT-001**: Introduce a shared helper module such as `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/network_proxy.py` that owns proxy resolution, validation, safe public state exposure, and urllib opener construction.
- **PAT-002**: Runtime download policy should be represented explicitly, for example via helpers that return `hostname`, `referer`, `timeout_plan`, `backoff_plan`, `proxy_enabled`, and `max_concurrency` for a given dataset/URL.
- **PAT-003**: The `/s3` frontend must derive upload concurrency guidance from backend-exposed effective policy rather than duplicating hard-coded assumptions in JavaScript only.

## 2. Implementation Steps

### Implementation Phase 1

- **GOAL-001**: Introduce one shared proxy configuration layer so all relevant outbound HTTP(S) fetch code can opt into the same production egress identity without exposing secrets or duplicating env parsing. Completion criteria: the backend can validate the four proxy env fields, derive safe public state, and build a reusable urllib proxy opener from one shared module.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-001 | Create `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/network_proxy.py` with a deterministic `ProxyConfig` model and helpers such as `resolve_proxy_config()`, `validate_proxy_config()`, `proxy_enabled()`, `public_proxy_state()`, and `build_urllib_proxy_handler()`. The canonical env inputs must be exactly `FAST_FASHION_PROXY_HOST`, `FAST_FASHION_PROXY_PORT`, `FAST_FASHION_PROXY_LOGIN`, and `FAST_FASHION_PROXY_PASSWORD`. |  |  |
| TASK-002 | In `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/network_proxy.py`, define exact validation rules: `port` must parse as an integer in the valid TCP range, `host` must be non-empty, `login` and `password` must be non-empty, and any partial configuration must raise a clear configuration error identifying which required fields are missing without echoing secret values. |  |  |
| TASK-003 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/server.py` near the existing `resolve_aws_*` helpers so the app can expose only safe derived proxy state such as `proxy_enabled` and `egress_proxy_mode='authenticated'|'direct'` in admin/config responses. Do not expose raw host, port, login, or password. |  |  |
| TASK-004 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/.env.example` to document the four proxy env variables and to explain that they control application egress for third-party web fetches rather than only source-image downloads. |  |  |
| TASK-005 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_env_authoritative.py` and/or add a dedicated config test so it verifies that proxy state is env-authoritative, partial configuration is rejected deterministically, and no API/request body can override the configured proxy state. |  |  |

### Implementation Phase 2

- **GOAL-002**: Apply the proxy to shared outbound web-fetch paths in the repository so the server’s production IP is hidden from third-party sites in the code paths that currently use direct urllib requests. Completion criteria: dataset downloads and upload source-image downloads both use the shared proxy helper for applicable outbound HTTP(S) traffic, while local/internal and boto3 S3 API traffic remain unchanged.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-006 | Refactor `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/dataset_service.py` `_write_response_to_file()` to build its urllib opener through the new shared proxy helper so direct downloads, Hugging Face downloads, and Kaggle archive downloads use the proxy when configured. Preserve existing headers and timeout behavior unless the shared helper requires explicit opener injection. |  |  |
| TASK-007 | Refactor `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` `S3JobManager._download()` so it also uses the shared proxy helper rather than building a direct `urlopen()` path inline. Keep browser-like headers and ASOS referer behavior intact. |  |  |
| TASK-008 | Add targeted automated coverage for `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/dataset_service.py` so proxy-enabled and direct-mode downloads can be asserted without making real network calls. Verify that the proxy is used by direct, Hugging Face, and Kaggle provider paths. |  |  |
| TASK-009 | Ensure the proxy helper is reusable by future third-party urllib-based fetch paths in the repo and document the required integration pattern in code comments or module docstrings so new fetchers do not bypass the proxy layer accidentally. |  |  |

### Implementation Phase 3

- **GOAL-003**: Replace the current ASOS-specific download implementation with a policy-driven strategy that combines proxy awareness, shorter timeouts, and controlled retry/backoff semantics. Completion criteria: ASOS downloads use the exact timeout plan `(10, 20, 30)`, the exact backoff sequence `(1, 3)`, direct mode keeps concurrency at `2`, proxied mode allows `4`, and final failures retain normalized metadata suitable for diagnosis.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-010 | Refactor `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_job_operations.py` `collect_upload_candidates()` so ASOS upload concurrency is computed from one explicit policy function: `2` when proxy is disabled, `8` when proxy is enabled, and current generic max `24` for other datasets. Persist the actual effective concurrency in the created job payload. |  |  |
| TASK-011 | Refactor `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` `S3JobManager._download()` into smaller helpers such as `_download_policy_for_url()`, `_build_request_headers()`, and `_download_with_policy()` so host-specific timeouts, proxy routing, and retry/backoff logic are explicit and testable. |  |  |
| TASK-012 | Replace the current ASOS timeout plan `(25, 45, 60)` in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` with the exact ASOS plan `(10, 20, 30)` and preserve existing non-ASOS timeout/header behavior unless an explicit host policy overrides it. |  |  |
| TASK-013 | Implement exact controlled retry semantics for ASOS image downloads in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py`: up to three total attempts, retries only for normalized retry-eligible errors (`HTTP 403`, timeout-class failures, transient `URLError` network failures), and exact sleeps of `1` then `3` seconds between retry attempts. |  |  |
| TASK-014 | Enrich upload item failure payloads in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` `_process_row()` so the final failure record includes `hostname`, `attempt_count`, `proxy_used`, `timeout_seconds`, `error_type`, and `http_status` when available. Keep item payloads compact and do not persist raw proxy settings. |  |  |
| TASK-015 | Preserve dry-run semantics in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py`: preview jobs must evaluate candidate handling and policy metadata without performing `put_object`, SQLite writes, or unnecessary external waits. |  |  |

### Implementation Phase 4

- **GOAL-004**: Add compact observability for proxy-backed ASOS downloads and surface effective egress policy/state in the protected admin UI so operators can see whether jobs ran direct or via proxy and whether failures were caused by `403`, timeout, or other network classes. Completion criteria: upload jobs expose aggregate download metrics, the job detail modal can show them, and the upload tab communicates the active proxy state before job creation.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-016 | Extend `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` `S3JobState` with a compact aggregate field such as `download_stats` that stores counts grouped by `hostname`, `result_status`, and `proxy_mode`. Update serialization, persistence, and load paths so the field survives restart/history inspection. |  |  |
| TASK-017 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` bookkeeping paths (`_record_item_unlocked()`, `_run_job()`, and related helpers) so each processed upload item increments aggregate metrics for outcomes such as `success`, `timeout`, `http_403`, `http_other`, and `network_error`, split by hostname and proxy mode. |  |  |
| TASK-018 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/server.py` OpenAPI schemas for `S3JobState`, `S3JobItem`, config responses, and upload-family responses so the new safe proxy-state fields and `download_stats` structure are documented and machine-readable. |  |  |
| TASK-019 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.html` upload panel copy so it explicitly states the proxy policy: direct mode caps ASOS concurrency at `2`, proxy mode allows up to `8`, and ASOS downloads use shorter retry-controlled timeouts. |  |  |
| TASK-020 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.js` upload-family configuration and form handling so the UI reads `proxy_enabled` and `asos_max_concurrency` from backend responses, clamps the ASOS concurrency input accordingly, and renders a user-visible hint showing whether jobs will egress directly or through the proxy. Keep the backend authoritative even if the UI is bypassed. |  |  |
| TASK-021 | Extend `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.js` job-detail rendering so upload jobs display compact `download_stats` sections grouped by hostname and failure class, making `images.asos-media.com` success/timeout/403 patterns visible alongside `proxy_mode` without exposing proxy secrets. |  |  |

### Implementation Phase 5

- **GOAL-005**: Align documentation, automated tests, and operator verification with the new proxy-backed egress model so the feature is safe to ship and diagnosable in production. Completion criteria: docs describe the four-env proxy configuration and ASOS concurrency rules, automated tests cover config validation and retry behavior, and browser-level verification confirms the admin UI messaging and effective cap handling.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-022 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/README.md` S3/admin and runtime-configuration sections to document the four proxy env variables, the scope of proxied egress, the exact ASOS concurrency rules (`2` direct / `8` proxied), the exact ASOS timeout plan `(10, 20, 30)`, and the presence of per-host/per-status diagnostics in upload job details. |  |  |
| TASK-023 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/docs.html` so the served static documentation reflects the same proxy configuration model, safe config exposure, and ASOS-specific concurrency/timeout behavior described in `README.md`. |  |  |
| TASK-024 | Add a new automated test file such as `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_proxy_config.py` that verifies exact env parsing, partial-config failure behavior, safe public-state exposure, and absence of raw proxy secrets in returned payloads. |  |  |
| TASK-025 | Add a new automated test file such as `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_dataset_service_proxy_usage.py` that mocks urllib behavior and verifies dataset downloads route through the proxy when configured and direct mode when not configured. |  |  |
| TASK-026 | Add a new automated test file such as `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_asos_retry_metrics.py` that simulates `HTTP 403`, timeout-class failures, and final success/failure paths, then asserts exact retry counts, backoff sequencing, normalized failure metadata, aggregate `download_stats` counters, and correct `proxy_mode` reporting. |  |  |
| TASK-027 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_upload_job_dry_run.py` so dry-run coverage confirms the new safe proxy state/policy metadata is present while still performing zero S3 writes and zero SQLite persistence side effects. |  |  |
| TASK-028 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_openapi_richness.py` so `/openapi.json` is required to expose the safe proxy-state fields and the upload job `download_stats` schema. |  |  |
| TASK-029 | Perform browser-level validation on `/s3`: unlock the page, switch the dataset between `shein` and `asos`, confirm the ASOS concurrency hint reflects `2` when proxy is absent and `4` when proxy is enabled, start one preview upload, and verify the job detail modal renders aggregate host/status diagnostics plus safe proxy-mode information without exposing proxy secrets. |  |  |

## 3. Alternatives

- **ALT-001**: Keep proxy support limited to source-image downloads only. Rejected because the clarified requirement is to hide the server’s production IP more broadly for outbound third-party fetches, not only for image URLs.
- **ALT-002**: Use process-wide `HTTP_PROXY` / `HTTPS_PROXY` environment variables and let Python apply them implicitly to all outbound traffic. Rejected because the user wants explicit dedicated proxy env fields and the application should control proxy usage deterministically in code.
- **ALT-003**: Store proxy settings in the protected `/s3` UI or SQLite config so operators can edit them from the browser. Rejected because the configuration contains secrets and the project already treats sensitive runtime config as environment-authoritative.
- **ALT-004**: Keep ASOS concurrency fixed at `2` even when a proxy is configured. Rejected because the requested behavior is to unlock up to `8` concurrent ASOS workers when the proxy is available.
- **ALT-005**: Route boto3 S3-compatible API traffic through the proxy by default. Rejected for this plan because the clarified requirement is about masking server IP to third-party web targets, while direct S3 API traffic is a separate decision and could introduce avoidable complexity/performance risk.

## 4. Dependencies

- **DEP-001**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/dataset_service.py` existing outbound download paths for direct URLs, Hugging Face, and Kaggle.
- **DEP-002**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` existing upload runner, source-image downloader, per-item accounting, persistence, and restart handling.
- **DEP-003**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_job_operations.py` existing upload-family request parsing and concurrency clamping behavior.
- **DEP-004**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/server.py` existing env-authoritative config exposure, OpenAPI generation, and protected S3 route handlers.
- **DEP-005**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.html` and `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.js` existing upload admin workflow and job detail modal.
- **DEP-006**: Python stdlib `urllib.request` facilities already used by the current downloader code and suitable for proxy-handler construction.
- **DEP-007**: Existing dry-run upload coverage in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_upload_job_dry_run.py`.
- **DEP-008**: Existing env-authoritative configuration coverage in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_env_authoritative.py`.

## 5. Files

- **FILE-001**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/network_proxy.py` — new shared proxy resolution/validation/opener helper.
- **FILE-002**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/dataset_service.py` — route direct/Hugging Face/Kaggle downloads through the shared proxy helper.
- **FILE-003**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` — refactor ASOS download policy, proxy-aware opener usage, retry/backoff logic, final failure metadata, and aggregate download stats.
- **FILE-004**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_job_operations.py` — compute policy-driven ASOS effective concurrency (`2` direct / `4` proxied) during upload candidate collection.
- **FILE-005**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/server.py` — resolve safe proxy state, expose it through protected responses, and document it in OpenAPI.
- **FILE-006**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.js` — clamp the ASOS concurrency input using backend-provided policy metadata and render aggregate upload diagnostics plus proxy-mode state in job details.
- **FILE-007**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.html` — update upload copy/hints to explain the proxy-backed ASOS policy.
- **FILE-008**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/.env.example` — document the four proxy env variables.
- **FILE-009**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/README.md` — document proxy configuration, ASOS concurrency rules, timeouts, and diagnostics.
- **FILE-010**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/docs.html` — align static docs with the new egress/proxy policy.
- **FILE-011**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_proxy_config.py` — new proxy-config validation coverage.
- **FILE-012**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_dataset_service_proxy_usage.py` — new dataset-download proxy usage coverage.
- **FILE-013**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_asos_retry_metrics.py` — retry/metrics coverage for proxied/direct ASOS upload downloads.
- **FILE-014**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_upload_job_dry_run.py` — preserve dry-run guarantees while asserting safe proxy-state presence.
- **FILE-015**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_openapi_richness.py` — require the new documented safe fields.

## 6. Testing

- **TEST-001**: Run `python3 /home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_proxy_config.py` and verify exact env parsing, partial-config failure behavior, and absence of raw proxy secrets in public payloads.
- **TEST-002**: Run `python3 /home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_dataset_service_proxy_usage.py` and verify direct, Hugging Face, and Kaggle downloads use the proxy when configured and direct mode when not configured.
- **TEST-003**: Run `python3 /home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_env_authoritative.py` and verify proxy state is env-authoritative, ASOS effective concurrency is `2` without proxy and `4` with proxy, and no request payload can override those rules.
- **TEST-004**: Run `python3 /home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_asos_retry_metrics.py` and verify exact ASOS timeout plan `(10, 20, 30)`, exact backoff sequence `(1, 3)`, retry-eligible error handling, aggregate `download_stats`, and safe `proxy_mode` reporting.
- **TEST-005**: Run `python3 /home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_upload_job_dry_run.py` and verify preview uploads still perform zero S3 writes and zero SQLite writes while exposing safe effective policy metadata.
- **TEST-006**: Run `python3 /home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_openapi_richness.py` and verify `/openapi.json` includes the safe proxy-state fields and `download_stats` schema additions.
- **TEST-007**: Perform browser-level validation on `/s3` against a running local server in both direct mode and proxy-configured mode, confirming the ASOS concurrency hint changes from `2` to `4` and that upload job details display host/status diagnostics without secret leakage.

## 7. Risks & Assumptions

- **RISK-001**: If any runtime fetch path continues to call raw `urllib.request.urlopen()` directly, some third-party traffic will still leak the server IP instead of using the proxy.
- **RISK-002**: Proxy providers can still return degraded performance, `403`, or timeouts, so proxy support improves resilience and IP masking but does not guarantee universal success.
- **RISK-003**: If raw exception messages are surfaced without normalization, proxy credentials could leak into job details or API responses.
- **RISK-004**: Persisting too much per-attempt detail for large ASOS runs could bloat `s3_jobs` history payloads and slow the `/s3` job detail modal. The implementation must prefer aggregate counters and compact final-failure metadata.
- **RISK-005**: Shortening ASOS timeouts too aggressively could increase false failures on slower networks, so the exact `(10, 20, 30)` plan must be validated with mocked tests and one admin dry run before rollout.
- **ASSUMPTION-001**: The target proxy is compatible with Python stdlib `urllib.request.ProxyHandler` and can be represented using host/port/login/password with a conventional HTTP proxy scheme.
- **ASSUMPTION-002**: The practical meaning of “hide the production server IP” in this codebase is to proxy application-initiated third-party web fetches, not to modify OS-level routing for every process on the machine.
- **ASSUMPTION-003**: Direct boto3 S3 API traffic can remain unproxied for this feature without violating the user’s clarified goal, because the main IP-sensitive targets are third-party source/dataset hosts rather than the configured S3 endpoint.
- **ASSUMPTION-004**: ASOS remains the only dataset requiring the special concurrency rule in this implementation. Other hosts may reuse the new policy framework later without changing this plan’s acceptance criteria.

## 8. Related Specifications / Further Reading

- `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/plan/refactor-s3-job-workflows-1.md`
- `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/dataset_service.py`
- `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_job_operations.py`
- `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py`
- `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/server.py`
- `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.html`
- `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.js`
- `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/README.md`
- `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/docs.html`
- `memory/2026-04-12.md`

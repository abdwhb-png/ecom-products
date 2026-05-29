---
goal: Refactor protected S3 admin jobs into separated upload, URL migration, and stale-state cleanup workflows with one shared dry-run lifecycle
version: 1.0
date_created: 2026-05-29
last_updated: 2026-05-29
owner: Fast Fashion Dashboard agent
status: 'Planned'
tags: [refactor, s3, jobs, frontend, backend, api, admin]
---

# Introduction

![Status: Planned](https://img.shields.io/badge/status-Planned-blue)

This plan refactors the protected `/s3` admin area so S3 upload jobs, URL migration jobs, and stale-state cleanup jobs are fully separated in backend routes and frontend tabs while sharing one deterministic lifecycle: preview as dry run, launch as write mode, stop as cancellation, inspect details, and review history. The current implementation mixes cleanup into the migration tab in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.html` lines 124-156, infers job families from `kind` string prefixes in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.js` functions `renderJobCard()` and `renderS3Jobs()` (current definitions near lines 316-359), and duplicates backend job creation logic across `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/server.py` handlers `handle_s3_jobs_create()`, `handle_s3_migration_job_create()`, and `handle_s3_cleanup_job_create()` (current definitions near lines 2442-2771).

## 1. Requirements & Constraints

- **REQ-001**: The protected S3 admin domain must expose exactly three canonical job families: `upload`, `url_migration`, and `state_cleanup`.
- **REQ-002**: Every canonical job family must support the same user-visible workflow in the UI and API: `preview` (dry run only), `start` (write mode), `stop` (cancel active job), `detail`, and `history`.
- **REQ-003**: Preview operations must create persisted job-history entries with `dry_run=true` and must not write to S3, SQLite `s3_objects`, or backup files.
- **REQ-004**: Job history cards and job detail views must render a `dry-run` badge whenever `dry_run=true` and must not render an `upload` family badge once jobs are separated by tab.
- **REQ-005**: The `/s3` page must expose three dedicated top-level tabs and three dedicated history lists: one for uploads, one for URL migrations, and one for stale-state cleanup.
- **REQ-006**: The frontend must stop inferring job family from `kind` string prefixes such as `migration*`; it must read an explicit canonical backend field instead.
- **REQ-007**: The backend must expose family-separated list/create routes so the frontend never needs to fetch a mixed global history list and then reclassify jobs client-side.
- **REQ-008**: Existing persisted jobs stored through `s3_jobs.py` and the SQLite `s3_jobs` table must remain readable; legacy `kind` values (`upload`, `migration`, `migration_preview`, `cleanup`, `cleanup_preview`) must be normalized into the new canonical fields during load.
- **REQ-009**: Upload jobs must gain a real preview mode that evaluates candidate products and expected outcomes without performing `put_object`, `UPDATE s3_objects`, or other writes.
- **REQ-010**: Frontend and backend code for job lifecycle handling must be centralized so one registry/configuration source defines family labels, route paths, dry-run semantics, and metric labels.
- **REQ-011**: Tab-level stop actions must cancel only active jobs belonging to the current family; they must not cancel unrelated active jobs from other tabs.
- **REQ-012**: The protected API contract and static docs must be updated in the same change: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/server.py` OpenAPI spec, `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/README.md`, and `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/docs.html`.
- **SEC-001**: All protected S3 admin JSON routes must continue to require valid bearer-token access and/or the existing short-lived S3 admin cookie flow; the refactor must not weaken current access control.
- **SEC-002**: Dry-run preview jobs must not create JSON backups because backups are write operations and would violate the dry-run guarantee.
- **CON-001**: No database refresh/reset command may be introduced or executed as part of this refactor.
- **CON-002**: Existing restart behavior in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` must remain intact: active jobs loaded after restart become `interrupted` instead of remaining `running`.
- **CON-003**: The refactor must extend the existing `server.py`, `s3_jobs.py`, `/scripts/*.py`, and `/s3` admin structure instead of introducing a parallel job system unrelated to the current protected S3 admin surface.
- **GUD-001**: Canonical persisted job fields must be named `job_family` (`upload|url_migration|state_cleanup`) and `dry_run` (`true|false`).
- **GUD-002**: Legacy `kind` may remain as a compatibility alias during migration, but no new routing, rendering, or filtering logic may branch on `kind` string prefixes after the refactor.
- **PAT-001**: Backend centralization must use one shared registry module at `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_job_operations.py` that maps each `job_family` to its collector, runner builder, API route prefix, success label, and dry-run behavior.
- **PAT-002**: Frontend centralization must use one `JOB_FAMILY_CONFIG` map in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.js` keyed by the same canonical `job_family` values used by the backend.
- **PAT-003**: Family-specific list rendering, hints, button text, and detail labels must all derive from the same `JOB_FAMILY_CONFIG` entry for each family.

## 2. Implementation Steps

### Implementation Phase 1

- **GOAL-001**: Introduce a canonical persisted S3 job model and shared backend operation registry so every job record has explicit `job_family` and `dry_run` metadata, legacy jobs remain readable, and family-specific behavior is declared in one backend location. Completion criteria: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` can load both legacy and refactored jobs, `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_job_operations.py` exists, and no backend route or renderer needs to infer family from `kind` prefix parsing.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-001 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` `S3JobState` (current definition near lines 26-43) to add `job_family: str` and `dry_run: bool`, keep `kind` only as a compatibility alias, and ensure `list_jobs()` / `get_job()` serialize the new fields. |  |  |
| TASK-002 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` `_coerce_job()` (current definition near lines 519-539) to normalize legacy `kind` values into canonical `job_family` + `dry_run`, including `migration_preview -> url_migration + dry_run=true` and `cleanup_preview -> state_cleanup + dry_run=true`. |  |  |
| TASK-003 | Refactor `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` `start_job()`, `start_custom_job()`, `_run_job()`, and `_run_custom_job()` (current definitions near lines 83-224) so one shared internal bookkeeping path creates job records, persists progress, and counts success for both write jobs and dry-run preview jobs. Introduce explicit support for `item.status == 'preview'` as a successful dry-run result when `job.dry_run=true`. |  |  |
| TASK-004 | Create `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_job_operations.py` with a single `JOB_DEFINITIONS` registry keyed by `upload`, `url_migration`, and `state_cleanup`. Each registry entry must define exact route slug, collector function, runner builder, default metric label, and frontend-facing copy used by the API response layer. |  |  |
| TASK-005 | Add or update an automated compatibility test in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_job_history_persistence.py` so a persisted legacy payload using `migration_preview`, `cleanup`, and `upload` loads back with the correct canonical `job_family` and `dry_run` values after restart. |  |  |

### Implementation Phase 2

- **GOAL-002**: Separate backend route families and unify create/preview/cancel semantics so uploads, URL migrations, and stale-state cleanup are isolated API workflows with identical dry-run behavior. Completion criteria: `server.py` exposes canonical list/create endpoints for all three families, preview is expressed only as `dry_run=true` job creation, and family handlers are thin adapters over the shared registry instead of bespoke duplicated logic.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-006 | Replace the current mixed route layout in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/server.py` OpenAPI `paths` section (current S3 job routes near lines 984-1124) with canonical family-separated endpoints: `GET/POST /api/s3/upload-jobs`, `GET/POST /api/s3/url-migration-jobs`, and `GET/POST /api/s3/state-cleanup-jobs`. Keep `GET /api/s3/jobs/{job_id}` and `POST /api/s3/jobs/{job_id}/cancel` as shared detail/cancel endpoints. |  |  |
| TASK-007 | Refactor `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/server.py` handlers `handle_s3_jobs_list()`, `handle_s3_jobs_create()`, `handle_s3_migration_summary()`, `handle_s3_cleanup_summary()`, `handle_s3_migration_job_create()`, and `handle_s3_cleanup_job_create()` (current definitions near lines 2438-2778) into family-specific list/create handlers that call the shared `JOB_DEFINITIONS` registry. Remove frontend dependence on `handle_s3_migration_summary()` and `handle_s3_cleanup_summary()` by making preview creation itself the single source of truth. |  |  |
| TASK-008 | Implement upload dry-run collection in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_job_operations.py` by reusing the current dataset-selection logic from `handle_s3_jobs_create()` and the current URL-resolution logic, but forbidding `put_object`, `save_s3_state()`, `on_uploaded()`, `migrate_write_backup()`, and `s3_cleanup_write_backup()` whenever `dry_run=true`. |  |  |
| TASK-009 | Ensure migration and cleanup family runners in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_job_operations.py` reuse the existing script helpers from `/scripts/migrate_aws_public_urls.py` and `/scripts/cleanup_stale_s3_objects.py`, but branch cleanly between `dry_run=true` item recording and `dry_run=false` write execution without duplicating per-family control flow in `server.py`. |  |  |
| TASK-010 | Add explicit family filtering to backend list responses so each family tab can request only its own history set. If temporary compatibility routes are retained for one release, mark them deprecated in OpenAPI and implement them as thin shims over the new family handlers. |  |  |
| TASK-011 | Keep shared cancellation behavior in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/server.py` `handle_s3_job_cancel()` but reject cancellation of jobs not in an active status and ensure family-separated frontend stop buttons only target IDs returned by the current family list endpoint. |  |  |

### Implementation Phase 3

- **GOAL-003**: Refactor the `/s3` frontend into three isolated tabs with one shared family-driven workflow implementation so the UI is consistent, dry-run aware, and maintainable. Completion criteria: `/s3` shows three tabs, each tab can preview/start/stop its own family, cleanup is no longer rendered in the migration tab, and all history/detail rendering derives from explicit `job_family` and `dry_run` fields.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-012 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.html` (current S3 workspace markup near lines 55-158) to expose three top-level tabs and three dedicated panels: `upload`, `url-migration`, and `state-cleanup`. Move the cleanup controls and history list out of the current migration panel and give cleanup its own list container such as `cleanupJobsList`. |  |  |
| TASK-013 | Replace the ad hoc DOM/event wiring in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.js` `init()`, `bindEvents()`, `refreshS3Jobs()`, and `renderS3Jobs()` (current definitions near lines 94-107, 110-140, 260-314, and 354-363) with one `JOB_FAMILY_CONFIG` map and generic helper functions `refreshFamilyJobs(family)`, `submitFamilyJobAction(family, dryRun)`, `renderFamilyJobList(family, jobs)`, and `updateFamilyHint(family, jobs)`. |  |  |
| TASK-014 | Refactor `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.js` `renderJobCard()` and `renderJobDetailsModal()` (current definitions near lines 316-335 and 456-543) so card badges and modal labels derive from `job.job_family` and `job.dry_run`. Remove the static `upload` badge, add a `dry-run` badge, and keep only status badges plus family-specific metric labels such as `Uploadés`, `Migrés`, or `Nettoyés`. |  |  |
| TASK-015 | Replace the current global stop logic in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.js` `stopActiveS3Job()` (current definition near lines 801-822) with family-scoped stop handlers so the upload tab cannot cancel migration/cleanup jobs and the cleanup tab cannot cancel upload/migration jobs. Keep modal-level `Annuler ce job` behavior for the selected job ID. |  |  |
| TASK-016 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.js` preview/start functions (current definitions near lines 639-799) so all three families follow the same request pattern: one create call with `dry_run=true` for preview, one create call with `dry_run=false` for write mode, and no separate summary-fetch step before the preview job is created. |  |  |
| TASK-017 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/styles.css` S3 admin styles (current S3 section near lines 1255-1755) to support the third tab, family-specific panel spacing, and a dedicated `.job-dry-run` badge style that remains readable in both job cards and the job detail modal. |  |  |

### Implementation Phase 4

- **GOAL-004**: Bring docs and automated verification into alignment with the refactored protected S3 job workflow so the change is reproducible and safe to ship. Completion criteria: docs describe the three separated job families and dry-run behavior, OpenAPI exposes the new fields/routes, automated tests cover family separation and dry-run behavior, and browser validation confirms correct tab placement and badge rendering.

| Task | Description | Completed | Date |
|------|-------------|-----------|------|
| TASK-018 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/server.py` OpenAPI schema definitions (current `S3JobState`, request bodies, and S3 route schemas near lines 575-706 and 984-1124) to add `job_family`, `dry_run`, family-separated request/response schemas, and any deprecated compatibility routes retained during rollout. |  |  |
| TASK-019 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/README.md` S3 admin documentation (current S3 notes near lines 155-166) so it describes three separate S3 admin job families, dry-run-only preview semantics, and the new family-specific protected routes. |  |  |
| TASK-020 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/docs.html` so the static served docs mention the separated S3 admin job families, the `dry_run` field, and the new canonical S3 route families instead of the old mixed summary/job flow. |  |  |
| TASK-021 | Add `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_upload_job_dry_run.py` to assert that upload preview creates a `dry_run=true` job, records preview items, performs zero S3 writes, and does not update `s3_objects`. |  |  |
| TASK-022 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_migration_job.py` and `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_cleanup_job.py` so both dry-run and write-mode variants assert the canonical `job_family` values (`url_migration`, `state_cleanup`) and `dry_run` behavior. |  |  |
| TASK-023 | Add a route-level test file such as `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_job_family_routes.py` to verify that each family list endpoint returns only its own jobs and that a cleanup preview no longer appears in the upload history set. |  |  |
| TASK-024 | Update `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_openapi_richness.py` so it asserts the new protected S3 route families and the presence of `job_family` + `dry_run` in the schema exported by `/openapi.json`. |  |  |
| TASK-025 | Perform browser-level verification on `/s3`: unlock the page, visit all three tabs, create one preview job per family, confirm each preview appears only in the correct history list with a `dry-run` badge, confirm the write-mode start button creates a non-dry-run job in the same family, and confirm the stop control only cancels jobs from the current tab. |  |  |

## 3. Alternatives

- **ALT-001**: Fix only the immediate classification bug in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.js` by teaching `renderS3Jobs()` to recognize `cleanup*` kinds. Rejected because it leaves the mixed workflow, the shared migration/cleanup tab, the global stop action, and the backend duplication intact.
- **ALT-002**: Keep separate summary endpoints (`/api/s3/migration-summary` and `/api/s3/cleanup-summary`) as the canonical preview flow and add a dedicated upload summary endpoint. Rejected because it would preserve two different mental models: summary-first preview for some families and job-first execution for others.
- **ALT-003**: Duplicate full list/create/render logic once per family in both `server.py` and `s3.js` without shared registries. Rejected because that would lock the codebase into the same drift and inconsistency problem the user reported.

## 4. Dependencies

- **DEP-001**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` existing persistence, cancellation, and restart recovery logic.
- **DEP-002**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/migrate_aws_public_urls.py` functions `collect_changes`, `write_backup`, and `apply_changes`.
- **DEP-003**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/cleanup_stale_s3_objects.py` functions `collect_stale_rows`, `write_backup`, and `apply_cleanup`.
- **DEP-004**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/shared-ui.js` existing tab/password-toggle helpers used by `/s3`.
- **DEP-005**: Protected API auth behavior implemented in `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/server.py` for bearer tokens and the `ff_s3_auth` cookie.

## 5. Files

- **FILE-001**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.html` — replace the current two-tab S3 admin layout with three dedicated family tabs and history sections.
- **FILE-002**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3.js` — centralize family config, API requests, list rendering, modal labels, dry-run badges, and family-scoped stop behavior.
- **FILE-003**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/styles.css` — style the third tab, family sections, and `dry-run` badge.
- **FILE-004**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py` — add canonical job metadata and unify bookkeeping for preview/write jobs.
- **FILE-005**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_job_operations.py` — new shared backend registry and runner/collector helpers for all S3 job families.
- **FILE-006**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/server.py` — expose family-separated routes, family-aware handlers, and updated OpenAPI definitions.
- **FILE-007**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_upload_job_dry_run.py` — new upload preview coverage.
- **FILE-008**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_migration_job.py` — update migration family and dry-run assertions.
- **FILE-009**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_cleanup_job.py` — update cleanup family and dry-run assertions.
- **FILE-010**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_job_history_persistence.py` — legacy normalization and restart compatibility coverage.
- **FILE-011**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_job_family_routes.py` — new family-separation API coverage.
- **FILE-012**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_openapi_richness.py` — validate the new protected S3 route families and schema fields.
- **FILE-013**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/README.md` — describe the new admin workflow and route layout.
- **FILE-014**: `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/docs.html` — keep static docs aligned with the protected S3 admin API.

## 6. Testing

- **TEST-001**: Run `python3 /home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_upload_job_dry_run.py` and verify upload preview creates `dry_run=true` jobs with zero S3 and SQLite writes.
- **TEST-002**: Run `python3 /home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_migration_job.py` and verify both preview and write-mode migration jobs produce the correct `job_family`, `dry_run`, and persisted history.
- **TEST-003**: Run `python3 /home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_cleanup_job.py` and verify both preview and write-mode cleanup jobs produce the correct `job_family`, `dry_run`, and cleanup side effects only in write mode.
- **TEST-004**: Run `python3 /home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_job_history_persistence.py` and verify legacy jobs normalize correctly and interrupted-on-restart behavior remains intact.
- **TEST-005**: Run `python3 /home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_s3_job_family_routes.py` and verify family-specific list endpoints never leak jobs into the wrong tab history.
- **TEST-006**: Run `python3 /home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/test_openapi_richness.py` and verify `/openapi.json` exposes `job_family`, `dry_run`, and the new S3 route families.
- **TEST-007**: Perform browser-level validation on `/s3` against a running local server and verify unlock flow, three tabs, dry-run badge visibility, correct history placement, modal detail labeling, and family-scoped stop behavior.

## 7. Risks & Assumptions

- **RISK-001**: Any missed legacy `kind` normalization path will place old jobs in the wrong tab or remove their dry-run badge after deployment.
- **RISK-002**: Upload dry-run preview can become slow if it performs expensive existence checks for many candidate images; the implementation must keep current `limit` and `concurrency` boundaries and avoid unbounded scans.
- **RISK-003**: Removing or renaming summary endpoints without synchronized doc and UI changes will leave the protected S3 admin UI partially broken.
- **RISK-004**: Family-scoped stop behavior can still cancel the wrong job if the frontend keeps a mixed list cache instead of requesting family-separated history.
- **ASSUMPTION-001**: The protected S3 admin API can accept additive route changes and controlled deprecations without breaking public product/category clients because the S3 admin endpoints are already behind protected access.
- **ASSUMPTION-002**: Reusing the existing numeric field `uploaded` as the generic success counter is acceptable as long as UI labels are family-specific and `dry_run` is explicit.
- **ASSUMPTION-003**: No schema migration is required in SQLite because S3 job payloads are stored as JSON and can be normalized during read/write inside `s3_jobs.py`.
- **ASSUMPTION-004**: Browser verification tooling will be available when the implementation starts so the refactor can be validated on the actual `/s3` page instead of by source inspection alone.

## 8. Related Specifications / Further Reading

- `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/README.md`
- `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/docs.html`
- `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/server.py`
- `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/s3_jobs.py`
- `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/migrate_aws_public_urls.py`
- `/home/ai/.openclaw/workspace-fast-fashion-dashboard/fast-fashion-dashboard/scripts/cleanup_stale_s3_objects.py`
- `/home/ai/.openclaw/workspace-fast-fashion-dashboard/MEMORY.md`

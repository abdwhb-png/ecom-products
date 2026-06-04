from __future__ import annotations

import math
from http import HTTPStatus
from urllib.parse import parse_qs, urlparse

from s3_job_operations import JOB_DEFINITIONS, STATE_CLEANUP_JOB_FAMILY, UPLOAD_JOB_FAMILY, URL_MIGRATION_JOB_FAMILY, build_job_id
from s3_jobs import ACTIVE_JOB_STATUSES

from backend.auth import handle_s3_auth as auth_handle_s3_auth
from backend.http_utils import error_response, json_response
from backend.text_utils import parse_bool, parse_positive_int


def s3_job_definition(job_family: str):
    definition = JOB_DEFINITIONS.get(job_family)
    if not definition:
        raise ValueError(f'Unknown S3 job family: {job_family}')
    return definition


def handle_s3_auth(handler, read_json_body):
    return auth_handle_s3_auth(handler, read_json_body)


def handle_s3_config_get(handler, *, effective_s3_config_fn):
    json_response(handler, {'data': effective_s3_config_fn()})


def handle_s3_family_jobs_list(handler, job_family: str, *, manager, load_s3_state_fn, effective_s3_config_fn, publicize_job_payload):
    definition = s3_job_definition(job_family)
    load_s3_state_fn(force=True)
    query = parse_qs(urlparse(handler.path).query)
    page = parse_positive_int((query.get('page') or ['1'])[0], 1)
    page_size = parse_positive_int((query.get('pageSize') or ['20'])[0], 20, maximum=100)
    listed = manager.list_jobs(job_family=job_family, page=page, page_size=page_size)
    json_response(handler, {
        'data': [publicize_job_payload(job, include_items=False) for job in listed['jobs']],
        'pagination': listed['pagination'],
        'config': effective_s3_config_fn(),
        'job_family': job_family,
        'family': definition.api_meta(),
    })


def handle_s3_family_summary(handler, job_family: str, *, make_context_fn):
    definition = s3_job_definition(job_family)
    if not definition.summary_builder:
        raise ValueError(f'No summary endpoint available for {job_family}')
    payload = handler._read_json_body() if handler.headers.get('Content-Length') else {}
    context = make_context_fn()
    collected = definition.collector(payload, context)
    json_response(handler, {
        'data': definition.summary_builder(collected, context),
        'job_family': job_family,
        'family': definition.api_meta(),
    })


def handle_s3_family_job_create(handler, job_family: str, *, manager, make_context_fn, publicize_job_payload):
    definition = s3_job_definition(job_family)
    payload = handler._read_json_body() if handler.headers.get('Content-Length') else {}
    dry_run = parse_bool(payload.get('dry_run', payload.get('preview', False)))
    context = make_context_fn()
    initial_collected = definition.collector(payload, context)
    dataset_id = str(initial_collected.get('dataset_id') or '').strip()
    job_id = build_job_id(job_family, dataset_id if dataset_id and dataset_id != 'all' else None)
    if job_family == UPLOAD_JOB_FAMILY:
        active_same_dataset = manager.count_active_jobs(job_family=UPLOAD_JOB_FAMILY, dataset_id=dataset_id)
        selection_mode = str(payload.get('selection_mode') or 'pending').strip().lower() or 'pending'
        context_for_claim = context

        def _bind_claim_conn(conn):
            nonlocal context_for_claim
            context_for_claim = make_context_fn(db_conn=conn)

        def _collect_unclaimed(excluded_product_ids: set[str]):
            next_payload = dict(payload)
            if excluded_product_ids:
                next_payload['_excluded_product_ids'] = sorted(excluded_product_ids)
            return definition.collector(next_payload, context_for_claim)

        collected = manager.reserve_upload_candidates(
            job_id=job_id,
            dataset_id=dataset_id,
            collector=_collect_unclaimed,
            db_conn_binder=_bind_claim_conn,
        )
        if not collected.get('rows'):
            if active_same_dataset > 0:
                return error_response(
                    handler,
                    f"Aucun produit libre pour lancer un nouvel upload {dataset_id} maintenant. Un autre job actif a déjà réservé les candidats disponibles.",
                    HTTPStatus.CONFLICT,
                    code='s3_upload_no_free_products',
                )
            if selection_mode != 'all':
                return error_response(
                    handler,
                    f"Aucun produit candidat disponible pour {dataset_id} avec selection_mode={selection_mode}.",
                    HTTPStatus.CONFLICT,
                    code='s3_upload_no_candidates',
                )
    else:
        collected = initial_collected
    run_spec = definition.runner_builder(job_id, dry_run, collected, context_for_claim if job_family == UPLOAD_JOB_FAMILY else context)
    start_mode = run_spec.get('mode')
    kwargs = dict(run_spec.get('kwargs') or {})
    try:
        if start_mode == 'start_job':
            future = manager.start_job(**kwargs)
        elif start_mode == 'start_custom_job':
            initial_patch = dict(run_spec.get('initial_job_patch') or {})
            future = manager.start_custom_job(**kwargs)
            if initial_patch:
                manager.update_job(job_id, **initial_patch)
        else:
            raise ValueError(f'Unsupported S3 job runner mode: {start_mode}')
    except Exception:
        if job_family == UPLOAD_JOB_FAMILY:
            manager.release_job_claims(job_id)
        raise
    json_response(handler, {
        'data': publicize_job_payload(manager.get_job(job_id)),
        'future': bool(future),
        'job_family': job_family,
        'family': definition.api_meta(),
    }, status=HTTPStatus.ACCEPTED)


def handle_s3_job_cancel(handler, job_id, *, manager, publicize_job_payload):
    job = manager.get_job(job_id)
    if not job:
        return error_response(handler, f'Job not found: {job_id}', HTTPStatus.NOT_FOUND, code='not_found')
    if job.get('status') not in ACTIVE_JOB_STATUSES:
        return error_response(handler, f'Job is not active: {job_id}', HTTPStatus.BAD_REQUEST, code='invalid_request')
    if not manager.cancel_job(job_id):
        return error_response(handler, f'Job not found: {job_id}', HTTPStatus.NOT_FOUND, code='not_found')
    json_response(handler, {'data': publicize_job_payload(manager.get_job(job_id))}, status=HTTPStatus.ACCEPTED)


def handle_s3_job_detail(handler, job_id, *, manager, load_s3_state_fn, publicize_job_item, publicize_job_payload):
    load_s3_state_fn(force=True)
    raw_job = manager.get_job(job_id)
    if not raw_job:
        return error_response(handler, f'Job not found: {job_id}', HTTPStatus.NOT_FOUND, code='not_found')
    query = parse_qs(urlparse(handler.path).query)
    page = parse_positive_int((query.get('page') or ['1'])[0], 1)
    page_size = parse_positive_int((query.get('page_size') or ['12'])[0], 12, maximum=50)
    raw_items = list(raw_job.get('items') or [])
    total_items = len(raw_items)
    start = (page - 1) * page_size
    end = start + page_size
    paged_items = [publicize_job_item(item) for item in raw_items[start:end]]
    job = publicize_job_payload(raw_job, include_items=False)
    json_response(handler, {
        'data': {
            'job': job,
            'items': paged_items,
            'page': page,
            'page_size': page_size,
            'total_items': total_items,
            'total_pages': max(1, math.ceil(total_items / page_size)) if total_items else 1,
        }
    })


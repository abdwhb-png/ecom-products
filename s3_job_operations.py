from __future__ import annotations

import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from scripts.cleanup_stale_s3_objects import apply_cleanup as s3_cleanup_apply
from scripts.cleanup_stale_s3_objects import collect_stale_rows as s3_cleanup_collect
from scripts.cleanup_stale_s3_objects import write_backup as s3_cleanup_write_backup
from scripts.migrate_aws_public_urls import apply_changes as migrate_apply_changes
from scripts.migrate_aws_public_urls import collect_changes as migrate_collect_changes
from scripts.migrate_aws_public_urls import write_backup as migrate_write_backup

try:
    import boto3
    from botocore.config import Config
except Exception:  # pragma: no cover
    boto3 = None
    Config = None


UPLOAD_JOB_FAMILY = 'upload'
URL_MIGRATION_JOB_FAMILY = 'url_migration'
STATE_CLEANUP_JOB_FAMILY = 'state_cleanup'
JOB_FAMILIES = (
    UPLOAD_JOB_FAMILY,
    URL_MIGRATION_JOB_FAMILY,
    STATE_CLEANUP_JOB_FAMILY,
)

LEGACY_KIND_MAP: dict[str, tuple[str, bool]] = {
    'upload': (UPLOAD_JOB_FAMILY, False),
    'migration': (URL_MIGRATION_JOB_FAMILY, False),
    'migration_preview': (URL_MIGRATION_JOB_FAMILY, True),
    'cleanup': (STATE_CLEANUP_JOB_FAMILY, False),
    'cleanup_preview': (STATE_CLEANUP_JOB_FAMILY, True),
}

DEFAULT_KIND_BY_FAMILY = {
    UPLOAD_JOB_FAMILY: 'upload',
    URL_MIGRATION_JOB_FAMILY: 'migration',
    STATE_CLEANUP_JOB_FAMILY: 'cleanup',
}

PREVIEW_KIND_BY_FAMILY = {
    UPLOAD_JOB_FAMILY: 'upload',
    URL_MIGRATION_JOB_FAMILY: 'migration_preview',
    STATE_CLEANUP_JOB_FAMILY: 'cleanup_preview',
}


def parse_positive_int(value: Any, default: int, minimum: int = 1, maximum: int | None = None) -> int:
    try:
        parsed = int(str(value).strip())
    except Exception:
        parsed = default
    parsed = max(minimum, parsed)
    if maximum is not None:
        parsed = min(parsed, maximum)
    return parsed


def parse_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    normalized = str(value).strip().lower()
    if normalized in {'1', 'true', 'yes', 'on'}:
        return True
    if normalized in {'0', 'false', 'no', 'off'}:
        return False
    return default


def canonical_kind(job_family: str, dry_run: bool) -> str:
    family = str(job_family or '').strip() or UPLOAD_JOB_FAMILY
    if dry_run and family in PREVIEW_KIND_BY_FAMILY:
        return PREVIEW_KIND_BY_FAMILY[family]
    return DEFAULT_KIND_BY_FAMILY.get(family, 'upload')


def normalize_job_metadata(*, kind: Any = None, job_family: Any = None, dry_run: Any = None) -> dict[str, Any]:
    provided_kind = str(kind or '').strip()
    provided_family = str(job_family or '').strip()
    explicit_dry_run = None if dry_run is None else bool(dry_run)

    if provided_family in JOB_FAMILIES:
        family = provided_family
        resolved_dry_run = bool(explicit_dry_run) if explicit_dry_run is not None else False
    elif provided_kind in LEGACY_KIND_MAP:
        family, inferred_dry_run = LEGACY_KIND_MAP[provided_kind]
        resolved_dry_run = inferred_dry_run if explicit_dry_run is None else bool(explicit_dry_run)
    else:
        family = UPLOAD_JOB_FAMILY
        resolved_dry_run = bool(explicit_dry_run) if explicit_dry_run is not None else False

    return {
        'job_family': family,
        'dry_run': resolved_dry_run,
        'kind': canonical_kind(family, resolved_dry_run),
    }


def build_job_id(job_family: str, dataset_id: str | None = None, now: float | None = None) -> str:
    timestamp = int(now if now is not None else time.time())
    family = str(job_family or '').strip() or UPLOAD_JOB_FAMILY
    prefix = str(dataset_id or '').strip().lower().replace(':', '-')
    family_slug = family.replace('_', '-')
    if prefix:
        return f'{prefix}-{family_slug}-{timestamp}'
    return f'{family_slug}-{timestamp}'


@dataclass(frozen=True)
class JobOperationDefinition:
    job_family: str
    route_slug: str
    source: str
    label: str
    metric_label: str
    history_title: str
    preview_label: str
    start_label: str
    stop_label: str
    collector: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]
    runner_builder: Callable[[str, bool, dict[str, Any], dict[str, Any]], dict[str, Any]]
    summary_builder: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]] | None = None

    def api_meta(self) -> dict[str, Any]:
        return {
            'job_family': self.job_family,
            'route_slug': self.route_slug,
            'label': self.label,
            'metric_label': self.metric_label,
            'history_title': self.history_title,
            'preview_label': self.preview_label,
            'start_label': self.start_label,
            'stop_label': self.stop_label,
        }


def _resolve_upload_source_urls(row: dict[str, Any], *, parse_json_list_fn: Callable[[Any], list[Any]]) -> list[str]:
    urls: list[str] = []
    for value in [row.get('image')] + parse_json_list_fn(row.get('image_urls_json')):
        if not isinstance(value, str):
            continue
        cleaned = value.strip()
        if not cleaned:
            continue
        lowered = cleaned.lower()
        if lowered.startswith(('http://', 'https://')):
            urls.append(cleaned)
    return list(dict.fromkeys(urls))


def _make_upload_s3_client_factory(config: dict[str, Any], *, resolve_region_fn: Callable[[str | None, str | None], str | None]):
    def factory():
        if boto3 is None:
            raise RuntimeError('boto3 is not available')
        env_access_key = os.getenv('AWS_ACCESS_KEY_ID') or os.getenv('AWS_ACCESS_KEY')
        env_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY') or os.getenv('AWS_SECRET_KEY')
        endpoint_url = config.get('endpoint_url') or None
        env_session_token = os.getenv('AWS_SESSION_TOKEN')
        if endpoint_url and 'r2.cloudflarestorage.com' in str(endpoint_url).lower():
            env_session_token = None
        session = boto3.session.Session(
            aws_access_key_id=env_access_key or None,
            aws_secret_access_key=env_secret_key or None,
            aws_session_token=env_session_token or None,
            region_name=resolve_region_fn(endpoint_url, config.get('region_name')),
        )
        client_kwargs: dict[str, Any] = {'endpoint_url': endpoint_url}
        if endpoint_url and Config is not None:
            client_kwargs['config'] = Config(s3={'addressing_style': 'path'})
        return session.client('s3', **client_kwargs)

    return factory


def collect_upload_candidates(payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    dataset_id = str(payload.get('dataset_id') or 'shein').strip().lower()
    source = str(payload.get('source') or 'products').strip().lower()
    limit = parse_positive_int(payload.get('limit', 100), 100)
    concurrency = parse_positive_int(payload.get('concurrency', 4), 4, maximum=24)
    if dataset_id == 'asos':
        concurrency = min(concurrency, 2)

    allowed_datasets = set(context['allowed_datasets'])
    if dataset_id not in allowed_datasets:
        raise ValueError(f'Unknown dataset: {dataset_id}')

    config = context['effective_s3_config']()
    bucket = str(config.get('bucket') or '').strip()
    prefix = str(config.get('prefix') or '').strip()
    if not bucket:
        raise ValueError('Missing AWS_BUCKET')

    conn = context['db_connect']()
    try:
        rows = conn.execute(
            'SELECT * FROM products WHERE dataset_id = ? ORDER BY id ASC LIMIT ?',
            (dataset_id, limit),
        ).fetchall()
        selected = [dict(row) for row in rows]
    finally:
        conn.close()

    return {
        'dataset_id': dataset_id,
        'source': source,
        'rows': selected,
        'limit': limit,
        'concurrency': concurrency,
        'source_filter': payload.get('source_filter'),
        'bucket': bucket,
        'prefix': prefix,
        'config': config,
        'total': len(selected),
    }


def build_upload_runner(job_id: str, dry_run: bool, collected: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    parse_json_list_fn = context['parse_json_list']
    resolve_region_fn = context['resolve_s3_region']

    def resolve_source_url(row: dict[str, Any]) -> list[str]:
        return _resolve_upload_source_urls(row, parse_json_list_fn=parse_json_list_fn)

    on_uploaded = None
    if not dry_run:
        def on_uploaded(row: dict[str, Any], item: dict[str, Any]) -> None:
            context['persist_upload_item'](
                dataset_id=collected['dataset_id'],
                bucket=collected['bucket'],
                row=row,
                item=item,
            )

    return {
        'mode': 'start_job',
        'kwargs': {
            'job_id': job_id,
            'dataset_id': collected['dataset_id'],
            'source': collected['source'],
            'bucket': collected['bucket'],
            'prefix': collected['prefix'],
            'limit': collected['limit'],
            'concurrency': collected['concurrency'],
            'source_filter': collected.get('source_filter'),
            'rows': collected['rows'],
            's3_client_factory': _make_upload_s3_client_factory(collected['config'], resolve_region_fn=resolve_region_fn),
            'resolve_source_url': resolve_source_url,
            'on_uploaded': on_uploaded,
            'dry_run': dry_run,
            'job_family': UPLOAD_JOB_FAMILY,
        },
    }


def collect_url_migration_changes(payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    sample_limit = parse_positive_int(payload.get('sample_limit', 25), 25, maximum=200)
    public_url = str(context['resolve_aws_public_url']() or '').strip()
    if not public_url:
        raise ValueError('Missing AWS_URL')
    return {
        'dataset_id': 'all',
        'source': 'migration',
        'public_url': public_url,
        'sample_limit': sample_limit,
        'bucket': str(context['resolve_aws_bucket']() or '').strip() or None,
        'prefix': str(context['resolve_aws_prefix']() or '').strip(),
        'total': 0,
    }


def build_url_migration_runner(job_id: str, dry_run: bool, collected: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    def runner(record_item: Callable[[dict[str, Any]], None], cancel_event) -> None:
        run_conn = context['db_connect']()
        try:
            run_conn.row_factory = sqlite3.Row
            local_changes = migrate_collect_changes(run_conn, collected['public_url'])
            selected = local_changes[: collected['sample_limit']] if dry_run else local_changes
            context['update_job'](
                job_id,
                total=len(selected),
                limit=max(1, len(selected) or 1),
                last_message='Prévisualisation prête.' if dry_run else f"{len(selected)} élément(s) à traiter.",
            )
            if dry_run:
                for change in selected:
                    if cancel_event.is_set():
                        break
                    record_item({
                        'status': 'preview',
                        'message': 'Preview migration item',
                        'timestamp': time.time(),
                        'goods_id': change['goods_id'],
                        'old_s3_url': change['old_s3_url'],
                        'new_s3_url': change['new_s3_url'],
                        'changed_fields': change['changed_fields'],
                    })
                return

            backup_path = Path(context['root']) / f"s3_objects_backup_{int(time.time())}.json"
            migrate_write_backup(run_conn, backup_path)
            updated_at = time.time()
            for change in selected:
                if cancel_event.is_set():
                    raise RuntimeError('Migration cancelled')
                migrate_apply_changes(run_conn, [change], updated_at=updated_at)
                record_item({
                    'status': 'uploaded',
                    'message': 'Migrated stored URLs to AWS_URL',
                    'timestamp': time.time(),
                    'goods_id': change['goods_id'],
                    'old_s3_url': change['old_s3_url'],
                    'new_s3_url': change['new_s3_url'],
                    'changed_fields': change['changed_fields'],
                    'backup_path': str(backup_path),
                })
            context['load_s3_state'](force=True)
        finally:
            run_conn.close()

    return {
        'mode': 'start_custom_job',
        'kwargs': {
            'job_id': job_id,
            'dataset_id': collected['dataset_id'],
            'source': collected['source'],
            'total': 0,
            'runner': runner,
            'bucket': collected['bucket'],
            'prefix': collected['prefix'],
            'concurrency': 1,
            'limit': 1,
            'dry_run': dry_run,
            'job_family': URL_MIGRATION_JOB_FAMILY,
        },
        'initial_job_patch': {
            'last_message': 'Analyse en arrière-plan…',
        },
    }


def summarize_url_migration(collected: dict[str, Any], _context: dict[str, Any]) -> dict[str, Any]:
    sample_limit = collected['sample_limit']
    return {
        'total': len(collected['changes']),
        'sample_limit': sample_limit,
        'public_url': collected['public_url'],
        'sample': [
            {
                'goods_id': change['goods_id'],
                'old_s3_url': change['old_s3_url'],
                'new_s3_url': change['new_s3_url'],
                'changed_fields': change['changed_fields'],
            }
            for change in collected['changes'][:sample_limit]
        ],
    }


def collect_state_cleanup_changes(payload: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    sample_limit = parse_positive_int(payload.get('sample_limit', 25), 25, maximum=200)
    bucket = str(context['resolve_aws_bucket']() or '').strip()
    return {
        'dataset_id': 'all',
        'source': 'cleanup',
        'sample_limit': sample_limit,
        'bucket': bucket or None,
        'prefix': str(context['resolve_aws_prefix']() or '').strip(),
        'total': 0,
    }


def build_state_cleanup_runner(job_id: str, dry_run: bool, collected: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    def runner(record_item: Callable[[dict[str, Any]], None], cancel_event) -> None:
        run_conn = context['db_connect']()
        try:
            run_conn.row_factory = sqlite3.Row
            local_changes = s3_cleanup_collect(run_conn, bucket=str(collected.get('bucket') or ''))
            selected = local_changes[: collected['sample_limit']] if dry_run else local_changes
            context['update_job'](
                job_id,
                total=len(selected),
                limit=max(1, len(selected) or 1),
                last_message='Prévisualisation prête.' if dry_run else f"{len(selected)} élément(s) à traiter.",
            )
            if dry_run:
                for change in selected:
                    if cancel_event.is_set():
                        break
                    record_item({
                        'status': 'preview',
                        'message': 'Preview stale S3-state cleanup item',
                        'timestamp': time.time(),
                        'goods_id': change['goods_id'],
                        'bucket': change['bucket'],
                        'current_bucket': change['current_bucket'],
                        's3_url': change['s3_url'],
                        'reason': change['reason'],
                    })
                return

            backup_path = Path(context['root']) / f"s3_objects_cleanup_backup_{int(time.time())}.json"
            s3_cleanup_write_backup(run_conn, backup_path)
            updated_at = time.time()
            for change in selected:
                if cancel_event.is_set():
                    raise RuntimeError('Cleanup cancelled')
                s3_cleanup_apply(run_conn, [change], updated_at=updated_at)
                record_item({
                    'status': 'uploaded',
                    'message': 'Cleared stale saved_on_s3 state',
                    'timestamp': time.time(),
                    'goods_id': change['goods_id'],
                    'bucket': change['bucket'],
                    'current_bucket': change['current_bucket'],
                    's3_url': change['s3_url'],
                    'reason': change['reason'],
                    'backup_path': str(backup_path),
                })
            context['load_s3_state'](force=True)
        finally:
            run_conn.close()

    return {
        'mode': 'start_custom_job',
        'kwargs': {
            'job_id': job_id,
            'dataset_id': collected['dataset_id'],
            'source': collected['source'],
            'total': 0,
            'runner': runner,
            'bucket': collected['bucket'],
            'prefix': collected['prefix'],
            'concurrency': 1,
            'limit': 1,
            'dry_run': dry_run,
            'job_family': STATE_CLEANUP_JOB_FAMILY,
        },
        'initial_job_patch': {
            'last_message': 'Analyse en arrière-plan…',
        },
    }


def summarize_state_cleanup(collected: dict[str, Any], _context: dict[str, Any]) -> dict[str, Any]:
    sample_limit = collected['sample_limit']
    return {
        'total': len(collected['changes']),
        'sample_limit': sample_limit,
        'current_bucket': collected['bucket'],
        'sample': [
            {
                'goods_id': change['goods_id'],
                'bucket': change['bucket'],
                'current_bucket': change['current_bucket'],
                's3_url': change['s3_url'],
                'reason': change['reason'],
            }
            for change in collected['changes'][:sample_limit]
        ],
    }


JOB_DEFINITIONS: dict[str, JobOperationDefinition] = {
    UPLOAD_JOB_FAMILY: JobOperationDefinition(
        job_family=UPLOAD_JOB_FAMILY,
        route_slug='upload-jobs',
        source='products',
        label='Uploads S3',
        metric_label='Uploadés',
        history_title='Jobs d’upload',
        preview_label='Prévisualiser l’upload',
        start_label='Lancer l’upload S3',
        stop_label='Stop job upload actif',
        collector=collect_upload_candidates,
        runner_builder=build_upload_runner,
    ),
    URL_MIGRATION_JOB_FAMILY: JobOperationDefinition(
        job_family=URL_MIGRATION_JOB_FAMILY,
        route_slug='url-migration-jobs',
        source='migration',
        label='Migrations d’URL',
        metric_label='Migrés',
        history_title='Jobs de migration',
        preview_label='Prévisualiser la migration',
        start_label='Lancer la migration',
        stop_label='Stop job migration actif',
        collector=collect_url_migration_changes,
        runner_builder=build_url_migration_runner,
        summary_builder=summarize_url_migration,
    ),
    STATE_CLEANUP_JOB_FAMILY: JobOperationDefinition(
        job_family=STATE_CLEANUP_JOB_FAMILY,
        route_slug='state-cleanup-jobs',
        source='cleanup',
        label='Cleanup état S3',
        metric_label='Nettoyés',
        history_title='Jobs de cleanup',
        preview_label='Prévisualiser le cleanup',
        start_label='Lancer le cleanup',
        stop_label='Stop job cleanup actif',
        collector=collect_state_cleanup_changes,
        runner_builder=build_state_cleanup_runner,
        summary_builder=summarize_state_cleanup,
    ),
}

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Callable

try:
    import boto3
    from botocore.config import Config
except Exception:  # pragma: no cover
    boto3 = None
    Config = None

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / 'catalog.db'
ENV_PATH = ROOT / '.env'


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding='utf-8', errors='replace').splitlines():
        line = raw.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip().strip('"').strip("'")


def current_bucket() -> str:
    return os.getenv('AWS_BUCKET', '').strip()


def current_public_url() -> str:
    return os.getenv('AWS_URL', '').strip()


def current_endpoint_url() -> str:
    return os.getenv('AWS_ENDPOINT_URL', '').strip()


def current_region() -> str | None:
    endpoint = current_endpoint_url().lower()
    explicit = (os.getenv('AWS_REGION') or os.getenv('AWS_DEFAULT_REGION') or '').strip()
    if explicit:
        return explicit
    if 'r2.cloudflarestorage.com' in endpoint:
        return 'auto'
    return 'us-east-1' if endpoint else None


def make_object_exists_checker() -> Callable[[str, str], bool] | None:
    if boto3 is None:
        return None
    env_access_key = os.getenv('AWS_ACCESS_KEY_ID') or os.getenv('AWS_ACCESS_KEY')
    env_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY') or os.getenv('AWS_SECRET_KEY')
    endpoint_url = current_endpoint_url() or None
    env_session_token = os.getenv('AWS_SESSION_TOKEN')
    if endpoint_url and 'r2.cloudflarestorage.com' in endpoint_url.lower():
        env_session_token = None
    session = boto3.session.Session(
        aws_access_key_id=env_access_key or None,
        aws_secret_access_key=env_secret_key or None,
        aws_session_token=env_session_token or None,
        region_name=current_region(),
    )
    client_kwargs = {'endpoint_url': endpoint_url}
    if endpoint_url and Config is not None:
        client_kwargs['config'] = Config(s3={'addressing_style': 'path'})
    s3 = session.client('s3', **client_kwargs)

    def object_exists(bucket: str, key: str) -> bool:
        if not bucket or not key:
            return False
        try:
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except Exception:
            return False

    return object_exists


def resolve_object_key(row: sqlite3.Row | dict) -> str:
    object_key = str(row['object_key'] or '').strip()
    if object_key:
        return object_key
    s3_url = str(row['s3_url'] or '').strip()
    if s3_url.startswith('s3://'):
        _, rest = s3_url.split('s3://', 1)
        parts = rest.split('/', 1)
        if len(parts) == 2:
            return parts[1].strip()
    return ''


def collect_stale_rows(conn: sqlite3.Connection, *, bucket: str | None = None, object_exists: Callable[[str, str], bool] | None = None) -> list[dict]:
    effective_bucket = (bucket if bucket is not None else current_bucket()).strip()
    rows = conn.execute(
        "SELECT goods_id, dataset_id, product_id, source_url, s3_url, bucket, object_key, source_image_count, s3_image_count, failed_image_count, saved_on_s3 FROM s3_objects ORDER BY goods_id"
    ).fetchall()
    stale: list[dict] = []
    checker = object_exists if callable(object_exists) else make_object_exists_checker()
    for row in rows:
        saved_on_s3 = bool(row['saved_on_s3'])
        row_bucket = str(row['bucket'] or '').strip()
        object_key = resolve_object_key(row)
        if not saved_on_s3:
            continue
        reason = None
        if not effective_bucket:
            reason = 'saved_on_s3_without_current_bucket'
        elif row_bucket and row_bucket != effective_bucket:
            reason = 'bucket_mismatch'
        elif object_key and callable(checker) and not checker(effective_bucket, object_key):
            reason = 'object_missing_or_inaccessible'
        elif not object_key:
            reason = 'missing_object_key'
        if not reason:
            continue
        stale.append({
            'goods_id': row['goods_id'],
            'dataset_id': row['dataset_id'],
            'product_id': row['product_id'],
            'source_url': row['source_url'],
            's3_url': row['s3_url'],
            'bucket': row_bucket or None,
            'current_bucket': effective_bucket or None,
            'object_key': object_key or None,
            'source_image_count': int(row['source_image_count'] or 0),
            's3_image_count': int(row['s3_image_count'] or 0),
            'failed_image_count': int(row['failed_image_count'] or 0),
            'reason': reason,
        })
    return stale


def create_backup_payload(conn: sqlite3.Connection) -> list[dict]:
    return [dict(r) for r in conn.execute("SELECT * FROM s3_objects ORDER BY goods_id").fetchall()]


def write_backup(conn: sqlite3.Connection, path: Path) -> Path:
    payload = create_backup_payload(conn)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return path


def apply_cleanup(
    conn: sqlite3.Connection,
    rows: list[dict],
    *,
    updated_at: float | None = None,
    progress_cb: Callable[[dict], None] | None = None,
) -> int:
    now = updated_at if updated_at is not None else time.time()
    updated = 0
    for row in rows:
        conn.execute(
            "UPDATE s3_objects SET s3_url = NULL, s3_image_urls_json = '[]', image_pairs_json = '[]', s3_image_count = 0, failed_image_count = 0, saved_on_s3 = 0, saved_at = NULL, updated_at = ? WHERE goods_id = ?",
            (now, row['goods_id']),
        )
        updated += 1
        if callable(progress_cb):
            progress_cb(row)
    conn.commit()
    return updated


def main() -> int:
    load_env(ENV_PATH)
    parser = argparse.ArgumentParser()
    parser.add_argument('--preview', action='store_true')
    parser.add_argument('--limit', type=int, default=10)
    parser.add_argument('--db-path', default=str(DB_PATH))
    parser.add_argument('--backup-path', default='')
    args = parser.parse_args()

    db_path = Path(args.db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    stale = collect_stale_rows(conn)

    if args.preview:
        print(json.dumps({'count': len(stale), 'preview': stale[: args.limit], 'current_bucket': current_bucket() or None}, ensure_ascii=False, indent=2))
        conn.close()
        return 0

    backup_path = Path(args.backup_path) if args.backup_path else ROOT / f"s3_objects_cleanup_backup_{int(time.time())}.json"
    write_backup(conn, backup_path)
    updated = apply_cleanup(conn, stale)
    conn.close()
    print(json.dumps({'updated': updated, 'backup': str(backup_path), 'current_bucket': current_bucket() or None}, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Callable

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


def aws_url() -> str:
    return os.getenv('AWS_URL', '').strip()


def to_public_url(value: str | None, public_base: str | None = None) -> str | None:
    if not value or not isinstance(value, str):
        return value
    value = value.strip()
    if not value.startswith('s3://'):
        return value
    public = (public_base if public_base is not None else aws_url()).rstrip('/')
    if not public:
        raise RuntimeError('AWS_URL is required for migration')
    try:
        _, rest = value.split('s3://', 1)
        _bucket, key = rest.split('/', 1)
    except Exception:
        return value
    return f'{public}/{key}'


def collect_changes(conn: sqlite3.Connection, public_base: str | None = None) -> list[dict]:
    public_base = (public_base if public_base is not None else aws_url()).strip()
    rows = conn.execute(
        "SELECT goods_id, s3_url, s3_image_urls_json, image_pairs_json FROM s3_objects ORDER BY goods_id"
    ).fetchall()

    changed: list[dict] = []
    for row in rows:
        old_s3 = row['s3_url']
        new_s3 = to_public_url(old_s3, public_base)
        old_urls = json.loads(row['s3_image_urls_json'] or '[]')
        new_urls = [to_public_url(v, public_base) for v in old_urls]
        old_pairs = json.loads(row['image_pairs_json'] or '[]')
        new_pairs = []
        changed_fields: list[str] = []
        for pair in old_pairs:
            if isinstance(pair, dict):
                pair = dict(pair)
                original_pair_s3 = pair.get('s3_url')
                pair['s3_url'] = to_public_url(pair.get('s3_url'), public_base)
                if pair.get('s3_url') != original_pair_s3 and 'image_pairs_json' not in changed_fields:
                    changed_fields.append('image_pairs_json')
            new_pairs.append(pair)
        if new_s3 != old_s3:
            changed_fields.append('s3_url')
        if new_urls != old_urls:
            changed_fields.append('s3_image_urls_json')
        if new_pairs != old_pairs and 'image_pairs_json' not in changed_fields:
            changed_fields.append('image_pairs_json')
        if changed_fields:
            changed.append({
                'goods_id': row['goods_id'],
                'old_s3_url': old_s3,
                'new_s3_url': new_s3,
                'old_urls': old_urls,
                'new_urls': new_urls,
                'old_pairs': old_pairs,
                'new_pairs': new_pairs,
                'changed_fields': changed_fields,
            })
    return changed


def create_backup_payload(conn: sqlite3.Connection) -> list[dict]:
    return [dict(r) for r in conn.execute("SELECT * FROM s3_objects ORDER BY goods_id").fetchall()]


def write_backup(conn: sqlite3.Connection, path: Path) -> Path:
    payload = create_backup_payload(conn)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return path


def apply_changes(
    conn: sqlite3.Connection,
    changes: list[dict],
    *,
    updated_at: float | None = None,
    progress_cb: Callable[[dict], None] | None = None,
) -> int:
    now = updated_at if updated_at is not None else time.time()
    updated = 0
    for change in changes:
        conn.execute(
            "UPDATE s3_objects SET s3_url = ?, s3_image_urls_json = ?, image_pairs_json = ?, updated_at = ? WHERE goods_id = ?",
            (
                change['new_s3_url'],
                json.dumps(change['new_urls'], ensure_ascii=False),
                json.dumps(change['new_pairs'], ensure_ascii=False),
                now,
                change['goods_id'],
            ),
        )
        updated += 1
        if callable(progress_cb):
            progress_cb(change)
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

    public_base = aws_url()
    changes = collect_changes(conn, public_base)

    if args.preview:
        print(json.dumps({'count': len(changes), 'preview': changes[: args.limit]}, ensure_ascii=False, indent=2))
        return 0

    backup_path = Path(args.backup_path) if args.backup_path else ROOT / f"s3_objects_backup_{int(time.time())}.json"
    write_backup(conn, backup_path)
    updated = apply_changes(conn, changes)
    conn.close()
    print(json.dumps({'updated': updated, 'backup': str(backup_path)}, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

from __future__ import annotations

import sqlite3


def db_connect(db_path, *, sql_unaccent_fn, migrate_legacy_state, migrate_legacy_jobs):
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA busy_timeout = 30000')
    try:
        conn.create_function('unaccent', 1, sql_unaccent_fn)
    except Exception:
        pass
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS image_status (
            dataset_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            image_url TEXT,
            ok INTEGER NOT NULL,
            status_code INTEGER,
            content_type TEXT,
            checked_at REAL NOT NULL,
            PRIMARY KEY (dataset_id, product_id)
        )
        '''
    )
    conn.execute('CREATE INDEX IF NOT EXISTS idx_image_status_dataset_ok ON image_status(dataset_id, ok)')
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS s3_config (
            config_key TEXT PRIMARY KEY,
            config_value TEXT NOT NULL,
            updated_at REAL NOT NULL
        )
        '''
    )
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS s3_objects (
            goods_id TEXT PRIMARY KEY,
            dataset_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            source_url TEXT,
            s3_url TEXT,
            bucket TEXT,
            object_key TEXT,
            source_image_urls_json TEXT NOT NULL DEFAULT '[]',
            s3_image_urls_json TEXT NOT NULL DEFAULT '[]',
            image_pairs_json TEXT NOT NULL DEFAULT '[]',
            source_image_count INTEGER NOT NULL DEFAULT 0,
            s3_image_count INTEGER NOT NULL DEFAULT 0,
            failed_image_count INTEGER NOT NULL DEFAULT 0,
            saved_on_s3 INTEGER NOT NULL DEFAULT 0,
            saved_at REAL,
            updated_at REAL NOT NULL
        )
        '''
    )
    conn.execute('CREATE INDEX IF NOT EXISTS idx_s3_objects_dataset_product ON s3_objects(dataset_id, product_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_s3_objects_saved ON s3_objects(saved_on_s3)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_s3_objects_dataset_saved_product ON s3_objects(dataset_id, saved_on_s3, product_id)')
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS s3_jobs (
            job_id TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            started_at REAL,
            updated_at REAL NOT NULL
        )
        '''
    )
    conn.execute('CREATE INDEX IF NOT EXISTS idx_s3_jobs_started_at ON s3_jobs(started_at DESC)')
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS s3_job_claims (
            dataset_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            job_id TEXT NOT NULL,
            claimed_at REAL NOT NULL,
            PRIMARY KEY (dataset_id, product_id)
        )
        '''
    )
    conn.execute('CREATE INDEX IF NOT EXISTS idx_s3_job_claims_job_id ON s3_job_claims(job_id)')
    migrate_legacy_state(conn)
    migrate_legacy_jobs(conn)
    conn.commit()
    return conn


def health_status(db_path):
    status = {
        'ok': False,
        'db_path': str(db_path),
        'db_exists': db_path.exists(),
        'datasets_count': 0,
    }
    if not status['db_exists']:
        return status
    try:
        conn = sqlite3.connect(db_path)
        try:
            row = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='datasets'").fetchone()
            has_datasets_table = bool(row and row[0])
            status['has_datasets_table'] = has_datasets_table
            if not has_datasets_table:
                return status
            count_row = conn.execute('SELECT COUNT(*) FROM datasets').fetchone()
            status['datasets_count'] = int(count_row[0] or 0) if count_row else 0
            status['ok'] = status['datasets_count'] > 0
            return status
        finally:
            conn.close()
    except Exception as exc:
        status['error'] = str(exc)
        return status


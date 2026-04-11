#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path

try:
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
except Exception as exc:  # pragma: no cover - dependency is already present in this workspace
    print(f'ERROR: boto3 is required: {exc}', file=sys.stderr)
    raise SystemExit(2)


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding='utf-8').splitlines():
        line = raw.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, value = line.split('=', 1)
        key = key.strip()
        if not key or key in os.environ:
            continue
        os.environ[key] = value.strip().strip('"').strip("'")


def env(name: str, default: str = '') -> str:
    return os.getenv(name, default).strip()


def resolve_region(endpoint_url: str, explicit_region: str) -> str | None:
    region = explicit_region.strip()
    if region:
        return region
    endpoint = endpoint_url.lower().strip()
    if 'r2.cloudflarestorage.com' in endpoint:
        return 'auto'
    return env('AWS_REGION') or env('AWS_DEFAULT_REGION') or 'us-east-1'


def build_client(endpoint_url: str, region_name: str):
    session_token = env('AWS_SESSION_TOKEN') or None
    if endpoint_url and 'r2.cloudflarestorage.com' in endpoint_url.lower():
        session_token = None
    session = boto3.session.Session(
        aws_access_key_id=env('AWS_ACCESS_KEY_ID') or env('AWS_ACCESS_KEY') or None,
        aws_secret_access_key=env('AWS_SECRET_ACCESS_KEY') or env('AWS_SECRET_KEY') or None,
        aws_session_token=session_token,
        region_name=resolve_region(endpoint_url, region_name),
    )
    client_kwargs = {'endpoint_url': endpoint_url or None}
    if endpoint_url:
        client_kwargs['config'] = Config(s3={'addressing_style': 'path'})
    return session.client('s3', **client_kwargs)


def bucket_region_args(region_name: str):
    region = resolve_region('', region_name) or 'us-east-1'
    if region == 'us-east-1':
        return {}
    return {'CreateBucketConfiguration': {'LocationConstraint': region}}


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    load_env_file(repo_root / '.env')

    parser = argparse.ArgumentParser(description='Smoke test S3-compatible operations against Cloudflare R2 or AWS S3.')
    parser.add_argument('--bucket', default=env('S3_BUCKET') or env('FAST_FASHION_S3_BUCKET'), help='Target bucket name (defaults to S3_BUCKET from env)')
    parser.add_argument('--endpoint-url', default=env('S3_ENDPOINT_URL') or env('AWS_ENDPOINT_URL'), help='S3 endpoint URL (defaults to S3_ENDPOINT_URL from env)')
    parser.add_argument('--region', default=env('AWS_REGION') or env('AWS_DEFAULT_REGION') or env('S3_REGION'), help='Region name (defaults to env, or auto for R2)')
    parser.add_argument('--prefix', default=f'openclaw-smoke/{time.strftime("%Y%m%d-%H%M%S")}', help='Object prefix used for the smoke test')
    parser.add_argument('--keep-object', action='store_true', help='Keep the uploaded object instead of deleting it at the end')
    parser.add_argument('--json', action='store_true', help='Print a JSON summary instead of human-readable logs')
    args = parser.parse_args()

    missing = []
    if not args.bucket:
        missing.append('S3_BUCKET')
    if not args.endpoint_url:
        missing.append('S3_ENDPOINT_URL')
    if not (env('AWS_ACCESS_KEY_ID') or env('AWS_ACCESS_KEY')):
        missing.append('AWS_ACCESS_KEY_ID')
    if not (env('AWS_SECRET_ACCESS_KEY') or env('AWS_SECRET_KEY')):
        missing.append('AWS_SECRET_ACCESS_KEY')
    if missing:
        print(f'ERROR: Missing required env vars: {", ".join(missing)}', file=sys.stderr)
        return 2

    client = build_client(args.endpoint_url, args.region)
    prefix = args.prefix.strip('/ ')
    object_key = f'{prefix}/{uuid.uuid4().hex[:12]}.txt'
    payload = f'openclaw smoke test {time.time():.0f}\n'.encode('utf-8')

    summary = {
        'bucket': args.bucket,
        'endpoint_url': args.endpoint_url,
        'region': resolve_region(args.endpoint_url, args.region),
        'prefix': prefix,
        'object_key': object_key,
        'steps': [],
    }

    def step(name: str, fn, summarize=None):
        start = time.time()
        result = fn()
        rendered = summarize(result) if callable(summarize) else result
        summary['steps'].append({'name': name, 'seconds': round(time.time() - start, 3), 'result': rendered})
        return result

    try:
        step('head_bucket', lambda: client.head_bucket(Bucket=args.bucket), lambda _data: {'ok': True})
        list_before = step(
            'list_objects_v2_before',
            lambda: client.list_objects_v2(Bucket=args.bucket, Prefix=prefix, MaxKeys=5),
            lambda data: {
                'key_count': int(data.get('KeyCount', 0) or 0),
                'sample_keys': [item.get('Key') for item in (data.get('Contents') or [])[:3]],
            },
        )
        step(
            'put_object',
            lambda: client.put_object(Bucket=args.bucket, Key=object_key, Body=payload, ContentType='text/plain'),
            lambda data: {'etag': data.get('ETag')},
        )
        head = step(
            'head_object',
            lambda: client.head_object(Bucket=args.bucket, Key=object_key),
            lambda data: {'etag': data.get('ETag'), 'content_type': data.get('ContentType')},
        )
        listed = step(
            'list_objects_v2_after',
            lambda: client.list_objects_v2(Bucket=args.bucket, Prefix=object_key, MaxKeys=5),
            lambda data: {
                'key_count': int(data.get('KeyCount', 0) or 0),
                'sample_keys': [item.get('Key') for item in (data.get('Contents') or [])[:3]],
            },
        )
        if not args.keep_object:
            step('delete_object', lambda: client.delete_object(Bucket=args.bucket, Key=object_key), lambda _data: {'ok': True})
            try:
                client.head_object(Bucket=args.bucket, Key=object_key)
                raise RuntimeError('Object still exists after delete_object')
            except ClientError as exc:
                code = exc.response.get('Error', {}).get('Code', '')
                if code not in {'404', 'NoSuchKey', 'NotFound'}:
                    raise
        summary['verification'] = {
            'list_before_keys': int(list_before.get('KeyCount', 0) or 0),
            'etag': head.get('ETag'),
            'list_after_keys': int(listed.get('KeyCount', 0) or 0),
            'deleted': not args.keep_object,
        }
    except ClientError as exc:
        err = exc.response.get('Error', {})
        summary['error'] = {
            'type': 'ClientError',
            'code': err.get('Code'),
            'message': err.get('Message') or str(exc),
        }
        if args.json:
            print(json.dumps(summary, ensure_ascii=False, indent=2))
        else:
            print(f"ERROR: {summary['error']['code']}: {summary['error']['message']}", file=sys.stderr)
        return 1
    except Exception as exc:
        summary['error'] = {'type': type(exc).__name__, 'message': str(exc)}
        if args.json:
            print(json.dumps(summary, ensure_ascii=False, indent=2))
        else:
            print(f'ERROR: {type(exc).__name__}: {exc}', file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        print(f"OK: bucket={args.bucket} endpoint={args.endpoint_url} region={summary['region']}")
        for item in summary['steps']:
            print(f"- {item['name']}: {item['seconds']}s")
        print(f"- object: {object_key}")
        print(f"- deleted: {not args.keep_object}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

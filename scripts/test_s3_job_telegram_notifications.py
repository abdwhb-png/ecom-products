#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import s3_jobs  # noqa: E402
from s3_jobs import S3JobManager  # noqa: E402


class FakeS3:
    def __init__(self):
        self.objects: dict[str, dict] = {}

    def head_object(self, Bucket: str, Key: str):
        if Key not in self.objects:
            raise RuntimeError('NotFound')
        return self.objects[Key]

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str, Metadata: dict):
        self.objects[Key] = {
            'Bucket': Bucket,
            'Key': Key,
            'Body': Body,
            'ContentType': ContentType,
            'Metadata': Metadata,
        }
        return {'ETag': 'fake'}


def resolve_source_url(_row):
    return ['https://example.test/main.jpg']


def main() -> int:
    previous_env = {key: os.environ.get(key) for key in ['TELEGRAM_BOT_TOKEN', 'TELEGRAM_CHAT_ID']}
    original_boto3 = s3_jobs.boto3
    try:
        os.environ['TELEGRAM_BOT_TOKEN'] = 'unit-test-token'
        os.environ['TELEGRAM_CHAT_ID'] = '123456'
        s3_jobs.boto3 = object()

        with tempfile.TemporaryDirectory() as tmpdir:
            manager = S3JobManager(store_path=Path(tmpdir) / 'jobs.json')
            fake_s3 = FakeS3()
            sent_notifications: list[dict] = []

            def fake_notify(job: dict):
                sent_notifications.append(job)

            manager._send_terminal_notification = fake_notify  # type: ignore[method-assign]
            manager._download = lambda url, dataset_id=None: (b'img-bytes', 'image/jpeg', {'hostname': 'example.test', 'proxy_used': False, 'proxy_mode': 'direct', 'attempt_count': 1, 'timeout_seconds': 20, 'error_type': None, 'http_status': None})  # type: ignore[method-assign]

            future = manager.start_job(
                job_id='notify-upload-1',
                dataset_id='shein',
                source='products',
                bucket='unit-test-bucket',
                prefix='notify',
                limit=10,
                concurrency=1,
                rows=[{'id': 'product-1', 'name': 'Notify upload product', 'image': 'https://example.test/main.jpg', 'image_urls_json': json.dumps(['https://example.test/main.jpg'])}],
                s3_client_factory=lambda: fake_s3,
                resolve_source_url=resolve_source_url,
            )
            future.result(timeout=10)
            assert len(sent_notifications) == 1, sent_notifications
            assert sent_notifications[0]['job_id'] == 'notify-upload-1', sent_notifications
            assert sent_notifications[0]['status'] == 'completed', sent_notifications

            def failing_runner(record_item, cancel_event):
                raise RuntimeError('boom-notify')

            custom_future = manager.start_custom_job(
                job_id='notify-cleanup-1',
                dataset_id='all',
                source='cleanup',
                total=1,
                runner=failing_runner,
                job_family='state_cleanup',
                kind='cleanup',
            )
            custom_future.result(timeout=10)
            assert len(sent_notifications) == 2, sent_notifications
            assert sent_notifications[1]['job_id'] == 'notify-cleanup-1', sent_notifications
            assert sent_notifications[1]['status'] == 'failed', sent_notifications
            assert 'boom-notify' in (sent_notifications[1].get('error') or sent_notifications[1].get('last_message') or ''), sent_notifications

            del os.environ['TELEGRAM_BOT_TOKEN']
            notification_attempts: list[dict] = []
            original_notifications_enabled = manager._notifications_enabled
            original_send_terminal_notification = manager._send_terminal_notification

            def fake_notifications_enabled():
                return False

            def fake_send_terminal_notification(job: dict):
                notification_attempts.append(job)
                return original_send_terminal_notification(job)

            manager._notifications_enabled = fake_notifications_enabled  # type: ignore[assignment]
            manager._send_terminal_notification = fake_send_terminal_notification  # type: ignore[assignment]
            future_without_env = manager.start_job(
                job_id='notify-upload-2',
                dataset_id='shein',
                source='products',
                bucket='unit-test-bucket',
                prefix='notify',
                limit=10,
                concurrency=1,
                rows=[{'id': 'product-2', 'name': 'Notify upload product 2', 'image': 'https://example.test/main.jpg', 'image_urls_json': json.dumps(['https://example.test/main.jpg'])}],
                s3_client_factory=lambda: fake_s3,
                resolve_source_url=resolve_source_url,
            )
            future_without_env.result(timeout=10)
            assert len(notification_attempts) == 1, notification_attempts
            assert manager._notifications_enabled() is False
            manager._notifications_enabled = original_notifications_enabled  # type: ignore[assignment]
            manager._send_terminal_notification = original_send_terminal_notification  # type: ignore[assignment]

            os.environ['TELEGRAM_BOT_TOKEN'] = 'unit-test-token'
            os.environ['TELEGRAM_CHAT_ID'] = '123456'
            manager._send_terminal_notification = lambda job: (_ for _ in ()).throw(RuntimeError('telegram-down'))  # type: ignore[assignment]
            future_best_effort = manager.start_job(
                job_id='notify-upload-3',
                dataset_id='shein',
                source='products',
                bucket='unit-test-bucket',
                prefix='notify',
                limit=10,
                concurrency=1,
                rows=[{'id': 'product-3', 'name': 'Notify upload product 3', 'image': 'https://example.test/main.jpg', 'image_urls_json': json.dumps(['https://example.test/main.jpg'])}],
                s3_client_factory=lambda: fake_s3,
                resolve_source_url=resolve_source_url,
            )
            future_best_effort.result(timeout=10)
            resilient_job = manager.get_job('notify-upload-3')
            assert resilient_job is not None, manager.list_jobs()
            assert resilient_job['status'] == 'completed', resilient_job
    finally:
        s3_jobs.boto3 = original_boto3
        for key, value in previous_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    print('OK: terminal S3 jobs send Telegram notifications only when TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are configured')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

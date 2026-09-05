from __future__ import annotations

import secrets
from datetime import UTC, datetime

from celery import Celery
from celery.exceptions import Retry
from celery.schedules import crontab
from redis import Redis
from redis.exceptions import RedisError

from etf_sentinel.config import get_settings
from etf_sentinel.database import SessionLocal
from etf_sentinel.services.monitoring import record_monitoring_check
from etf_sentinel.services.pipeline import run_demo_pipeline

settings = get_settings()
celery_app = Celery("etf_sentinel", broker=settings.redis_url, backend=settings.redis_url)
celery_app.conf.update(
    timezone="Asia/Shanghai",
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_track_started=True,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    beat_schedule={
        "daily-close-research-pipeline": {
            "task": "etf_sentinel.daily_pipeline",
            "schedule": crontab(hour=16, minute=30, day_of_week="1-5"),
        },
        "hourly-risk-monitor": {
            "task": "etf_sentinel.hourly_monitor",
            "schedule": crontab(hour=f"*/{settings.monitoring_interval_hours}", minute=0),
            "options": {"expires": 300},
        },
    },
)


@celery_app.task(
    bind=True,
    name="etf_sentinel.hourly_monitor",
    max_retries=3,
    soft_time_limit=90,
    time_limit=120,
)
def hourly_monitor(self) -> dict[str, object]:
    # Shared projection, no HTTP self-call and no provider/ledger mutation.
    from etf_sentinel.main import build_monitoring_report

    redis_client = Redis.from_url(
        settings.redis_url, decode_responses=True, socket_connect_timeout=3, socket_timeout=3
    )
    token = secrets.token_hex(16)
    lock_key = "etf-sentinel:lock:hourly-monitor"
    acquired = False
    try:
        acquired = redis_client.set(lock_key, token, nx=True, ex=180)
        if not acquired:
            # A surviving lease can belong to a lost worker, not a completed job.
            # Give healthy duplicates a short retry, then allow the old lease to expire.
            ttl = redis_client.ttl(lock_key)
            delay = 5 if self.request.retries == 0 else max(1, min(181, ttl + 1))
            raise self.retry(exc=RuntimeError("MONITORING_LEASE_BUSY"), countdown=delay)
        with SessionLocal() as session:
            return record_monitoring_check(
                session,
                settings,
                now=datetime.now(UTC),
                build_report=lambda: build_monitoring_report(session),
            )
    except Retry:
        raise
    except Exception:
        # Suppress provider/DB exception text in Celery logs and result backend.
        raise self.retry(
            exc=RuntimeError("MONITORING_CHECK_FAILED"),
            countdown=min(120, 10 * 2**self.request.retries),
        ) from None
    finally:
        if acquired:
            try:
                redis_client.eval(
                    "if redis.call('get', KEYS[1]) == ARGV[1] then "
                    "return redis.call('del', KEYS[1]) else return 0 end",
                    1,
                    lock_key,
                    token,
                )
            except RedisError:
                pass  # Lease expires; do not mask the recorded task outcome.
        redis_client.close()


@celery_app.task(
    bind=True,
    name="etf_sentinel.daily_pipeline",
    autoretry_for=(ConnectionError, TimeoutError),
    retry_backoff=True,
    retry_jitter=True,
    max_retries=3,
    soft_time_limit=600,
    time_limit=660,
)
def daily_pipeline(self) -> dict[str, object]:
    del self
    date_key = settings.demo_evaluation_time.astimezone(UTC).date().isoformat()
    lock_key = f"etf-sentinel:lock:daily:{date_key}"
    token = secrets.token_hex(16)
    redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    acquired = redis_client.set(lock_key, token, nx=True, ex=720)
    if not acquired:
        return {"status": "SKIPPED_DUPLICATE", "idempotency_key": date_key}
    try:
        with SessionLocal() as session:
            result = run_demo_pipeline(session, settings)
        if result.get("task_status") == "RUNNING":
            return {"status": "SKIPPED_DUPLICATE", **result}
        return {"status": "SUCCEEDED", **result}
    finally:
        redis_client.eval(
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
            "return redis.call('del', KEYS[1]) else return 0 end",
            1,
            lock_key,
            token,
        )

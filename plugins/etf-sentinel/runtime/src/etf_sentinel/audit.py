from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session, aliased

from etf_sentinel.models import AuditLog

SENSITIVE_KEYS = {
    "authorization",
    "cookie",
    "password",
    "secret",
    "token",
    "api_key",
    "account_number",
}


def sanitize_details(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): "[REDACTED]" if str(key).lower() in SENSITIVE_KEYS else sanitize_details(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [sanitize_details(item) for item in value]
    return value


def append_audit(
    session: Session,
    *,
    event_type: str,
    object_type: str,
    object_id: str | None = None,
    actor: str = "system",
    trace_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> AuditLog:
    if session.get_bind().dialect.name == "postgresql":
        # All writers (hourly/daily/manual acknowledgement) serialize the chain tail
        # until commit or rollback, including the empty-chain bootstrap case.
        session.execute(text("SELECT pg_advisory_xact_lock(453947192614)"))
    successor = aliased(AuditLog)
    heads = list(
        session.scalars(
            select(AuditLog)
            .where(
                ~select(successor.id)
                .where(successor.previous_hash == AuditLog.record_hash)
                .exists()
            )
            .limit(2)
        )
    )
    if len(heads) > 1 or (not heads and session.scalar(select(AuditLog.id).limit(1)) is not None):
        raise ValueError("审计链头缺失或存在分叉，停止追加并等待完整性复核。")
    previous = heads[0] if heads else None
    clean_details = sanitize_details(details or {})
    payload = {
        "event_type": event_type,
        "object_type": object_type,
        "object_id": object_id,
        "actor": actor,
        "trace_id": trace_id or str(uuid.uuid4()),
        "details": clean_details,
        "previous_hash": previous.record_hash if previous else None,
    }
    record_hash = hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode()
    ).hexdigest()
    row = AuditLog(record_hash=record_hash, **payload)
    session.add(row)
    session.flush()
    return row

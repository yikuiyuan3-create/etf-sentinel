from __future__ import annotations

import hmac
from datetime import UTC, datetime

from etf_sentinel.models import NewsEvent, ScheduledEvent
from etf_sentinel.providers.base import content_hash


def _timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    aware = value if value.tzinfo else value.replace(tzinfo=UTC)
    return aware.astimezone(UTC).isoformat()


def news_event_record_payload(event: NewsEvent) -> dict[str, object]:
    """Canonical immutable news fact, excluding database identity and its own hash."""
    return {
        "provider_code": event.provider_code,
        "normalized_url": event.normalized_url,
        "source_uri": event.source_uri,
        "title": event.title,
        "short_summary": event.short_summary,
        "source_name": event.source_name,
        "event_time": _timestamp(event.event_time),
        "published_at": _timestamp(event.published_at),
        "first_seen_at": _timestamp(event.first_seen_at),
        "available_at": _timestamp(event.available_at),
        "as_of": _timestamp(event.as_of),
        "ingested_at": _timestamp(event.ingested_at),
        "revision": event.revision,
        "timezone": event.timezone,
        "currency": event.currency,
        "latency_class": event.latency_class,
        "license_scope": event.license_scope,
        "data_mode": event.data_mode,
        "quality_flags": sorted(event.quality_flags or []),
        "raw_payload_hash": event.raw_payload_hash,
        "dedupe_hash": event.dedupe_hash,
        "cluster_key": event.cluster_key,
        "entities": event.entities,
        "exposures": event.exposures,
        "relevance": event.relevance,
        "direction": event.direction,
        "severity": event.severity,
        "novelty": event.novelty,
        "source_grade": event.source_grade,
    }


def news_event_record_hash(event: NewsEvent) -> str:
    return content_hash(news_event_record_payload(event))


def verify_news_event_record(event: NewsEvent) -> bool:
    return isinstance(event.record_hash, str) and hmac.compare_digest(
        event.record_hash, news_event_record_hash(event)
    )


def scheduled_event_record_payload(event: ScheduledEvent) -> dict[str, object]:
    """Canonical calendar fact including mutable revision-governance state."""
    return {
        "event_code": event.event_code,
        "name_zh": event.name_zh,
        "event_type": event.event_type,
        "scheduled_at": _timestamp(event.scheduled_at),
        "event_time": _timestamp(event.event_time),
        "published_at": _timestamp(event.published_at),
        "first_announced_at": _timestamp(event.first_announced_at),
        "first_seen_at": _timestamp(event.first_seen_at),
        "available_at": _timestamp(event.available_at),
        "as_of": _timestamp(event.as_of),
        "ingested_at": _timestamp(event.ingested_at),
        "revision": event.revision,
        "timezone": event.timezone,
        "currency": event.currency,
        "latency_class": event.latency_class,
        "alert_lead_hours": event.alert_lead_hours,
        "regions": event.regions,
        "asset_classes": event.asset_classes,
        "source_uri": event.source_uri,
        "provider_code": event.provider_code,
        "data_mode": event.data_mode,
        "license_scope": event.license_scope,
        "quality_flags": sorted(event.quality_flags or []),
        "raw_payload_hash": event.raw_payload_hash,
        "status": event.status,
        "is_current": event.is_current,
    }


def scheduled_event_record_hash(event: ScheduledEvent) -> str:
    return content_hash(scheduled_event_record_payload(event))


def verify_scheduled_event_record(event: ScheduledEvent) -> bool:
    return isinstance(event.record_hash, str) and hmac.compare_digest(
        event.record_hash, scheduled_event_record_hash(event)
    )

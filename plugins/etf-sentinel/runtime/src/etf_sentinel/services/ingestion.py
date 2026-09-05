from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from pandas.api.types import is_object_dtype
from sqlalchemy import select
from sqlalchemy.orm import Session

from etf_sentinel.audit import append_audit
from etf_sentinel.config import Settings
from etf_sentinel.enums import DataMode, LicenseStatus
from etf_sentinel.models import (
    DataSnapshot,
    EtfInstrument,
    NewsEvent,
    ProviderRegistry,
    ScheduledEvent,
)
from etf_sentinel.providers.base import (
    authorize_provider,
    content_hash,
    normalize_url,
    validate_fact_frame,
)
from etf_sentinel.providers.demo import DemoFixtureProvider, default_demo_identifiers
from etf_sentinel.services.fact_integrity import (
    news_event_record_hash,
    scheduled_event_record_hash,
    verify_news_event_record,
    verify_scheduled_event_record,
)

FACT_TIME_COLUMNS = [
    "event_time",
    "published_at",
    "first_seen_at",
    "available_at",
    "as_of",
    "ingested_at",
]


class SnapshotIntegrityError(RuntimeError):
    pass


def canonical_frame_hash(frame: pd.DataFrame) -> str:
    canonical = frame.copy()
    sort_columns = [
        column
        for column in ["instrument_id", "series_code", "event_time", "revision"]
        if column in canonical
    ]
    if sort_columns:
        canonical = canonical.sort_values(sort_columns).reset_index(drop=True)
    for column in FACT_TIME_COLUMNS:
        if column in canonical:
            parsed = pd.to_datetime(canonical[column], utc=True, errors="raise")
            canonical[column] = parsed.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    for column in canonical.columns:
        if is_object_dtype(canonical[column].dtype):
            canonical[column] = canonical[column].map(_canonical_cell)
    canonical = canonical[sorted(canonical.columns)]
    payload = canonical.to_csv(index=False, lineterminator="\n", float_format="%.12g")
    return hashlib.sha256(payload.encode()).hexdigest()


def _canonical_cell(value):
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return value


def verify_snapshot_frame(frame: pd.DataFrame, *, expected_hash: str, expected_rows: int) -> None:
    if len(frame) != expected_rows:
        raise SnapshotIntegrityError("快照行数与登记值不一致，已失败关闭。")
    if canonical_frame_hash(frame) != expected_hash:
        raise SnapshotIntegrityError("快照内容哈希与登记值不一致，已失败关闭。")


def register_default_providers(session: Session) -> None:
    definitions: list[dict[str, Any]] = [
        {
            "provider_code": "demo_fixture",
            "display_name": "Deterministic Demo Fixture",
            "purpose": "仅用于内部测试和产品演示",
            "markets": ["XDEM"],
            "regions": ["中国", "全球", "亚太"],
            "display_right": True,
            "non_display_algorithm_right": True,
            "derivative_right": True,
            "cache_right": True,
            "training_right": True,
            "redistribution_right": False,
            "review_status": LicenseStatus.APPROVED.value,
            "reviewed_by": "system-fixture-policy",
            "reviewed_at": datetime(2025, 1, 1, tzinfo=UTC),
            "terms_uri": "internal://demo-fixture-policy",
            "notes": "不对应真实行情，不得用于投资判断。",
        },
        {
            "provider_code": "gdelt_doc_v2",
            "display_name": "GDELT DOC 2.0",
            "purpose": "全球新闻元数据与事件发现",
            "markets": ["GLOBAL_NEWS"],
            "regions": ["GLOBAL"],
            "review_status": LicenseStatus.PENDING.value,
            "terms_uri": "https://www.gdeltproject.org/about.html",
            "notes": "公开可访问不等于已取得企业缓存、派生、训练或再分发权。",
        },
        {
            "provider_code": "twelve_data",
            "display_name": "Twelve Data",
            "purpose": "行情与ETF资料（环境变量启用）",
            "markets": ["PROVIDER_COVERAGE_DEPENDENT"],
            "regions": ["GLOBAL"],
            "review_status": LicenseStatus.PENDING.value,
            "terms_uri": "https://twelvedata.com/terms",
            "notes": "必须按合同套餐逐项确认展示、算法、缓存、派生、训练和再分发权。",
        },
        {
            "provider_code": "cn_commercial_pending",
            "display_name": "中国商业行情（待定）",
            "purpose": "中国ETF行情",
            "markets": ["CN"],
            "regions": ["CN"],
            "review_status": LicenseStatus.BLOCKED.value,
            "notes": "未签署正式数据许可，接口骨架保持禁用。",
        },
        {
            "provider_code": "hk_commercial_pending",
            "display_name": "香港商业行情（待定）",
            "purpose": "香港ETF行情",
            "markets": ["HK"],
            "regions": ["HK"],
            "review_status": LicenseStatus.BLOCKED.value,
            "notes": "未签署正式数据许可，接口骨架保持禁用。",
        },
    ]
    for definition in definitions:
        existing = session.scalar(
            select(ProviderRegistry).where(
                ProviderRegistry.provider_code == definition["provider_code"]
            )
        )
        if existing is None:
            session.add(ProviderRegistry(**definition))
        # Registry rows are seed-only.  A later bootstrap must never overwrite
        # an auditor's suspension, expiry, rights change, or reviewer identity.
    session.flush()


def ingest_demo_snapshot(
    session: Session, settings: Settings, *, commit: bool = True
) -> DataSnapshot:
    register_default_providers(session)
    authorize_provider(
        session,
        "demo_fixture",
        purposes={"display", "algorithm", "derivative", "cache", "training"},
        at=datetime.now(UTC),
    )
    provider = DemoFixtureProvider()
    metadata = provider.fetch_etf_metadata()
    for row in metadata:
        current = session.get(EtfInstrument, row["id"])
        if current is None:
            session.add(EtfInstrument(**row))
    session.flush()

    identifiers = default_demo_identifiers(provider)
    frame = provider.fetch_bars(
        identifiers, start=datetime(2023, 11, 13).date(), end=datetime(2025, 1, 31).date()
    )
    config_payload = {
        "provider": provider.provider_code,
        "start": "2023-11-13",
        "end": "2025-01-31",
        "fixture_version": "v1",
        "instrument_count": len(identifiers),
    }
    config_hash = content_hash(config_payload)
    snapshot_hash = canonical_frame_hash(frame)
    existing = session.scalar(
        select(DataSnapshot).where(DataSnapshot.snapshot_hash == snapshot_hash)
    )
    snapshot_path = settings.snapshot_dir / f"demo-market-{snapshot_hash[:16]}.parquet"
    if not snapshot_path.exists():
        _atomic_parquet_write(frame, snapshot_path)
    persisted_frame = pd.read_parquet(snapshot_path)
    verify_snapshot_frame(
        persisted_frame,
        expected_hash=snapshot_hash,
        expected_rows=len(frame),
    )
    if existing is None:
        existing = DataSnapshot(
            provider_code=provider.provider_code,
            dataset_type="MARKET_BARS",
            source_uri="internal://deterministic-demo-fixture/v1",
            as_of=pd.to_datetime(frame["as_of"], utc=True).max().to_pydatetime(),
            available_at=pd.to_datetime(frame["available_at"], utc=True).max().to_pydatetime(),
            data_mode=DataMode.DEMO_FIXTURE.value,
            license_scope="INTERNAL_TEST_AND_DEMONSTRATION_ONLY",
            parquet_uri=str(snapshot_path.resolve()),
            snapshot_hash=snapshot_hash,
            config_hash=config_hash,
            row_count=len(frame),
            quality_flags=["SYNTHETIC_NOT_MARKET_DATA"],
            revision="fixture-v1",
        )
        session.add(existing)
        session.flush()
        append_audit(
            session,
            event_type="DATA_SNAPSHOT_INGESTED",
            object_type="DataSnapshot",
            object_id=existing.id,
            details={
                "provider": provider.provider_code,
                "license_status": LicenseStatus.APPROVED.value,
                "snapshot_hash": snapshot_hash,
                "row_count": len(frame),
                "data_mode": DataMode.DEMO_FIXTURE.value,
            },
        )
    _ingest_demo_news(session, provider, settings)
    _ingest_demo_events(session, provider, settings)
    _ingest_demo_macro_snapshot(session, provider, settings)
    if commit:
        session.commit()
    else:
        session.flush()
    return existing


def _ingest_demo_macro_snapshot(
    session: Session, provider: DemoFixtureProvider, settings: Settings
) -> DataSnapshot:
    records = provider.fetch_macro(
        ["DEMO_LIQUIDITY", "DEMO_GROWTH", "DEMO_INFLATION"],
        (settings.demo_evaluation_time - pd.Timedelta(days=365)).date(),
        settings.demo_evaluation_time.date(),
    )
    rows = [
        {
            "series_code": record.series_code,
            "value": record.value,
            "provider": record.provider,
            "source_uri": str(record.source_uri),
            "event_time": record.event_time,
            "published_at": record.published_at,
            "first_seen_at": record.first_seen_at,
            "available_at": record.available_at,
            "as_of": record.as_of,
            "ingested_at": record.ingested_at,
            "revision": record.vintage,
            "vintage": record.vintage,
            "timezone": record.timezone,
            "currency": record.currency,
            "latency_class": record.latency_class,
            "license_scope": record.license_scope,
            "data_mode": record.data_mode.value,
            "quality_flags": record.quality_flags,
            "raw_payload_hash": record.raw_payload_hash,
        }
        for record in records
    ]
    frame = pd.DataFrame(rows)
    validate_fact_frame(frame)
    snapshot_hash = canonical_frame_hash(frame)
    existing = session.scalar(
        select(DataSnapshot).where(DataSnapshot.snapshot_hash == snapshot_hash)
    )
    snapshot_path = settings.snapshot_dir / f"demo-macro-{snapshot_hash[:16]}.parquet"
    if not snapshot_path.exists():
        _atomic_parquet_write(frame, snapshot_path)
    persisted_frame = pd.read_parquet(snapshot_path)
    verify_snapshot_frame(
        persisted_frame,
        expected_hash=snapshot_hash,
        expected_rows=len(frame),
    )
    if existing is None:
        config_hash = content_hash(
            {
                "provider": provider.provider_code,
                "series": sorted(frame["series_code"].astype(str).tolist()),
                "fixture_version": "v1",
            }
        )
        existing = DataSnapshot(
            provider_code=provider.provider_code,
            dataset_type="MACRO_FACTS",
            source_uri="internal://deterministic-demo-macro-fixture/v1",
            as_of=pd.to_datetime(frame["as_of"], utc=True).max().to_pydatetime(),
            available_at=pd.to_datetime(frame["available_at"], utc=True).max().to_pydatetime(),
            data_mode=DataMode.DEMO_FIXTURE.value,
            license_scope="INTERNAL_TEST_AND_DEMONSTRATION_ONLY",
            parquet_uri=str(snapshot_path.resolve()),
            snapshot_hash=snapshot_hash,
            config_hash=config_hash,
            row_count=len(frame),
            quality_flags=["SYNTHETIC_NOT_MARKET_DATA"],
            revision="fixture-v1",
        )
        session.add(existing)
        session.flush()
        append_audit(
            session,
            event_type="MACRO_SNAPSHOT_INGESTED",
            object_type="DataSnapshot",
            object_id=existing.id,
            details={
                "provider": provider.provider_code,
                "snapshot_hash": snapshot_hash,
                "row_count": len(frame),
                "data_mode": DataMode.DEMO_FIXTURE.value,
            },
        )
    return existing


def _ingest_demo_news(session: Session, provider: DemoFixtureProvider, settings: Settings) -> None:
    events = provider.fetch_news(
        "demo",
        settings.demo_evaluation_time.replace(hour=0) - pd.Timedelta(days=7),
        settings.demo_evaluation_time,
    )
    for event in events:
        normalized = normalize_url(str(event.source_uri))
        dedupe_hash = content_hash(
            {"url": normalized, "title": event.title.lower(), "cluster": event.cluster_key}
        )
        candidate = NewsEvent(
            provider_code=event.provider,
            normalized_url=normalized,
            source_uri=str(event.source_uri),
            title=event.title,
            short_summary=event.short_summary,
            source_name=event.source_name,
            event_time=event.event_time,
            published_at=event.published_at,
            first_seen_at=event.first_seen_at,
            available_at=event.available_at,
            as_of=event.as_of,
            ingested_at=event.ingested_at,
            revision=event.revision,
            timezone=event.timezone,
            currency=event.currency,
            latency_class=event.latency_class,
            license_scope=event.license_scope,
            data_mode=event.data_mode.value,
            quality_flags=event.quality_flags,
            raw_payload_hash=event.raw_payload_hash,
            dedupe_hash=dedupe_hash,
            cluster_key=event.cluster_key,
            entities=event.entities,
            exposures=event.exposures,
            relevance=event.relevance,
            direction=event.direction,
            severity=event.severity,
            novelty=event.novelty,
            source_grade=event.source_grade,
        )
        candidate.record_hash = news_event_record_hash(candidate)
        current = session.scalar(
            select(NewsEvent)
            .where(NewsEvent.dedupe_hash == dedupe_hash)
            .execution_options(populate_existing=True)
        )
        if current is not None:
            current_hash = news_event_record_hash(current)
            if current_hash != candidate.record_hash:
                raise SnapshotIntegrityError(
                    "同一新闻去重键的结构化事实发生变化，已失败关闭；请使用受控新 revision。"
                )
            if current.record_hash is None:
                current.record_hash = current_hash
            elif not verify_news_event_record(current):
                raise SnapshotIntegrityError("新闻结构化事实完整性校验失败，已停止重放。")
            continue
        session.add(candidate)


def _ingest_demo_events(
    session: Session, provider: DemoFixtureProvider, settings: Settings
) -> None:
    events = provider.fetch_events(
        settings.demo_evaluation_time,
        settings.demo_evaluation_time + pd.Timedelta(days=7),
    )
    for event in events:
        candidate = ScheduledEvent(
            event_code=event.event_code,
            name_zh=event.name_zh,
            event_type="MACRO_RELEASE",
            scheduled_at=event.scheduled_at,
            event_time=event.event_time,
            published_at=event.published_at,
            first_announced_at=event.first_announced_at,
            first_seen_at=event.first_seen_at,
            available_at=event.available_at,
            as_of=event.as_of,
            ingested_at=event.ingested_at,
            revision=event.revision,
            timezone=event.timezone,
            currency=event.currency,
            latency_class=event.latency_class,
            alert_lead_hours=48,
            regions=event.regions,
            asset_classes=event.asset_classes,
            source_uri=str(event.source_uri),
            provider_code=event.provider,
            data_mode=event.data_mode.value,
            license_scope=event.license_scope,
            quality_flags=event.quality_flags,
            raw_payload_hash=event.raw_payload_hash,
            status=event.status,
            is_current=True,
        )
        candidate.record_hash = scheduled_event_record_hash(candidate)
        existing = session.scalar(
            select(ScheduledEvent)
            .where(
                ScheduledEvent.provider_code == event.provider,
                ScheduledEvent.event_code == event.event_code,
                ScheduledEvent.revision == event.revision,
            )
            .execution_options(populate_existing=True)
        )
        if existing is not None:
            existing_hash = scheduled_event_record_hash(existing)
            if existing_hash != candidate.record_hash:
                raise SnapshotIntegrityError(
                    "相同供应商、事件代码与 revision 的日历结构化事实发生变化，已失败关闭。"
                )
            if existing.record_hash is None:
                existing.record_hash = existing_hash
            elif not verify_scheduled_event_record(existing):
                raise SnapshotIntegrityError("事件日历事实完整性校验失败，已停止重放。")
            continue
        prior_current = list(
            session.scalars(
                select(ScheduledEvent).where(
                    ScheduledEvent.provider_code == event.provider,
                    ScheduledEvent.event_code == event.event_code,
                    ScheduledEvent.is_current.is_(True),
                )
            )
        )
        for prior in prior_current:
            prior.is_current = False
            prior.record_hash = scheduled_event_record_hash(prior)
            append_audit(
                session,
                event_type="SCHEDULED_EVENT_SUPERSEDED",
                object_type="ScheduledEvent",
                object_id=prior.id,
                details={"replacement_revision": event.revision},
            )
        scheduled_event = candidate
        session.add(scheduled_event)
        session.flush()
        append_audit(
            session,
            event_type="SCHEDULED_EVENT_INGESTED",
            object_type="ScheduledEvent",
            object_id=scheduled_event.id,
            details={
                "provider": event.provider,
                "event_code": event.event_code,
                "revision": event.revision,
                "raw_payload_hash": event.raw_payload_hash,
                "status": event.status,
            },
        )


def load_snapshot(snapshot: DataSnapshot, *, evaluation_time: datetime) -> pd.DataFrame:
    frame = pd.read_parquet(snapshot.parquet_uri)
    available = pd.to_datetime(frame["available_at"], utc=True)
    cutoff = pd.Timestamp(evaluation_time)
    if cutoff.tzinfo is None:
        cutoff = cutoff.tz_localize("UTC")
    else:
        cutoff = cutoff.tz_convert("UTC")
    return frame.loc[available <= cutoff].copy()


def _atomic_parquet_write(frame: pd.DataFrame, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".tmp.parquet")
    frame.to_parquet(temporary, index=False, compression="zstd")
    temporary.replace(target)

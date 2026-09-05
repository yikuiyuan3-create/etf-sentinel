from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
import sqlalchemy as sa

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ALEMBIC_CONFIG = PROJECT_ROOT / "alembic.ini"


def _safe_alembic_env(tmp_path: Path, database_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "APP_ENV": "test",
            "APP_HOST": "127.0.0.1",
            "TRADING_MODE": "paper",
            "DATABASE_URL": f"sqlite:///{database_path}",
            "SNAPSHOT_DIR": str(tmp_path / "snapshots"),
            "EXPORT_DIR": str(tmp_path / "exports"),
            "MARKET_DATA_PROVIDER": "demo_fixture",
            "ENABLE_PUBLIC_SERVICE": "false",
            "ENABLE_PAID_SUBSCRIPTIONS": "false",
            "ENABLE_PURCHASE_REDIRECT": "false",
            "ENABLE_THIRD_PARTY_FUNDS": "false",
            "ENABLE_LIVE_TRADING": "false",
            "ENABLE_ORDER_ENTRY": "false",
            "TWELVE_DATA_ENABLED": "false",
            "GDELT_ENABLED": "false",
            "EMAIL_ALERTS_ENABLED": "false",
            "WEBHOOK_ALERTS_ENABLED": "false",
            "AUTH_ENABLED": "false",
        }
    )
    return env


def _run_alembic(tmp_path: Path, database_path: Path, *arguments: str) -> None:
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "alembic", "-c", str(ALEMBIC_CONFIG), *arguments],
        cwd=PROJECT_ROOT,
        env=_safe_alembic_env(tmp_path, database_path),
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"Alembic {' '.join(arguments)} failed with exit {result.returncode}\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


def _table_schema(
    database_path: Path, table_name: str
) -> tuple[dict[str, dict[str, object]], set[str]]:
    engine = sa.create_engine(f"sqlite:///{database_path}")
    try:
        inspector = sa.inspect(engine)
        columns = {column["name"]: column for column in inspector.get_columns(table_name)}
        indexes = {index["name"] for index in inspector.get_indexes(table_name)}
        return columns, indexes
    finally:
        engine.dispose()


@pytest.mark.integration
def test_fresh_sqlite_upgrade_head_creates_point_in_time_lineage(tmp_path: Path) -> None:
    database_path = tmp_path / "fresh.sqlite3"

    _run_alembic(tmp_path, database_path, "upgrade", "head")

    signal_columns, signal_indexes = _table_schema(database_path, "signals")
    assert signal_columns["recorded_at"]["nullable"] is False
    assert signal_columns["is_current"]["nullable"] is False
    assert {"ix_signals_recorded_at", "ix_signals_is_current"} <= signal_indexes

    event_columns, event_indexes = _table_schema(database_path, "scheduled_events")
    required_not_null = {
        "event_time",
        "published_at",
        "first_seen_at",
        "available_at",
        "as_of",
        "ingested_at",
        "revision",
        "timezone",
        "latency_class",
        "quality_flags",
        "raw_payload_hash",
        "is_current",
    }
    assert required_not_null <= event_columns.keys()
    assert all(event_columns[name]["nullable"] is False for name in required_not_null)
    assert event_columns["currency"]["nullable"] is True
    assert event_columns["record_hash"]["nullable"] is True
    assert {
        "ix_scheduled_events_event_time",
        "ix_scheduled_events_first_seen_at",
        "ix_scheduled_events_available_at",
        "ix_scheduled_events_is_current",
    } <= event_indexes
    alert_columns, alert_indexes = _table_schema(database_path, "alerts")
    assert {
        "provider_code",
        "source_object_type",
        "source_object_id",
        "content_hash",
    } <= alert_columns.keys()
    assert alert_columns["source_object_type"]["nullable"] is False
    assert alert_columns["provider_code"]["nullable"] is True
    assert alert_columns["source_object_id"]["nullable"] is True
    assert alert_columns["content_hash"]["nullable"] is True
    assert {"ix_alerts_provider_code", "ix_alerts_source_object_id"} <= alert_indexes
    news_columns, _news_indexes = _table_schema(database_path, "news_events")
    assert news_columns["record_hash"]["nullable"] is True
    with sqlite3.connect(database_path) as connection:
        revision = connection.execute("SELECT version_num FROM alembic_version").fetchone()
    assert revision == ("0006_fact_record_integrity",)


@pytest.mark.integration
def test_legacy_0002_sqlite_upgrade_backfills_point_in_time_lineage(tmp_path: Path) -> None:
    database_path = tmp_path / "legacy.sqlite3"
    _run_alembic(tmp_path, database_path, "upgrade", "0002_scheduled_events")

    with sqlite3.connect(database_path) as connection:
        connection.execute("DROP INDEX IF EXISTS ix_signals_recorded_at")
        connection.execute("DROP INDEX IF EXISTS ix_signals_is_current")
        connection.execute("ALTER TABLE signals DROP COLUMN recorded_at")
        connection.execute("ALTER TABLE signals DROP COLUMN is_current")
        connection.execute("DROP INDEX IF EXISTS ix_alerts_provider_code")
        connection.execute("DROP INDEX IF EXISTS ix_alerts_source_object_id")
        connection.execute("ALTER TABLE alerts DROP COLUMN content_hash")
        connection.execute("ALTER TABLE alerts DROP COLUMN source_object_id")
        connection.execute("ALTER TABLE alerts DROP COLUMN source_object_type")
        connection.execute("ALTER TABLE alerts DROP COLUMN provider_code")
        # Revision 0001 creates current metadata, so rebuild this table to the
        # actual pre-lineage 0002 shape before exercising the forward migration.
        connection.execute("DROP TABLE scheduled_events")
        connection.execute(
            """
            CREATE TABLE scheduled_events (
                id VARCHAR(36) NOT NULL PRIMARY KEY,
                event_code VARCHAR(100) NOT NULL,
                name_zh VARCHAR(300) NOT NULL,
                event_type VARCHAR(80) NOT NULL,
                scheduled_at DATETIME NOT NULL,
                first_announced_at DATETIME NOT NULL,
                alert_lead_hours INTEGER NOT NULL,
                regions JSON NOT NULL,
                asset_classes JSON NOT NULL,
                source_uri TEXT NOT NULL,
                provider_code VARCHAR(80) NOT NULL,
                data_mode VARCHAR(24) NOT NULL,
                license_scope TEXT NOT NULL,
                status VARCHAR(24) NOT NULL,
                created_at DATETIME NOT NULL,
                CONSTRAINT uq_scheduled_events_event_code_scheduled_at
                    UNIQUE (event_code, scheduled_at)
            )
            """
        )
        connection.execute(
            "CREATE INDEX ix_scheduled_events_event_code ON scheduled_events (event_code)"
        )
        connection.execute(
            "CREATE INDEX ix_scheduled_events_event_type ON scheduled_events (event_type)"
        )
        connection.execute(
            "CREATE INDEX ix_scheduled_events_scheduled_at ON scheduled_events (scheduled_at)"
        )
        connection.execute(
            "CREATE INDEX ix_scheduled_events_provider_code ON scheduled_events (provider_code)"
        )
        connection.execute(
            "CREATE INDEX ix_scheduled_events_data_mode ON scheduled_events (data_mode)"
        )
        connection.execute(
            """
            INSERT INTO signals (
                id, idempotency_key, instrument_id, data_snapshot_id, model_version_id,
                data_as_of, available_at, generated_at, horizon_days, state,
                probability, confidence, confidence_interval,
                market_score, macro_score, news_score, liquidity_score, composite_score,
                supporting_evidence, opposing_evidence, source_links, feature_values,
                risk_rules_hit, trigger_conditions, invalidation_conditions,
                suggested_risk_budget_min, suggested_risk_budget_max, limitations,
                data_mode, latency_status, code_version
            ) VALUES (
                :id, :idempotency_key, :instrument_id, :data_snapshot_id, :model_version_id,
                :data_as_of, :available_at, :generated_at, :horizon_days, :state,
                :probability, :confidence, :confidence_interval,
                :market_score, :macro_score, :news_score, :liquidity_score, :composite_score,
                :supporting_evidence, :opposing_evidence, :source_links, :feature_values,
                :risk_rules_hit, :trigger_conditions, :invalidation_conditions,
                :suggested_risk_budget_min, :suggested_risk_budget_max, :limitations,
                :data_mode, :latency_status, :code_version
            )
            """,
            {
                "id": "legacy-signal",
                "idempotency_key": "legacy-idempotency-key",
                "instrument_id": "legacy-instrument",
                "data_snapshot_id": "legacy-snapshot",
                "model_version_id": "legacy-model",
                "data_as_of": "2025-01-02 07:00:00",
                "available_at": "2025-01-02 07:10:00",
                "generated_at": "2025-01-02 08:00:00",
                "horizon_days": 20,
                "state": "WATCH",
                "probability": 0.55,
                "confidence": 0.60,
                "confidence_interval": "[0.45, 0.65]",
                "market_score": 0.10,
                "macro_score": 0.00,
                "news_score": 0.00,
                "liquidity_score": 0.20,
                "composite_score": 0.12,
                "supporting_evidence": "[]",
                "opposing_evidence": "[]",
                "source_links": "[]",
                "feature_values": "{}",
                "risk_rules_hit": "[]",
                "trigger_conditions": "[]",
                "invalidation_conditions": "[]",
                "suggested_risk_budget_min": 0.01,
                "suggested_risk_budget_max": 0.05,
                "limitations": "[]",
                "data_mode": "DEMO_FIXTURE",
                "latency_status": "ON_TIME",
                "code_version": "legacy-test",
            },
        )
        connection.execute(
            """
            INSERT INTO scheduled_events (
                id, event_code, name_zh, event_type, scheduled_at, first_announced_at,
                alert_lead_hours, regions, asset_classes, source_uri, provider_code,
                data_mode, license_scope, status, created_at
            ) VALUES (
                :id, :event_code, :name_zh, :event_type, :scheduled_at,
                :first_announced_at, :alert_lead_hours, :regions, :asset_classes,
                :source_uri, :provider_code, :data_mode, :license_scope, :status,
                :created_at
            )
            """,
            {
                "id": "legacy-event",
                "event_code": "LEGACY-CPI",
                "name_zh": "历史宏观事件",
                "event_type": "MACRO_DATA",
                "scheduled_at": "2025-01-10 01:30:00",
                "first_announced_at": "2024-12-01 08:00:00",
                "alert_lead_hours": 48,
                "regions": '["US"]',
                "asset_classes": '["EQUITY", "BOND"]',
                "source_uri": "https://example.invalid/calendar/legacy-cpi",
                "provider_code": "demo_fixture",
                "data_mode": "DEMO_FIXTURE",
                "license_scope": "INTERNAL_RESEARCH",
                "status": "SCHEDULED",
                "created_at": "2024-12-01 08:05:00",
            },
        )
        connection.execute(
            """
            INSERT INTO alerts (
                id, dedupe_key, alert_type, severity, status, title, message,
                instrument_id, signal_id, data_as_of, latency_status,
                trigger_reason, confidence, source_links, invalidation_conditions,
                disclaimer, channel, delivery_attempts, last_error, created_at,
                sent_at, acknowledged_at, acknowledged_by
            ) VALUES (
                :id, :dedupe_key, :alert_type, :severity, :status, :title, :message,
                :instrument_id, :signal_id, :data_as_of, :latency_status,
                :trigger_reason, :confidence, :source_links, :invalidation_conditions,
                :disclaimer, :channel, :delivery_attempts, :last_error, :created_at,
                :sent_at, :acknowledged_at, :acknowledged_by
            )
            """,
            {
                "id": "legacy-alert",
                "dedupe_key": "legacy-alert-dedupe",
                "alert_type": "SIGNAL_STATE_CHANGE",
                "severity": "WARN",
                "status": "SENT",
                "title": "历史信号预警",
                "message": "历史内容无法在迁移时重签。",
                "instrument_id": None,
                "signal_id": "legacy-signal",
                "data_as_of": "2025-01-02 07:00:00",
                "latency_status": "ON_TIME",
                "trigger_reason": "LEGACY",
                "confidence": 0.5,
                "source_links": "[]",
                "invalidation_conditions": "[]",
                "disclaimer": "legacy internal only",
                "channel": "IN_APP",
                "delivery_attempts": 1,
                "last_error": None,
                "created_at": "2025-01-02 08:00:00",
                "sent_at": "2025-01-02 08:00:00",
                "acknowledged_at": None,
                "acknowledged_by": None,
            },
        )
        connection.commit()

    _run_alembic(tmp_path, database_path, "stamp", "0002_scheduled_events")
    _run_alembic(tmp_path, database_path, "upgrade", "head")

    signal_columns, signal_indexes = _table_schema(database_path, "signals")
    assert signal_columns["recorded_at"]["nullable"] is False
    assert signal_columns["is_current"]["nullable"] is False
    assert {"ix_signals_recorded_at", "ix_signals_is_current"} <= signal_indexes

    event_columns, event_indexes = _table_schema(database_path, "scheduled_events")
    required_not_null = {
        "event_time",
        "published_at",
        "first_seen_at",
        "available_at",
        "as_of",
        "ingested_at",
        "revision",
        "timezone",
        "latency_class",
        "quality_flags",
        "raw_payload_hash",
        "is_current",
    }
    assert required_not_null <= event_columns.keys()
    assert all(event_columns[name]["nullable"] is False for name in required_not_null)
    assert event_columns["currency"]["nullable"] is True
    assert event_columns["record_hash"]["nullable"] is True
    assert {
        "ix_scheduled_events_event_time",
        "ix_scheduled_events_first_seen_at",
        "ix_scheduled_events_available_at",
        "ix_scheduled_events_is_current",
    } <= event_indexes
    alert_columns, alert_indexes = _table_schema(database_path, "alerts")
    assert alert_columns["source_object_type"]["nullable"] is False
    assert {"ix_alerts_provider_code", "ix_alerts_source_object_id"} <= alert_indexes
    news_columns, _news_indexes = _table_schema(database_path, "news_events")
    assert news_columns["record_hash"]["nullable"] is True
    with sqlite3.connect(database_path) as connection:
        migrated_signal = connection.execute(
            "SELECT recorded_at, is_current FROM signals WHERE id = 'legacy-signal'"
        ).fetchone()
        migrated_event = connection.execute(
            """
            SELECT event_time, published_at, first_seen_at, available_at, as_of,
                   ingested_at, revision, timezone, currency, latency_class,
                   quality_flags, raw_payload_hash, is_current, record_hash
              FROM scheduled_events
             WHERE id = 'legacy-event'
            """
        ).fetchone()
        migrated_alert = connection.execute(
            """
            SELECT provider_code, source_object_type, source_object_id, content_hash
              FROM alerts
             WHERE id = 'legacy-alert'
            """
        ).fetchone()
        revision = connection.execute("SELECT version_num FROM alembic_version").fetchone()
    assert migrated_signal is not None
    assert migrated_signal[0]
    assert migrated_signal[1] == 1
    assert migrated_event is not None
    assert migrated_event[:5] == (
        "2025-01-10 01:30:00",
        "2024-12-01 08:00:00",
        "2024-12-01 08:00:00",
        "2024-12-01 08:00:00",
        "2024-12-01 08:00:00",
    )
    expected_hash = hashlib.sha256(b"legacy-scheduled-event:legacy-event").hexdigest()
    assert migrated_event[5:10] == (
        "2024-12-01 08:05:00",
        f"legacy-{expected_hash[:16]}",
        "UTC",
        None,
        "LEGACY_MIGRATED",
    )
    assert json.loads(migrated_event[10]) == ["MIGRATED_LINEAGE_REVIEW_REQUIRED"]
    assert migrated_event[11] == expected_hash
    assert migrated_event[12] == 1
    # Migration cannot honestly sign historical structured facts whose source
    # payload is unavailable; legacy rows remain explicitly unverified.
    assert migrated_event[13] is None
    assert migrated_alert == (None, "LegacyUnverifiedSignal", "legacy-signal", None)
    assert revision == ("0006_fact_record_integrity",)

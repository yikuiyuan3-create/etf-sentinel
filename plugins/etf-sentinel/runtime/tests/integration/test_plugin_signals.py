"""Public HTTP contract for the minimal, bounded plugin signal projection."""

from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, select

from etf_sentinel import main
from etf_sentinel.database import get_db
from etf_sentinel.enums import DISCLAIMER
from etf_sentinel.models import ProviderRegistry, Signal
from etf_sentinel.services.pipeline import run_demo_pipeline
from etf_sentinel.services.signals import signal_record_hash

FIELDS = {
    "id",
    "instrument_id",
    "data_snapshot_id",
    "model_version_id",
    "horizon_days",
    "state",
    "probability",
    "confidence",
    "market_score",
    "macro_score",
    "news_score",
    "liquidity_score",
    "composite_score",
    "data_as_of",
    "available_at",
    "data_mode",
    "latency_status",
    "risk_rules_hit",
    "code_version",
    "is_current",
}


@pytest.fixture
def http_client(db_session, monkeypatch, test_settings):
    monkeypatch.setattr(main, "settings", test_settings)

    def override_db():
        yield db_session

    main.app.dependency_overrides[get_db] = override_db
    try:
        with TestClient(main.app) as client:
            yield client
    finally:
        main.app.dependency_overrides.clear()


@pytest.mark.integration
def test_plugin_limits_sql_selection_and_wire_fields(http_client, db_session, test_settings):
    run_demo_pipeline(db_session, test_settings)
    selections = []

    def record_selection(_conn, _cursor, statement, parameters, _context, _many):
        normalized = " ".join(statement.lower().split())
        if "from signals" in normalized and "order by signals.recorded_at desc" in normalized:
            selections.append((normalized, parameters))

    engine = db_session.get_bind()
    event.listen(engine, "before_cursor_execute", record_selection)
    try:
        for query, expected_limit in (
            ("?limit=1", 1),
            ("?limit=5", 5),
            ("?limit=20", 20),
            ("", 10),
        ):
            selections.clear()
            response = http_client.get(f"/api/v1/plugin/signals{query}")
            assert response.status_code == 200
            payload = response.json()
            assert payload["disclaimer"] == DISCLAIMER
            assert payload["data_mode"] == "DEMO_FIXTURE"
            assert len(payload["data"]) == expected_limit
            assert all(set(row) == FIELDS for row in payload["data"])
            assert all(datetime.fromisoformat(row["data_as_of"]).tzinfo for row in payload["data"])
            assert len(selections) == 1
            assert "limit ? offset ?" in selections[0][0]
            assert selections[0][1][-2:] == (expected_limit, 0)
        # The dashboard still receives its existing detailed contract.
        dashboard = http_client.get("/api/v1/signals").json()["data"]
        assert len(dashboard) == 60
        assert "feature_values" in dashboard[0]
    finally:
        event.remove(engine, "before_cursor_execute", record_selection)


@pytest.mark.parametrize("limit", ["0", "21", "-1", "true", "1.5", "invalid"])
def test_plugin_rejects_invalid_bounds_before_reading_signals(http_client, monkeypatch, limit):
    def must_not_read(*_args, **_kwargs):
        raise AssertionError("Invalid limits must not read signal data")

    monkeypatch.setattr(main, "_visible_signals", must_not_read)
    assert http_client.get(f"/api/v1/plugin/signals?limit={limit}").status_code == 422


@pytest.mark.integration
def test_plugin_removes_private_payload_and_risk_suffix_before_wire(
    http_client, db_session, test_settings
):
    run_demo_pipeline(db_session, test_settings)
    row = db_session.scalar(select(Signal).where(Signal.is_current.is_(True)))
    row.recorded_at = datetime.now(UTC) + timedelta(seconds=1)
    row.state = "BLOCKED_BY_RISK"
    row.risk_rules_hit = ["PORTFOLIO_LIMIT:PRIVATE_INTERNAL_TEXT", "PORTFOLIO_LIMIT:OTHER_PRIVATE"]
    row.supporting_evidence = [{"internal": "PRIVATE_INTERNAL_TEXT"}]
    row.source_links = ["https://private.invalid/PRIVATE_INTERNAL_TEXT"]
    row.feature_values = {**row.feature_values, "private": "PRIVATE_INTERNAL_TEXT"}
    row.feature_values = {**row.feature_values, "signal_record_hash": signal_record_hash(row)}
    db_session.commit()
    response = http_client.get("/api/v1/plugin/signals?limit=1")
    assert response.status_code == 200
    payload = response.json()["data"]
    assert len(payload) == 1
    assert payload[0]["id"] == row.id
    assert set(payload[0]) == FIELDS
    assert payload[0]["risk_rules_hit"] == ["PORTFOLIO_LIMIT"]
    assert "PRIVATE" not in response.text
    assert "private.invalid" not in response.text


@pytest.mark.parametrize("mode", ["LIVE_LICENSED", "DELAYED", "HISTORICAL"])
def test_plugin_refuses_non_demo_before_signal_read(http_client, monkeypatch, mode):
    monkeypatch.setattr(main, "_current_data_mode", lambda _db: mode)

    def must_not_read(*_args, **_kwargs):
        raise AssertionError("Non-demo requests must not read signals")

    monkeypatch.setattr(main, "_visible_signals", must_not_read)
    response = http_client.get("/api/v1/plugin/signals?limit=1")
    assert response.status_code == 403
    assert response.json() == {"detail": "NON_DEMO_MODE_BLOCKED"}


def test_plugin_refuses_non_paper_requests(http_client, monkeypatch):
    monkeypatch.setattr(main.settings, "trading_mode", "live")
    response = http_client.get("/api/v1/plugin/signals?limit=1")
    assert response.status_code == 403
    assert response.json() == {"detail": "UNSAFE_TRADING_MODE"}


@pytest.mark.integration
def test_plugin_preserves_license_and_integrity_fail_closed(http_client, db_session, test_settings):
    run_demo_pipeline(db_session, test_settings)
    assert http_client.get("/api/v1/plugin/signals?limit=1").json()["data"]
    provider = db_session.scalar(
        select(ProviderRegistry).where(ProviderRegistry.provider_code == "demo_fixture")
    )
    provider.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    db_session.commit()
    assert http_client.get("/api/v1/plugin/signals?limit=1").json()["data"] == []
    provider.expires_at = None
    db_session.commit()
    row = db_session.scalar(select(Signal).where(Signal.is_current.is_(True)))
    row.probability = 0.9999  # Deliberately leave its integrity hash unchanged.
    db_session.commit()
    assert http_client.get("/api/v1/plugin/signals?limit=1").json()["data"] == []


def test_plugin_endpoint_has_no_write_method(http_client):
    assert http_client.post("/api/v1/plugin/signals?limit=1").status_code == 405

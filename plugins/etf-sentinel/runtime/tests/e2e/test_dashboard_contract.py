from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from etf_sentinel.database import get_db
from etf_sentinel.enums import DISCLAIMER, DataMode, LicenseStatus, ModelStatus
from etf_sentinel.main import app
from etf_sentinel.models import (
    Alert,
    BacktestExperiment,
    DataSnapshot,
    EtfInstrument,
    ModelVersion,
    NewsEvent,
    ProviderRegistry,
    Signal,
    SimulationLedger,
)
from etf_sentinel.services.backtest import verify_backtest_artifact
from etf_sentinel.services.ledger import seal_simulation_ledger_entry
from etf_sentinel.services.logistic_model import verify_logistic_model_artifact
from etf_sentinel.services.pipeline import run_demo_pipeline


@pytest.mark.e2e
def test_dashboard_and_business_apis_show_demo_mode_and_full_disclaimer(
    db_session, test_settings
) -> None:
    run_demo_pipeline(db_session, test_settings)

    def override_db():
        yield db_session

    app.dependency_overrides[get_db] = override_db
    try:
        with TestClient(app) as client:
            dashboard = client.get("/")
            assert dashboard.status_code == 200
            assert "DEMO_FIXTURE" in dashboard.text
            assert DISCLAIMER in dashboard.text
            assert "TRADING_MODE=paper" in dashboard.text
            assert "无订单入口" in dashboard.text

            health = client.get("/health")
            assert health.status_code == 200
            assert health.json()["trading_mode"] == "paper"
            assert health.json()["data_mode"] == DataMode.DEMO_FIXTURE.value

            for path in [
                "/api/v1/status",
                "/api/v1/signals",
                "/api/v1/etfs",
                "/api/v1/alerts",
                "/api/v1/portfolio",
                "/api/v1/backtests/latest",
                "/api/v1/providers",
                "/api/v1/models",
                "/api/v1/audit",
            ]:
                response = client.get(path)
                assert response.status_code == 200, path
                payload = response.json()
                assert payload["data_mode"] == DataMode.DEMO_FIXTURE.value, path
                assert payload["disclaimer"] == DISCLAIMER, path

            portfolio = client.get("/api/v1/portfolio").json()["data"]
            assert portfolio["valuation_error"] is None
            assert portfolio["nav"] == pytest.approx(
                portfolio["cash"]
                + sum(float(item["market_value"]) for item in portfolio["holdings"])
            ), portfolio
            assert portfolio["valuation_time"]
            assert portfolio["max_drawdown"] is not None
            assert portfolio["holdings"]
            assert all(
                item["instrument_id"]
                and item["average_cost"] > 0
                and item["reference_price"] > 0
                and item["market_value"] > 0
                and 0 <= item["weight"] <= 1
                for item in portfolio["holdings"]
            )

            market_snapshot = db_session.scalar(
                select(DataSnapshot).where(DataSnapshot.dataset_type == "MARKET_BARS")
            )
            backtest_artifact = db_session.scalar(select(BacktestExperiment))
            assert market_snapshot is not None and backtest_artifact is not None
            assert verify_backtest_artifact(
                backtest_artifact, snapshot_hash=market_snapshot.snapshot_hash
            )
            original_backtest_values = {
                "metrics": dict(backtest_artifact.metrics),
                "status": backtest_artifact.status,
                "config": dict(backtest_artifact.config),
            }
            for attribute, tampered_value in [
                (
                    "metrics",
                    {**original_backtest_values["metrics"], "cagr": 999.0},
                ),
                ("status", "APPROVED_WITHOUT_REVIEW"),
                (
                    "config",
                    {**original_backtest_values["config"], "fee_bps": -1.0},
                ),
            ]:
                setattr(backtest_artifact, attribute, tampered_value)
                db_session.commit()
                blocked_artifact = client.get("/api/v1/backtests/latest").json()["data"]
                assert (
                    blocked_artifact["status"] == "ARTIFACT_INTEGRITY_BLOCKED_HISTORICAL_AUDIT_ONLY"
                )
                assert (
                    blocked_artifact["display_blocked_reason"]
                    == "BACKTEST_ARTIFACT_INTEGRITY_BLOCKED"
                )
                assert blocked_artifact["metrics"] == {}
                assert blocked_artifact["baselines"] == {}
                assert blocked_artifact["periods"] == {}
                assert blocked_artifact["leakage_checks"] == {}
                assert blocked_artifact["config"] == {}
                setattr(backtest_artifact, attribute, original_backtest_values[attribute])
                db_session.commit()
                assert verify_backtest_artifact(
                    backtest_artifact, snapshot_hash=market_snapshot.snapshot_hash
                )

            logistic_artifact = db_session.scalar(
                select(ModelVersion).where(ModelVersion.model_name == "LogisticBaselineV1")
            )
            assert logistic_artifact is not None
            assert verify_logistic_model_artifact(logistic_artifact)
            original_logistic_values = {
                "metrics": dict(logistic_artifact.metrics),
                "status": logistic_artifact.status,
                "limitations": list(logistic_artifact.limitations),
            }
            for attribute, tampered_value in [
                (
                    "metrics",
                    {**original_logistic_values["metrics"], "brier_score": 999.0},
                ),
                ("status", "CHAMPION"),
                (
                    "limitations",
                    [*original_logistic_values["limitations"], "未授权改写"],
                ),
            ]:
                setattr(logistic_artifact, attribute, tampered_value)
                db_session.commit()
                model_rows = client.get("/api/v1/models").json()["data"]
                blocked_model = next(row for row in model_rows if row["id"] == logistic_artifact.id)
                assert blocked_model["status"] == "ARTIFACT_INTEGRITY_BLOCKED_HISTORICAL_AUDIT_ONLY"
                assert blocked_model["recorded_status"] == logistic_artifact.status
                assert blocked_model["display_blocked_reason"] == "MODEL_ARTIFACT_INTEGRITY_BLOCKED"
                assert blocked_model["metrics"] == {}
                assert blocked_model["limitations"] == []
                assert blocked_model["approved_by"] is None
                assert blocked_model["approved_at"] is None
                setattr(logistic_artifact, attribute, original_logistic_values[attribute])
                db_session.commit()
                assert verify_logistic_model_artifact(logistic_artifact)

            buy_ledger = db_session.scalar(
                select(SimulationLedger).where(SimulationLedger.entry_type == "BUY")
            )
            assert buy_ledger is not None
            original_cash_delta = buy_ledger.cash_delta
            buy_ledger.cash_delta = original_cash_delta + 123.45
            db_session.commit()
            blocked_portfolio_response = client.get("/api/v1/portfolio")
            assert blocked_portfolio_response.status_code == 200
            blocked_portfolio = blocked_portfolio_response.json()["data"]
            assert blocked_portfolio["valuation_error"] == "LEDGER_INTEGRITY_BLOCKED"
            assert blocked_portfolio["cash"] is None
            assert blocked_portfolio["nav"] is None
            assert blocked_portfolio["holdings"] == []
            assert blocked_portfolio["fills"] == []
            ledger_status = client.get("/api/v1/status").json()["data"]
            assert ledger_status["candidate_output_fail_closed"] is True
            assert (
                ledger_status["candidate_output_fail_closed_reason"]
                == "PORTFOLIO_LEDGER_INTEGRITY_FAILED"
            )
            assert client.get("/api/v1/signals").json()["data"] == []
            buy_ledger.cash_delta = original_cash_delta
            db_session.commit()
            assert client.get("/api/v1/portfolio").json()["data"]["valuation_error"] is None

            realized_loss = SimulationLedger(
                idempotency_key="dashboard-portfolio-loss-gate",
                fill_id=None,
                instrument_id=None,
                entry_type="REALIZED_LOSS",
                occurred_at=datetime(2025, 1, 30, 6, 0, tzinfo=UTC),
                cash_delta=-200_000.0,
                quantity_delta=0.0,
                fee_amount=0.0,
                memo="组合风险读时门禁测试",
            )
            db_session.add(realized_loss)
            seal_simulation_ledger_entry(db_session, realized_loss)
            db_session.flush()
            portfolio_risk_status = client.get("/api/v1/status").json()["data"]
            assert portfolio_risk_status["candidate_output_fail_closed"] is True
            assert portfolio_risk_status["candidate_output_fail_closed_reason"].startswith(
                "PORTFOLIO_RISK_BLOCKED:PORTFOLIO_MAX_SIMULATION_LOSS"
            )
            assert client.get("/api/v1/signals").json()["data"] == []
            db_session.rollback()

            candidate = db_session.scalar(
                select(Signal).where(
                    Signal.is_current.is_(True), Signal.state.contains("CANDIDATE")
                )
            )
            assert candidate is not None
            instrument = db_session.get(EtfInstrument, candidate.instrument_id)
            assert instrument is not None

            news_events = candidate.feature_values["news_lineage"]["events"]
            assert news_events
            news_source = db_session.get(NewsEvent, news_events[0]["id"])
            assert news_source is not None
            original_news_title = news_source.title
            news_source.title = "TAMPERED NEWS TITLE"
            db_session.commit()
            news_status = client.get("/api/v1/status").json()["data"]
            assert news_status["candidate_output_fail_closed"] is True
            assert news_status["candidate_output_fail_closed_reason"] == (
                "NEWS_LINEAGE_INTEGRITY_FAILED"
            )
            assert client.get("/api/v1/signals").json()["data"] == []
            news_detail = client.get(f"/signals/{candidate.id}")
            assert news_detail.status_code == 503
            assert "NEWS_LINEAGE_INTEGRITY_FAILED" in news_detail.json()["detail"]
            news_source.title = original_news_title
            db_session.commit()

            tampered_alert = db_session.scalar(select(Alert).where(Alert.signal_id.is_not(None)))
            assert tampered_alert is not None
            original_alert_reason = tampered_alert.trigger_reason
            tampered_alert.trigger_reason = "TAMPERED_AFTER_DELIVERY"
            db_session.commit()
            tampered_alert_view = next(
                row
                for row in client.get("/api/v1/alerts").json()["data"]
                if row["id"] == tampered_alert.id
            )
            assert tampered_alert_view["confidence"] is None
            assert tampered_alert_view["sources"] == []
            assert tampered_alert_view["trigger_reason"] == "ALERT_DERIVED_DISPLAY_BLOCKED"
            assert tampered_alert_view["display_blocked_reason"] == (
                "ALERT_CONTENT_INTEGRITY_FAILED"
            )
            rejected_ack = client.post(
                f"/api/v1/alerts/{tampered_alert.id}/acknowledge",
                headers={"X-ETF-Sentinel-Intent": "acknowledge"},
            )
            assert rejected_ack.status_code == 409
            tampered_alert.trigger_reason = original_alert_reason
            db_session.commit()

            original_state = candidate.state
            original_risk_rules = list(candidate.risk_rules_hit)
            original_budget = candidate.suggested_risk_budget_max
            for attribute, tampered_value in [
                ("state", "WATCH"),
                ("risk_rules_hit", [*original_risk_rules, "TAMPERED_AFTER_GENERATION"]),
                ("suggested_risk_budget_max", original_budget + 0.001),
            ]:
                setattr(candidate, attribute, tampered_value)
                db_session.commit()
                integrity_ready = client.get("/health/ready")
                integrity_status = client.get("/api/v1/status").json()["data"]
                assert integrity_ready.json()["status"] == "degraded_fail_closed"
                assert integrity_status["candidate_output_fail_closed_reason"] == (
                    "SIGNAL_RECORD_INTEGRITY_FAILED"
                )
                assert client.get("/api/v1/signals").json()["data"] == []
                integrity_detail = client.get(f"/signals/{candidate.id}")
                assert integrity_detail.status_code == 503
                assert "完整性校验失败" in integrity_detail.json()["detail"]
                setattr(
                    candidate,
                    attribute,
                    {
                        "state": original_state,
                        "risk_rules_hit": original_risk_rules,
                        "suggested_risk_budget_max": original_budget,
                    }[attribute],
                )
                db_session.commit()

            for attribute, blocked_value in [("leveraged", True), ("active", False)]:
                original_value = getattr(instrument, attribute)
                setattr(instrument, attribute, blocked_value)
                db_session.commit()
                master_status = client.get("/api/v1/status").json()["data"]
                assert master_status["candidate_output_fail_closed"] is True
                assert master_status["candidate_output_fail_closed_reason"] == (
                    "DECISION_POLICY_CHANGED"
                )
                assert client.get("/api/v1/signals").json()["data"] == []
                master_detail = client.get(f"/signals/{candidate.id}")
                assert master_detail.status_code == 503
                assert "DECISION_POLICY_CHANGED" in master_detail.json()["detail"]
                setattr(instrument, attribute, original_value)
                db_session.commit()

            registry = db_session.scalar(
                select(ProviderRegistry).where(ProviderRegistry.provider_code == "demo_fixture")
            )
            assert registry is not None
            original_markets = list(registry.markets)
            registry.markets = [market for market in registry.markets if market != "XDEM"]
            db_session.commit()

            ready = client.get("/health/ready")
            status = client.get("/api/v1/status").json()["data"]
            visible = client.get("/api/v1/signals").json()["data"]
            visible_etfs = client.get("/api/v1/etfs").json()["data"]
            visible_alerts = client.get("/api/v1/alerts").json()["data"]
            scoped_models = client.get("/api/v1/models").json()["data"]
            scoped_logistic = [
                row for row in scoped_models if row["model_name"] == "LogisticBaselineV1"
            ]
            detail = client.get(f"/signals/{candidate.id}")
            assert ready.json()["status"] == "degraded_fail_closed"
            assert status["candidate_output_fail_closed"] is True
            assert status["candidate_output_fail_closed_reason"] == (
                "MARKET_OR_REGION_LICENSE_BLOCKED"
            )
            assert visible == []
            assert visible_etfs == []
            assert scoped_logistic
            assert all(
                row["status"] == "LICENSE_BLOCKED_HISTORICAL_AUDIT_ONLY"
                and row["display_blocked_reason"] == "PROVIDER_LICENSE_BLOCKED"
                for row in scoped_logistic
            )
            assert detail.status_code == 503
            assert "MARKET_OR_REGION_LICENSE_BLOCKED" in detail.json()["detail"]
            signal_alert = db_session.scalar(select(Alert).where(Alert.signal_id.is_not(None)))
            calendar_alert = db_session.scalar(
                select(Alert).where(Alert.alert_type == "KNOWN_EVENT_WINDOW")
            )
            assert signal_alert is not None and calendar_alert is not None
            signal_alert_view = next(row for row in visible_alerts if row["id"] == signal_alert.id)
            calendar_alert_view = next(
                row for row in visible_alerts if row["id"] == calendar_alert.id
            )
            assert signal_alert_view["confidence"] is None
            assert signal_alert_view["sources"] == []
            assert signal_alert_view["trigger_reason"] == "SIGNAL_DERIVED_DISPLAY_BLOCKED"
            assert calendar_alert_view["confidence"] == calendar_alert.confidence
            assert calendar_alert_view["sources"] == calendar_alert.source_links
            assert calendar_alert_view["trigger_reason"] == calendar_alert.trigger_reason
            scoped_dashboard = client.get("/")
            assert instrument.provider_symbol not in scoped_dashboard.text
            registry.markets = original_markets
            db_session.commit()

            original_expiry = registry.expires_at
            registry.expires_at = pd.Timestamp("2020-01-01T00:00:00Z").to_pydatetime()
            db_session.commit()
            assert client.get("/api/v1/etfs").json()["data"] == []
            assert instrument.provider_symbol not in client.get("/").text
            registry.expires_at = original_expiry
            db_session.commit()

            rule_model = db_session.get(ModelVersion, candidate.model_version_id)
            assert rule_model is not None
            rule_model.status = ModelStatus.REJECTED.value
            db_session.commit()
            model_status = client.get("/api/v1/status").json()["data"]
            model_visible = client.get("/api/v1/signals").json()["data"]
            model_detail = client.get(f"/signals/{candidate.id}")
            assert model_status["candidate_output_fail_closed"] is True
            assert "MODEL_STATUS_REJECTED" in model_status["candidate_output_fail_closed_reason"]
            assert all("CANDIDATE" not in signal["state"] for signal in model_visible)
            assert model_detail.status_code == 503
            rule_model.status = ModelStatus.EXPERIMENTAL.value

            rule_model.feature_version = "unexpected-feature-version"
            db_session.commit()
            feature_status = client.get("/api/v1/status").json()["data"]
            assert (
                "MODEL_FEATURE_VERSION_UNEXPECTED"
                in feature_status["candidate_output_fail_closed_reason"]
            )
            rule_model.feature_version = "rule-features-v1"
            db_session.commit()

            historical_signal = db_session.scalar(select(Signal).where(Signal.is_current.is_(True)))
            assert historical_signal is not None
            historical_signal.is_current = False
            db_session.commit()
            obsolete_detail = client.get(f"/signals/{historical_signal.id}")
            assert obsolete_detail.status_code == 410
            historical_signal.is_current = True
            db_session.commit()

            registry.review_status = LicenseStatus.BLOCKED.value
            db_session.commit()
            assert client.get("/api/v1/etfs").json()["data"] == []
            assert instrument.provider_symbol not in client.get("/").text
            license_blocked = client.get("/api/v1/portfolio").json()["data"]
            assert license_blocked["valuation_error"] == "PROVIDER_LICENSE_BLOCKED"
            assert license_blocked["nav"] is None
            assert license_blocked["holdings"] == []
            blocked_backtest = client.get("/api/v1/backtests/latest").json()["data"]
            assert blocked_backtest["status"] == "LICENSE_BLOCKED_HISTORICAL_AUDIT_ONLY"
            assert blocked_backtest["metrics"] == {}
            assert blocked_backtest["baselines"] == {}
            assert blocked_backtest["periods"] == {}
            blocked_models = client.get("/api/v1/models").json()["data"]
            blocked_logistic = [
                row for row in blocked_models if row["model_name"] == "LogisticBaselineV1"
            ]
            assert blocked_logistic
            assert all(
                row["status"] == "LICENSE_BLOCKED_HISTORICAL_AUDIT_ONLY"
                and row["recorded_status"] in {"EXPERIMENTAL", "REJECTED"}
                and row["display_blocked_reason"] == "PROVIDER_LICENSE_BLOCKED"
                and row["metrics"] == {}
                and row["limitations"] == []
                and row["approved_by"] is None
                and row["approved_at"] is None
                for row in blocked_logistic
            )
            blocked_dashboard = client.get("/")
            assert "历史回测仅保留审计身份，绩效展示已阻断" in blocked_dashboard.text
            assert "派生模型指标展示已阻断" in blocked_dashboard.text
            registry.review_status = LicenseStatus.APPROVED.value
            db_session.commit()

            snapshot = db_session.scalar(
                select(DataSnapshot).where(DataSnapshot.dataset_type == "MARKET_BARS")
            )
            assert snapshot is not None
            tampered = pd.read_parquet(snapshot.parquet_uri)
            tampered.loc[tampered.index[-1], "close"] *= 100
            tampered.to_parquet(snapshot.parquet_uri, index=False)
            failed_closed = client.get("/api/v1/portfolio").json()["data"]
            assert failed_closed["valuation_error"] == "SNAPSHOT_UNAVAILABLE"
            assert failed_closed["nav"] is None
            assert failed_closed["holdings"] == []
            tampered_backtest = client.get("/api/v1/backtests/latest").json()["data"]
            assert tampered_backtest["status"] == "LICENSE_BLOCKED_HISTORICAL_AUDIT_ONLY"
            assert tampered_backtest["display_blocked_reason"] == "SNAPSHOT_INTEGRITY_BLOCKED"
            assert tampered_backtest["metrics"] == {}
            assert tampered_backtest["baselines"] == {}
            assert tampered_backtest["periods"] == {}
            tampered_models = client.get("/api/v1/models").json()["data"]
            tampered_logistic = [
                row for row in tampered_models if row["model_name"] == "LogisticBaselineV1"
            ]
            assert tampered_logistic
            assert all(
                row["status"] == "LICENSE_BLOCKED_HISTORICAL_AUDIT_ONLY"
                and row["display_blocked_reason"] == "SNAPSHOT_INTEGRITY_BLOCKED"
                and row["metrics"] == {}
                and row["limitations"] == []
                for row in tampered_logistic
            )
    finally:
        app.dependency_overrides.clear()


def test_openapi_exposes_no_broker_order_or_trade_submission_endpoint() -> None:
    schema = app.openapi()
    paths = schema["paths"]
    lowered_paths = " ".join(paths).lower()

    assert "/order" not in lowered_paths
    assert "/trade" not in lowered_paths
    assert "/broker" not in lowered_paths
    mutating_operations = {
        (path, method)
        for path, operations in paths.items()
        for method in operations
        if method.lower() in {"post", "put", "patch", "delete"}
    }
    assert mutating_operations == {("/api/v1/alerts/{alert_id}/acknowledge", "post")}

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pandas as pd
import pytest
from sqlalchemy import func, select

from etf_sentinel.config import Settings
from etf_sentinel.enums import AlertStatus, DataMode, LicenseStatus, ModelStatus, SignalState
from etf_sentinel.models import (
    Alert,
    AuditLog,
    DataSnapshot,
    EtfInstrument,
    ModelVersion,
    NewsEvent,
    ProviderRegistry,
    Signal,
    SimulationFill,
)
from etf_sentinel.providers.base import LicenseGateError, ProviderSchemaError
from etf_sentinel.providers.demo import DemoFixtureProvider
from etf_sentinel.services import ledger as ledger_service
from etf_sentinel.services.alerts import (
    CANDIDATE_STATES,
    acknowledge_alert,
    create_signal_alerts,
    verify_alert_content,
)
from etf_sentinel.services.fact_integrity import verify_news_event_record
from etf_sentinel.services.ingestion import (
    SnapshotIntegrityError,
    _ingest_demo_news,
    canonical_frame_hash,
    register_default_providers,
)
from etf_sentinel.services.ledger import simulate_candidate_fills
from etf_sentinel.services.signals import (
    eligible_news_events,
    generate_signals,
    signal_record_hash,
    verify_signal_news_lineage,
)


def _seal_signal(signal: Signal) -> None:
    signal.feature_values = {
        **(signal.feature_values or {}),
        "signal_record_hash": signal_record_hash(signal),
    }


def _seed_single_demo_input(db_session, *, evaluation_time: datetime):
    register_default_providers(db_session)
    provider = DemoFixtureProvider()
    metadata = provider.fetch_etf_metadata()[0]
    instrument = EtfInstrument(**metadata)
    db_session.add(instrument)
    db_session.flush()
    identifier = provider.resolve_identifier(metadata["provider_symbol"])
    frame = provider.fetch_bars([identifier], date(2023, 11, 13), date(2025, 1, 31))
    snapshot = DataSnapshot(
        provider_code=provider.provider_code,
        dataset_type="MARKET_BARS",
        source_uri="internal://deterministic-demo-fixture/test",
        as_of=frame["as_of"].max(),
        available_at=frame["available_at"].max(),
        data_mode="DEMO_FIXTURE",
        license_scope="INTERNAL_TEST_AND_DEMONSTRATION_ONLY",
        parquet_uri="unused-signal-test.parquet",
        snapshot_hash=canonical_frame_hash(frame),
        config_hash="d" * 64,
        row_count=len(frame),
        quality_flags=["SYNTHETIC_NOT_MARKET_DATA"],
        revision="test-v1",
    )
    db_session.add(snapshot)
    db_session.commit()
    settings = Settings(
        _env_file=None,
        app_env="test",
        trading_mode="paper",
        demo_evaluation_time=evaluation_time,
        data_stale_after_minutes=60,
    )
    return instrument, snapshot, frame, settings


def test_stale_data_emits_only_health_alert_and_never_candidate_or_fill(db_session) -> None:
    evaluation_time = datetime(2025, 2, 10, 8, 0, tzinfo=UTC)
    _instrument, snapshot, frame, settings = _seed_single_demo_input(
        db_session, evaluation_time=evaluation_time
    )

    first_signals = generate_signals(
        db_session,
        snapshot=snapshot,
        frame=frame,
        evaluation_time=evaluation_time,
        settings=settings,
    )
    repeated_signals = generate_signals(
        db_session,
        snapshot=snapshot,
        frame=frame,
        evaluation_time=evaluation_time,
        settings=settings,
    )
    first_alerts = create_signal_alerts(
        db_session, first_signals, settings=settings, now=evaluation_time
    )
    repeated_alerts = create_signal_alerts(
        db_session, repeated_signals, settings=settings, now=evaluation_time
    )
    fills = simulate_candidate_fills(
        db_session,
        signals=first_signals,
        full_frame=frame,
        settings=settings,
    )

    assert {signal.state for signal in first_signals} == {SignalState.DATA_STALE.value}
    assert all(signal.state not in CANDIDATE_STATES for signal in first_signals)
    assert [signal.id for signal in repeated_signals] == [signal.id for signal in first_signals]
    assert {alert.alert_type for alert in first_alerts} == {"DATA_HEALTH"}
    assert [alert.id for alert in repeated_alerts] == [alert.id for alert in first_alerts]
    assert fills == []
    assert db_session.scalar(select(func.count(Signal.id))) == 1
    assert db_session.scalar(select(func.count(Alert.id))) == 1


def test_expired_provider_license_blocks_candidate_generation(db_session) -> None:
    evaluation_time = datetime(2025, 1, 31, 8, 0, tzinfo=UTC)
    _instrument, snapshot, frame, settings = _seed_single_demo_input(
        db_session, evaluation_time=evaluation_time
    )
    settings = settings.model_copy(update={"data_stale_after_minutes": 48 * 60})
    registry = db_session.scalar(
        select(ProviderRegistry).where(ProviderRegistry.provider_code == "demo_fixture")
    )
    assert registry is not None
    registry.expires_at = evaluation_time - timedelta(seconds=1)
    db_session.commit()

    with pytest.raises(LicenseGateError, match="授权已过期"):
        generate_signals(
            db_session,
            snapshot=snapshot,
            frame=frame,
            evaluation_time=evaluation_time,
            settings=settings,
        )

    db_session.rollback()
    assert db_session.scalar(select(func.count(Signal.id))) == 0
    assert db_session.scalar(select(func.count(SimulationFill.id))) == 0


def test_signal_generation_point_in_time_slice_ignores_future_bar_pollution(db_session) -> None:
    evaluation_time = datetime(2025, 1, 30, 8, 0, tzinfo=UTC)
    _instrument, snapshot, frame, settings = _seed_single_demo_input(
        db_session, evaluation_time=evaluation_time
    )
    [baseline] = generate_signals(
        db_session,
        snapshot=snapshot,
        frame=frame,
        evaluation_time=evaluation_time,
        settings=settings,
    )
    poisoned = frame.copy()
    future = pd.to_datetime(poisoned["event_time"], utc=True) > evaluation_time
    assert future.any()
    poisoned.loc[future, "open"] = 1_000_000_000.0
    poisoned.loc[future, "high"] = 1_000_000_000.0
    poisoned.loc[future, "low"] = 1_000_000_000.0
    poisoned.loc[future, "close"] = 1_000_000_000.0
    poisoned.loc[future, "adjusted_close"] = 1_000_000_000.0
    poisoned.loc[future, "total_return_close"] = 1_000_000_000.0
    poisoned.loc[future, "volume"] = 1.0
    snapshot_values = {
        column.name: getattr(snapshot, column.name)
        for column in DataSnapshot.__table__.columns
        if column.name not in {"id", "ingested_at"}
    }
    snapshot_values.update(
        {
            "snapshot_hash": canonical_frame_hash(poisoned),
            "row_count": len(poisoned),
            "parquet_uri": "unused-signal-poisoned-test.parquet",
        }
    )
    poisoned_snapshot = DataSnapshot(**snapshot_values)
    db_session.add(poisoned_snapshot)
    db_session.commit()

    [result] = generate_signals(
        db_session,
        snapshot=poisoned_snapshot,
        frame=poisoned,
        evaluation_time=evaluation_time,
        settings=settings,
    )

    assert result.state == baseline.state
    assert result.probability == pytest.approx(baseline.probability)
    assert result.confidence == pytest.approx(baseline.confidence)
    assert result.composite_score == pytest.approx(baseline.composite_score)
    baseline_features = dict(baseline.feature_values)
    result_features = dict(result.feature_values)
    for key in ("decision_policy_hash", "signal_record_hash"):
        baseline_features.pop(key)
        result_features.pop(key)
    assert result_features == baseline_features


def test_signal_generation_rejects_provider_pollution_even_with_matching_hash(db_session) -> None:
    evaluation_time = datetime(2025, 1, 30, 8, 0, tzinfo=UTC)
    _instrument, snapshot, frame, settings = _seed_single_demo_input(
        db_session, evaluation_time=evaluation_time
    )
    polluted = frame.copy()
    polluted["provider"] = "unregistered-provider"
    snapshot.snapshot_hash = canonical_frame_hash(polluted)
    snapshot.row_count = len(polluted)
    db_session.commit()

    with pytest.raises(ProviderSchemaError, match="供应商"):
        generate_signals(
            db_session,
            snapshot=snapshot,
            frame=polluted,
            evaluation_time=evaluation_time,
            settings=settings,
        )


def test_signal_generation_rejects_unclear_or_incomplete_fact_lineage(db_session) -> None:
    evaluation_time = datetime(2025, 1, 30, 8, 0, tzinfo=UTC)
    _instrument, snapshot, frame, settings = _seed_single_demo_input(
        db_session, evaluation_time=evaluation_time
    )
    snapshot.license_scope = "UNCLEAR"
    db_session.commit()
    with pytest.raises(LicenseGateError, match="许可范围"):
        generate_signals(
            db_session,
            snapshot=snapshot,
            frame=frame,
            evaluation_time=evaluation_time,
            settings=settings,
        )

    snapshot.license_scope = "INTERNAL_TEST_AND_DEMONSTRATION_ONLY"
    incomplete = frame.drop(columns=["published_at"])
    snapshot.snapshot_hash = canonical_frame_hash(incomplete)
    snapshot.row_count = len(incomplete)
    db_session.commit()
    with pytest.raises(ProviderSchemaError, match="缺少"):
        generate_signals(
            db_session,
            snapshot=snapshot,
            frame=incomplete,
            evaluation_time=evaluation_time,
            settings=settings,
        )


def test_alert_cooldown_prevents_repeat_delivery_for_same_instrument_event(db_session) -> None:
    first_time = datetime(2025, 2, 10, 8, 0, tzinfo=UTC)
    _instrument, snapshot, frame, settings = _seed_single_demo_input(
        db_session, evaluation_time=first_time
    )
    first_signal = generate_signals(
        db_session,
        snapshot=snapshot,
        frame=frame,
        evaluation_time=first_time,
        settings=settings,
    )
    first_alert = create_signal_alerts(db_session, first_signal, settings=settings, now=first_time)
    assert len(first_alert) == 1
    assert first_alert[0].status == AlertStatus.SENT.value
    assert first_alert[0].provider_code == "demo_fixture"
    assert first_alert[0].source_object_type == "Signal"
    assert first_alert[0].source_object_id == first_signal[0].id
    assert verify_alert_content(first_alert[0])
    acknowledge_alert(db_session, first_alert[0].id, actor="test-reviewer")
    assert first_alert[0].status == AlertStatus.ACKNOWLEDGED.value

    repeat_time = first_time + timedelta(minutes=1)
    repeated_signal = generate_signals(
        db_session,
        snapshot=snapshot,
        frame=frame,
        evaluation_time=repeat_time,
        settings=settings,
    )
    repeated_alert = create_signal_alerts(
        db_session, repeated_signal, settings=settings, now=repeat_time
    )
    assert [alert.id for alert in repeated_alert] == [alert.id for alert in first_alert]
    assert repeated_alert[0].status == AlertStatus.ACKNOWLEDGED.value
    assert db_session.scalar(select(func.count(Alert.id))) == 1
    audit = db_session.scalar(
        select(AuditLog).where(
            AuditLog.event_type == "ALERT_ACKNOWLEDGED",
            AuditLog.object_id == first_alert[0].id,
        )
    )
    assert audit is not None
    assert audit.details["previous_status"] == AlertStatus.SENT.value


def test_new_policy_supersedes_prior_signal_across_time_and_kill_switch_has_no_candidate(
    db_session,
) -> None:
    first_time = datetime(2025, 1, 31, 8, 0, tzinfo=UTC)
    instrument, snapshot, frame, settings = _seed_single_demo_input(
        db_session, evaluation_time=first_time
    )
    settings = settings.model_copy(update={"data_stale_after_minutes": 48 * 60})
    [first] = generate_signals(
        db_session,
        snapshot=snapshot,
        frame=frame,
        evaluation_time=first_time,
        settings=settings,
    )
    blocked_settings = settings.model_copy(update={"global_kill_switch": True})

    [second] = generate_signals(
        db_session,
        snapshot=snapshot,
        frame=frame,
        evaluation_time=first_time + timedelta(days=1),
        settings=blocked_settings,
    )

    versions = list(
        db_session.scalars(
            select(Signal)
            .where(Signal.instrument_id == instrument.id, Signal.horizon_days == 20)
            .order_by(Signal.generated_at)
        )
    )
    assert [item.id for item in versions] == [first.id, second.id]
    assert first.is_current is False
    assert second.is_current is True
    assert second.state == SignalState.BLOCKED_BY_RISK.value
    assert "GLOBAL_KILL_SWITCH" in second.risk_rules_hit
    assert sum(item.is_current for item in versions) == 1
    assert (
        simulate_candidate_fills(
            db_session,
            signals=versions,
            full_frame=frame,
            settings=blocked_settings,
        )
        == []
    )


def test_generate_signals_applies_current_portfolio_cap_and_suppresses_instrument_alert(
    db_session,
) -> None:
    first_time = datetime(2025, 1, 30, 8, 0, tzinfo=UTC)
    _instrument, snapshot, frame, settings = _seed_single_demo_input(
        db_session, evaluation_time=first_time
    )
    settings = settings.model_copy(update={"data_stale_after_minutes": 48 * 60})
    [opening_signal] = generate_signals(
        db_session,
        snapshot=snapshot,
        frame=frame,
        evaluation_time=first_time,
        settings=settings,
    )
    opening_signal.state = SignalState.ENTRY_CANDIDATE.value
    opening_signal.risk_rules_hit = []
    opening_signal.suggested_risk_budget_min = 0.05
    opening_signal.suggested_risk_budget_max = 0.10
    _seal_signal(opening_signal)
    db_session.commit()
    assert simulate_candidate_fills(
        db_session,
        signals=[opening_signal],
        full_frame=frame,
        settings=settings,
    )
    fill_count = db_session.scalar(select(func.count(SimulationFill.id)))

    second_time = datetime(2025, 1, 31, 8, 0, tzinfo=UTC)
    strict_settings = settings.model_copy(
        update={
            "demo_evaluation_time": second_time,
            "single_etf_cap": 0.000001,
        }
    )
    [blocked] = generate_signals(
        db_session,
        snapshot=snapshot,
        frame=frame,
        evaluation_time=second_time,
        settings=strict_settings,
    )

    assert opening_signal.is_current is False
    assert blocked.is_current is True
    assert blocked.state == SignalState.BLOCKED_BY_RISK.value
    assert blocked.suggested_risk_budget_min == 0
    assert blocked.suggested_risk_budget_max == 0
    assert any(str(rule).startswith("PORTFOLIO_SINGLE_ETF_CAP:") for rule in blocked.risk_rules_hit)
    assert blocked.feature_values["portfolio_risk_rules"] == blocked.risk_rules_hit
    assert blocked.feature_values["portfolio_risk_context_hash"]
    assert (
        create_signal_alerts(
            db_session,
            [blocked],
            settings=strict_settings,
            now=second_time,
        )
        == []
    )
    assert db_session.scalar(select(func.count(SimulationFill.id))) == fill_count


def test_signal_generation_rechecks_portfolio_gate_before_persisting(
    db_session, monkeypatch
) -> None:
    evaluation_time = datetime(2025, 1, 30, 8, 0, tzinfo=UTC)
    _instrument, snapshot, frame, settings = _seed_single_demo_input(
        db_session, evaluation_time=evaluation_time
    )
    original_gate = ledger_service.assess_portfolio_risk_gate
    call_count = 0

    def changing_gate(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        gate = original_gate(*args, **kwargs)
        if call_count == 2:
            return type(gate)(
                rules=["PORTFOLIO_MAX_SIMULATION_LOSS"],
                context_hash="f" * 64,
                nav=gate.nav,
                max_drawdown=gate.max_drawdown,
            )
        return gate

    monkeypatch.setattr(ledger_service, "assess_portfolio_risk_gate", changing_gate)

    with pytest.raises(SnapshotIntegrityError, match="提交前组合风险门禁已变化"):
        generate_signals(
            db_session,
            snapshot=snapshot,
            frame=frame,
            evaluation_time=evaluation_time,
            settings=settings,
        )

    assert call_count == 2
    db_session.rollback()
    assert db_session.scalar(select(func.count(Signal.id))) == 0


def test_news_factor_excludes_data_mode_mismatch_and_revoked_provider_license(db_session) -> None:
    evaluation_time = datetime(2025, 1, 31, 8, 0, tzinfo=UTC)
    _instrument, snapshot, _frame, settings = _seed_single_demo_input(
        db_session, evaluation_time=evaluation_time
    )
    _ingest_demo_news(db_session, DemoFixtureProvider(), settings)
    db_session.commit()
    events = list(db_session.scalars(select(NewsEvent)))
    assert events
    assert eligible_news_events(db_session, snapshot=snapshot, evaluation_time=evaluation_time)

    original_modes = {event.id: event.data_mode for event in events}
    for event in events:
        event.data_mode = DataMode.DELAYED.value
    db_session.commit()
    assert (
        eligible_news_events(db_session, snapshot=snapshot, evaluation_time=evaluation_time) == []
    )

    for event in events:
        event.data_mode = original_modes[event.id]
    registry = db_session.scalar(
        select(ProviderRegistry).where(ProviderRegistry.provider_code == "demo_fixture")
    )
    assert registry is not None
    registry.review_status = LicenseStatus.BLOCKED.value
    db_session.commit()
    assert (
        eligible_news_events(db_session, snapshot=snapshot, evaluation_time=evaluation_time) == []
    )


def test_newly_available_news_creates_new_signal_version_and_supersedes_old(db_session) -> None:
    evaluation_time = datetime(2025, 1, 31, 8, 0, tzinfo=UTC)
    _instrument, snapshot, frame, settings = _seed_single_demo_input(
        db_session, evaluation_time=evaluation_time
    )
    settings = settings.model_copy(update={"data_stale_after_minutes": 48 * 60})
    [without_news] = generate_signals(
        db_session,
        snapshot=snapshot,
        frame=frame,
        evaluation_time=evaluation_time,
        settings=settings,
    )
    assert without_news.feature_values["news_lineage"]["status"] == "NONE_AVAILABLE"

    _ingest_demo_news(db_session, DemoFixtureProvider(), settings)
    db_session.commit()
    [with_news] = generate_signals(
        db_session,
        snapshot=snapshot,
        frame=frame,
        evaluation_time=evaluation_time,
        settings=settings,
    )

    assert with_news.id != without_news.id
    assert without_news.is_current is False
    assert with_news.is_current is True
    assert with_news.feature_values["news_lineage"]["status"] == "AVAILABLE"
    assert with_news.feature_values["news_lineage"]["events"]
    assert (
        with_news.feature_values["news_lineage"]["lineage_hash"]
        != (without_news.feature_values["news_lineage"]["lineage_hash"])
    )
    assert (
        with_news.feature_values["decision_policy_hash"]
        != (without_news.feature_values["decision_policy_hash"])
    )


def test_deleted_news_source_blocks_existing_candidate_execution(db_session) -> None:
    evaluation_time = datetime(2025, 1, 31, 8, 0, tzinfo=UTC)
    _instrument, snapshot, frame, settings = _seed_single_demo_input(
        db_session, evaluation_time=evaluation_time
    )
    settings = settings.model_copy(update={"data_stale_after_minutes": 48 * 60})
    _ingest_demo_news(db_session, DemoFixtureProvider(), settings)
    db_session.commit()
    [signal] = generate_signals(
        db_session,
        snapshot=snapshot,
        frame=frame,
        evaluation_time=evaluation_time,
        settings=settings,
    )
    signal.state = SignalState.ENTRY_CANDIDATE.value
    signal.risk_rules_hit = []
    _seal_signal(signal)
    sources = eligible_news_events(
        db_session,
        snapshot=snapshot,
        evaluation_time=evaluation_time,
    )
    assert sources
    source = sources[0]
    db_session.delete(source)
    db_session.commit()

    assert (
        simulate_candidate_fills(
            db_session,
            signals=[signal],
            full_frame=frame,
            settings=settings,
        )
        == []
    )
    assert db_session.scalar(select(func.count(SimulationFill.id))) == 0
    blocked = list(
        db_session.scalars(
            select(AuditLog.details).where(AuditLog.event_type == "SIMULATION_FILL_BLOCKED")
        )
    )
    assert any(details.get("rule") == "NEWS_LINEAGE_BLOCKED_AT_EXECUTION" for details in blocked)


def test_tampered_news_fact_blocks_existing_lineage_and_new_signal_generation(
    db_session,
) -> None:
    evaluation_time = datetime(2025, 1, 31, 8, 0, tzinfo=UTC)
    _instrument, snapshot, frame, settings = _seed_single_demo_input(
        db_session, evaluation_time=evaluation_time
    )
    settings = settings.model_copy(update={"data_stale_after_minutes": 48 * 60})
    _ingest_demo_news(db_session, DemoFixtureProvider(), settings)
    db_session.commit()
    [existing_signal] = generate_signals(
        db_session,
        snapshot=snapshot,
        frame=frame,
        evaluation_time=evaluation_time,
        settings=settings,
    )
    sources = eligible_news_events(
        db_session,
        snapshot=snapshot,
        evaluation_time=evaluation_time,
    )
    assert sources and all(verify_news_event_record(source) for source in sources)
    source = sources[0]
    source.title = f"{source.title}（未授权篡改）"
    db_session.commit()

    assert not verify_news_event_record(source)
    with pytest.raises(SnapshotIntegrityError, match="新闻结构化事实记录哈希"):
        verify_signal_news_lineage(db_session, existing_signal, snapshot=snapshot)
    with pytest.raises(SnapshotIntegrityError, match="新闻结构化事实记录哈希"):
        generate_signals(
            db_session,
            snapshot=snapshot,
            frame=frame,
            evaluation_time=evaluation_time + timedelta(minutes=1),
            settings=settings,
        )
    with pytest.raises(SnapshotIntegrityError, match="同一新闻去重键"):
        _ingest_demo_news(db_session, DemoFixtureProvider(), settings)

    db_session.rollback()
    assert db_session.scalar(select(func.count(Signal.id))) == 1


def test_candidate_alert_requires_an_actual_state_transition(db_session) -> None:
    now = datetime(2025, 1, 31, 8, 0, tzinfo=UTC)
    _instrument, snapshot, frame, settings = _seed_single_demo_input(
        db_session, evaluation_time=now
    )
    settings = settings.model_copy(
        update={"data_stale_after_minutes": 48 * 60, "alert_cooldown_minutes": 0}
    )
    [first] = generate_signals(
        db_session,
        snapshot=snapshot,
        frame=frame,
        evaluation_time=now,
        settings=settings,
    )
    first.state = SignalState.ENTRY_CANDIDATE.value
    first.recorded_at = now
    _seal_signal(first)
    db_session.commit()
    first_alerts = create_signal_alerts(db_session, [first], settings=settings, now=now)
    assert [alert.alert_type for alert in first_alerts] == ["SIGNAL_STATE_CHANGE"]

    def clone_signal(*, key: str, state: SignalState, at: datetime) -> Signal:
        values = {
            column.name: getattr(first, column.name)
            for column in Signal.__table__.columns
            if column.name not in {"id", "recorded_at"}
        }
        values.update(
            {
                "idempotency_key": key,
                "state": state.value,
                "generated_at": at,
                "recorded_at": at,
                "is_current": True,
            }
        )
        row = Signal(**values)
        _seal_signal(row)
        db_session.add(row)
        db_session.commit()
        return row

    unchanged = clone_signal(
        key="f" * 64,
        state=SignalState.ENTRY_CANDIDATE,
        at=now + timedelta(minutes=1),
    )
    assert (
        create_signal_alerts(
            db_session, [unchanged], settings=settings, now=now + timedelta(minutes=1)
        )
        == []
    )

    crossed = clone_signal(
        key="e" * 64,
        state=SignalState.REDUCE_CANDIDATE,
        at=now + timedelta(minutes=2),
    )
    crossed_alerts = create_signal_alerts(
        db_session, [crossed], settings=settings, now=now + timedelta(minutes=2)
    )
    assert len(crossed_alerts) == 1
    assert crossed_alerts[0].alert_type == "SIGNAL_STATE_CHANGE"
    assert crossed_alerts[0].signal_id == crossed.id


@pytest.mark.parametrize(
    ("attribute", "value", "expected_rule"),
    [
        ("status", ModelStatus.REJECTED.value, "MODEL_STATUS_REJECTED"),
        (
            "feature_version",
            "unexpected-feature-version",
            "MODEL_FEATURE_VERSION_UNEXPECTED",
        ),
    ],
)
def test_rule_model_governance_change_blocks_old_candidate_and_supersedes_on_regeneration(
    db_session,
    attribute: str,
    value: str,
    expected_rule: str,
) -> None:
    evaluation_time = datetime(2025, 1, 31, 8, 0, tzinfo=UTC)
    _instrument, snapshot, frame, settings = _seed_single_demo_input(
        db_session, evaluation_time=evaluation_time
    )
    settings = settings.model_copy(update={"data_stale_after_minutes": 48 * 60})
    [old_signal] = generate_signals(
        db_session,
        snapshot=snapshot,
        frame=frame,
        evaluation_time=evaluation_time,
        settings=settings,
    )
    old_signal.state = SignalState.ENTRY_CANDIDATE.value
    old_signal.risk_rules_hit = []
    _seal_signal(old_signal)
    db_session.commit()
    model = db_session.get(ModelVersion, old_signal.model_version_id)
    assert model is not None
    setattr(model, attribute, value)
    db_session.commit()

    fill_count = db_session.scalar(select(func.count(SimulationFill.id)))
    assert (
        simulate_candidate_fills(
            db_session,
            signals=[old_signal],
            full_frame=frame,
            settings=settings,
        )
        == []
    )
    assert db_session.scalar(select(func.count(SimulationFill.id))) == fill_count
    governance_audits = list(
        db_session.scalars(
            select(AuditLog.details).where(AuditLog.event_type == "SIMULATION_FILL_BLOCKED")
        )
    )
    assert any(
        details.get("rule") == "MODEL_GOVERNANCE_BLOCKED"
        and expected_rule in details.get("model_rules", [])
        for details in governance_audits
    )

    [replacement] = generate_signals(
        db_session,
        snapshot=snapshot,
        frame=frame,
        evaluation_time=evaluation_time + timedelta(minutes=1),
        settings=settings,
    )
    assert old_signal.is_current is False
    assert replacement.is_current is True
    assert replacement.state == SignalState.BLOCKED_BY_RISK.value
    assert expected_rule in replacement.risk_rules_hit
    assert replacement.id != old_signal.id

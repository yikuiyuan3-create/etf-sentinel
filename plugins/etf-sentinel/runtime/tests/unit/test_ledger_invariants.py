from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pandas as pd
import pytest
from sqlalchemy import func, select

from etf_sentinel.enums import DataMode, ModelStatus, SignalState
from etf_sentinel.models import (
    AuditLog,
    DataSnapshot,
    EtfInstrument,
    ModelVersion,
    ProviderRegistry,
    Signal,
    SimulationDecision,
    SimulationFill,
    SimulationLedger,
)
from etf_sentinel.services import ledger as ledger_service
from etf_sentinel.services.ingestion import (
    SnapshotIntegrityError,
    canonical_frame_hash,
    register_default_providers,
)
from etf_sentinel.services.ledger import (
    SimulationLedgerIntegrityError,
    apply_corporate_actions,
    assess_portfolio_risk_gate,
    ensure_opening_cash,
    next_executable_bar,
    portfolio_analytics,
    portfolio_state,
    seal_simulation_ledger_entry,
    simulate_candidate_fills,
)
from etf_sentinel.services.signals import (
    build_news_lineage,
    decision_policy_hash,
    signal_record_hash,
)


def _seal_signal(signal: Signal) -> None:
    signal.feature_values = {
        **(signal.feature_values or {}),
        "signal_record_hash": signal_record_hash(signal),
    }


def _policy_hash(
    db_session,
    snapshot: DataSnapshot,
    settings,
    *,
    portfolio_context_hash: str | None = None,
) -> str:
    news_lineage = build_news_lineage([])
    if portfolio_context_hash is None:
        portfolio_context_hash = assess_portfolio_risk_gate(
            db_session,
            None,
            settings=settings,
        ).context_hash
    return decision_policy_hash(
        db_session,
        snapshot=snapshot,
        settings=settings,
        news_lineage_hash=str(news_lineage["lineage_hash"]),
        portfolio_risk_context_hash=portfolio_context_hash,
    )


def _seed_candidate(db_session, settings, *, fx_rate: float = 1.0) -> tuple[Signal, pd.DataFrame]:
    register_default_providers(db_session)
    instrument = EtfInstrument(
        id="instrument-1",
        name_zh="测试ETF",
        figi="TESTFIGI0001",
        isin="DM0000000001",
        mic="XDEM",
        currency="CNY",
        exchange="测试交易所",
        provider_code="demo_fixture",
        provider_symbol="DEMO-001",
        asset_class="股票",
        industry="宽基",
        region="中国",
        leveraged=False,
        inverse=False,
        active=True,
        inception_date=date(2020, 1, 1),
        liquidity_tier=1,
    )
    snapshot = DataSnapshot(
        provider_code="demo_fixture",
        dataset_type="MARKET_BARS",
        source_uri="internal://test",
        as_of=datetime(2025, 1, 2, 7, 0, tzinfo=UTC),
        available_at=datetime(2025, 1, 2, 7, 10, tzinfo=UTC),
        data_mode=DataMode.DEMO_FIXTURE.value,
        license_scope="INTERNAL_TEST_AND_DEMONSTRATION_ONLY",
        parquet_uri="unused-ledger.parquet",
        snapshot_hash="a" * 64,
        config_hash="b" * 64,
        row_count=2,
        quality_flags=[],
        revision="test-v1",
    )
    model = ModelVersion(
        model_name="RuleBasedV1",
        version="1.0.0",
        status=ModelStatus.EXPERIMENTAL.value,
        feature_version="rule-features-v1",
        metrics={
            "governance_scope": "DEMO_FIXTURE_ONLY",
            "real_data_candidate_gate": "BLOCKED",
            "drift_status": "NOT_APPLICABLE_TO_FIXED_FIXTURE",
        },
        limitations=[],
    )
    db_session.add_all([instrument, snapshot, model])
    db_session.flush()
    news_lineage = build_news_lineage([])
    portfolio_gate = assess_portfolio_risk_gate(db_session, None, settings=settings)
    policy_hash = _policy_hash(
        db_session,
        snapshot,
        settings,
        portfolio_context_hash=portfolio_gate.context_hash,
    )
    signal = Signal(
        idempotency_key="candidate-signal-key",
        instrument_id=instrument.id,
        data_snapshot_id=snapshot.id,
        model_version_id=model.id,
        data_as_of=datetime(2025, 1, 2, 7, 0, tzinfo=UTC),
        available_at=datetime(2025, 1, 2, 7, 10, tzinfo=UTC),
        generated_at=datetime(2025, 1, 2, 8, 0, tzinfo=UTC),
        horizon_days=20,
        state=SignalState.ENTRY_CANDIDATE.value,
        probability=0.7,
        confidence=0.6,
        confidence_interval=[0.5, 0.8],
        market_score=0.6,
        macro_score=0.0,
        news_score=0.0,
        liquidity_score=0.9,
        composite_score=0.5,
        supporting_evidence=[],
        opposing_evidence=[],
        source_links=["internal://test"],
        feature_values={
            "decision_policy_hash": policy_hash,
            "macro_lineage": {"status": "UNAVAILABLE_REWEIGHTED"},
            "news_lineage": news_lineage,
            "portfolio_risk_context_hash": portfolio_gate.context_hash,
            "portfolio_risk_rules": portfolio_gate.rules,
        },
        risk_rules_hit=[],
        trigger_conditions=["test"],
        invalidation_conditions=["data stale"],
        suggested_risk_budget_min=0.05,
        suggested_risk_budget_max=0.10,
        limitations=["test fixture"],
        data_mode=DataMode.DEMO_FIXTURE.value,
        latency_status="ON_TIME",
        code_version="test",
        is_current=True,
    )
    db_session.add(signal)
    db_session.flush()
    frame = pd.DataFrame(
        [
            {
                "instrument_id": instrument.id,
                "event_time": datetime(2025, 1, 2, 7, 0, tzinfo=UTC),
                "open": 77.0,
                "close": 109.0,
                "fx_rate": fx_rate,
                "published_at": datetime(2025, 1, 2, 7, 10, tzinfo=UTC),
                "first_seen_at": datetime(2025, 1, 2, 7, 10, tzinfo=UTC),
                "available_at": datetime(2025, 1, 2, 7, 10, tzinfo=UTC),
                "as_of": datetime(2025, 1, 2, 7, 0, tzinfo=UTC),
                "revision": "test-v1",
                "split_factor": 1.0,
                "dividend": 0.0,
                "provider": "demo_fixture",
                "license_scope": "INTERNAL_TEST_AND_DEMONSTRATION_ONLY",
                "data_mode": DataMode.DEMO_FIXTURE.value,
            },
            {
                "instrument_id": instrument.id,
                "event_time": datetime(2025, 1, 3, 7, 0, tzinfo=UTC),
                "open": 113.0,
                "close": 114.0,
                "fx_rate": fx_rate,
                "published_at": datetime(2025, 1, 3, 7, 10, tzinfo=UTC),
                "first_seen_at": datetime(2025, 1, 3, 7, 10, tzinfo=UTC),
                "available_at": datetime(2025, 1, 3, 7, 10, tzinfo=UTC),
                "as_of": datetime(2025, 1, 3, 7, 0, tzinfo=UTC),
                "revision": "test-v1",
                "split_factor": 1.0,
                "dividend": 0.0,
                "provider": "demo_fixture",
                "license_scope": "INTERNAL_TEST_AND_DEMONSTRATION_ONLY",
                "data_mode": DataMode.DEMO_FIXTURE.value,
            },
        ]
    )
    snapshot.snapshot_hash = canonical_frame_hash(frame)
    snapshot.row_count = len(frame)
    signal.feature_values = {
        **signal.feature_values,
        "decision_policy_hash": _policy_hash(db_session, snapshot, settings),
    }
    _seal_signal(signal)
    db_session.commit()
    return signal, frame


def _seed_two_held_instruments(db_session, settings) -> tuple[str, str]:
    """Create two holdings through valid Signal→Decision→Fill→Ledger chains."""
    first_signal, first_frame = _seed_candidate(db_session, settings)
    first_instrument = db_session.get(EtfInstrument, first_signal.instrument_id)
    snapshot = db_session.get(DataSnapshot, first_signal.data_snapshot_id)
    assert first_instrument is not None and snapshot is not None
    second_instrument_id = "held-instrument-b"
    instrument_values = {
        column.name: getattr(first_instrument, column.name)
        for column in EtfInstrument.__table__.columns
        if column.name != "id"
    }
    instrument_values.update(
        {
            "id": second_instrument_id,
            "name_zh": "估值测试ETF B",
            "figi": "HELDTESTFIGI00000002",
            "isin": "DMH000000002",
            "provider_symbol": "HELD-002",
        }
    )
    db_session.add(EtfInstrument(**instrument_values))
    signal_values = {
        column.name: getattr(first_signal, column.name)
        for column in Signal.__table__.columns
        if column.name not in {"id", "recorded_at"}
    }
    signal_values.update(
        {
            "idempotency_key": "candidate-signal-key-held-b",
            "instrument_id": second_instrument_id,
            "composite_score": first_signal.composite_score - 0.01,
        }
    )
    second_signal = Signal(**signal_values)
    db_session.add(second_signal)
    second_frame = first_frame.copy()
    second_frame["instrument_id"] = second_instrument_id
    full_frame = pd.concat([first_frame, second_frame], ignore_index=True)
    snapshot.snapshot_hash = canonical_frame_hash(full_frame)
    snapshot.row_count = len(full_frame)
    context_hash = str(first_signal.feature_values["portfolio_risk_context_hash"])
    policy_hash = _policy_hash(
        db_session,
        snapshot,
        settings,
        portfolio_context_hash=context_hash,
    )
    for signal in (first_signal, second_signal):
        signal.feature_values = {
            **signal.feature_values,
            "decision_policy_hash": policy_hash,
        }
        _seal_signal(signal)
    db_session.commit()
    fills = simulate_candidate_fills(
        db_session,
        signals=[first_signal, second_signal],
        full_frame=full_frame,
        settings=settings,
    )
    assert {fill.instrument_id for fill in fills} == {
        first_signal.instrument_id,
        second_signal.instrument_id,
    }
    return first_signal.instrument_id, second_signal.instrument_id


def _valuation_fact(instrument_id: str, event_time: datetime, close: float) -> dict[str, object]:
    learned_at = event_time + timedelta(minutes=10)
    return {
        "instrument_id": instrument_id,
        "event_time": event_time,
        "published_at": learned_at,
        "first_seen_at": learned_at,
        "available_at": learned_at,
        "as_of": event_time,
        "revision": "valuation-v1",
        "close": close,
        "fx_rate": 1.0,
        "data_mode": DataMode.DELAYED.value,
    }


def _seed_skipped_no_next_bar_decision(db_session, settings) -> SimulationDecision:
    signal, frame = _seed_candidate(db_session, settings)
    no_next_bar = frame.iloc[[0]].copy()
    snapshot = db_session.get(DataSnapshot, signal.data_snapshot_id)
    assert snapshot is not None
    snapshot.snapshot_hash = canonical_frame_hash(no_next_bar)
    snapshot.row_count = len(no_next_bar)
    signal.feature_values = {
        **signal.feature_values,
        "decision_policy_hash": _policy_hash(
            db_session,
            snapshot,
            settings,
            portfolio_context_hash=str(signal.feature_values["portfolio_risk_context_hash"]),
        ),
    }
    _seal_signal(signal)
    db_session.commit()
    assert (
        simulate_candidate_fills(
            db_session,
            signals=[signal],
            full_frame=no_next_bar,
            settings=settings,
        )
        == []
    )
    decision = db_session.scalar(select(SimulationDecision))
    assert decision is not None
    assert decision.status == "SKIPPED_NO_NEXT_BAR"
    return decision


@pytest.mark.parametrize(
    ("attribute", "tampered_value"),
    [
        ("target_weight", 0.99),
        ("status", "SKIPPED_BELOW_LOT"),
        ("rationale", "tampered persisted rationale"),
    ],
)
def test_skipped_decision_content_mutation_is_detected(
    db_session,
    test_settings,
    attribute: str,
    tampered_value,
) -> None:
    decision = _seed_skipped_no_next_bar_decision(db_session, test_settings)
    setattr(decision, attribute, tampered_value)
    db_session.commit()

    with pytest.raises(SimulationLedgerIntegrityError, match="决策"):
        portfolio_state(db_session)


def test_deleting_skipped_decision_audit_is_detected(db_session, test_settings) -> None:
    decision = _seed_skipped_no_next_bar_decision(db_session, test_settings)
    decision_audit = db_session.scalar(
        select(AuditLog).where(
            AuditLog.event_type == "SIMULATION_DECISION_RECORDED",
            AuditLog.object_id == decision.id,
        )
    )
    assert decision_audit is not None
    db_session.execute(AuditLog.__table__.delete().where(AuditLog.id == decision_audit.id))
    db_session.commit()

    with pytest.raises(SimulationLedgerIntegrityError, match="决策"):
        portfolio_state(db_session)


def test_close_signal_executes_only_at_next_available_bar_open(db_session, test_settings) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)

    fills = simulate_candidate_fills(
        db_session,
        signals=[signal],
        full_frame=frame,
        settings=test_settings,
        slippage_bps=0.0,
    )

    assert len(fills) == 1
    fill = fills[0]
    assert fill.executable_price == pytest.approx(113.0)
    assert fill.executable_price != 109.0
    filled_at = pd.Timestamp(fill.filled_at)
    available_at = pd.Timestamp(signal.available_at)
    filled_at = (
        filled_at.tz_localize("UTC") if filled_at.tzinfo is None else filled_at.tz_convert("UTC")
    )
    available_at = (
        available_at.tz_localize("UTC")
        if available_at.tzinfo is None
        else available_at.tz_convert("UTC")
    )
    assert filled_at > available_at
    assert fill.filled_at.date() == date(2025, 1, 3)


def test_missing_bar_and_holiday_gap_choose_first_later_available_bar() -> None:
    instrument_id = "instrument-1"
    frame = pd.DataFrame(
        [
            {"instrument_id": instrument_id, "event_time": "2025-01-24T07:00:00Z", "open": 10},
            # Spring Festival and a deliberately missing bar leave a long calendar gap.
            {"instrument_id": instrument_id, "event_time": "2025-02-05T07:00:00Z", "open": 11},
            {"instrument_id": instrument_id, "event_time": "2025-02-06T07:00:00Z", "open": 12},
        ]
    )
    frame["published_at"] = pd.to_datetime(frame["event_time"], utc=True)
    frame["first_seen_at"] = pd.to_datetime(frame["event_time"], utc=True)
    frame["available_at"] = pd.to_datetime(frame["event_time"], utc=True)
    frame["as_of"] = pd.to_datetime(frame["event_time"], utc=True)

    selected = next_executable_bar(frame, instrument_id, datetime(2025, 1, 24, 7, 0, tzinfo=UTC))

    assert selected is not None
    assert selected["open"] == 11
    assert pd.Timestamp(selected["event_time"]) == pd.Timestamp("2025-02-05T07:00:00Z")


def test_cross_timezone_signal_clock_still_selects_strictly_later_bar() -> None:
    instrument_id = "instrument-1"
    frame = pd.DataFrame(
        [
            {"instrument_id": instrument_id, "event_time": "2025-01-02T07:00:00Z", "open": 10},
            {"instrument_id": instrument_id, "event_time": "2025-01-03T07:00:00Z", "open": 11},
        ]
    )
    frame["published_at"] = pd.to_datetime(frame["event_time"], utc=True)
    frame["first_seen_at"] = pd.to_datetime(frame["event_time"], utc=True)
    frame["available_at"] = pd.to_datetime(frame["event_time"], utc=True)
    frame["as_of"] = pd.to_datetime(frame["event_time"], utc=True)
    signal_close_shanghai = pd.Timestamp("2025-01-02T15:00:00+08:00").to_pydatetime()

    selected = next_executable_bar(frame, instrument_id, signal_close_shanghai)

    assert selected is not None
    assert selected["open"] == 11


def test_future_available_poisoned_bar_is_skipped_for_next_execution() -> None:
    instrument_id = "instrument-1"
    frame = pd.DataFrame(
        [
            {"instrument_id": instrument_id, "event_time": "2025-01-02T07:00:00Z", "open": 10},
            {
                "instrument_id": instrument_id,
                "event_time": "2025-01-03T07:00:00Z",
                "open": 1_000_000_000,
            },
            {"instrument_id": instrument_id, "event_time": "2025-01-06T07:00:00Z", "open": 12},
        ]
    )
    for column in ("published_at", "first_seen_at", "available_at", "as_of"):
        frame[column] = pd.to_datetime(frame["event_time"], utc=True)
    frame.loc[1, "available_at"] = datetime(2030, 1, 1, tzinfo=UTC)

    selected = next_executable_bar(
        frame,
        instrument_id,
        datetime(2025, 1, 2, 7, 0, tzinfo=UTC),
    )

    assert selected is not None
    assert selected["open"] == 12
    assert pd.Timestamp(selected["event_time"]) == pd.Timestamp("2025-01-06T07:00:00Z")


@pytest.mark.parametrize("fx_rate", [1.0, 0.92, 7.2])
def test_ledger_cash_position_and_fee_conserve_opening_value(
    db_session, test_settings, fx_rate: float
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings, fx_rate=fx_rate)
    initial_cash = 1_000_000.0

    [fill] = simulate_candidate_fills(
        db_session,
        signals=[signal],
        full_frame=frame,
        settings=test_settings,
        initial_cash=initial_cash,
        fee_bps=2.5,
        slippage_bps=3.0,
    )
    state = portfolio_state(db_session)
    marked_value = state.positions[signal.instrument_id] * fill.executable_price * fill.fx_rate

    assert state.cash + marked_value + state.total_fees == pytest.approx(initial_cash)
    assert fill.gross_amount == pytest.approx(marked_value)
    assert state.total_fees == pytest.approx(fill.fee)
    allocation_cap = min(signal.suggested_risk_budget_max, test_settings.single_etf_cap)
    assert fill.gross_amount + fill.fee <= initial_cash * allocation_cap + 1e-6


def test_repeated_fill_task_is_idempotent_across_decision_fill_and_ledger(
    db_session, test_settings
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)

    first = simulate_candidate_fills(
        db_session, signals=[signal], full_frame=frame, settings=test_settings
    )
    second = simulate_candidate_fills(
        db_session, signals=[signal], full_frame=frame, settings=test_settings
    )

    assert [item.id for item in second] == [item.id for item in first]
    assert db_session.scalar(select(func.count(SimulationDecision.id))) == 1
    assert db_session.scalar(select(func.count(SimulationFill.id))) == 1
    # One opening-cash entry and one buy entry; neither may duplicate.
    assert db_session.scalar(select(func.count(SimulationLedger.id))) == 2


def test_cross_version_signal_at_same_business_time_cannot_duplicate_decision_or_fill(
    db_session, test_settings
) -> None:
    original, frame = _seed_candidate(db_session, test_settings)
    assert simulate_candidate_fills(
        db_session,
        signals=[original],
        full_frame=frame,
        settings=test_settings,
    )
    decision_count = db_session.scalar(select(func.count(SimulationDecision.id)))
    fill_count = db_session.scalar(select(func.count(SimulationFill.id)))
    ledger_count = db_session.scalar(select(func.count(SimulationLedger.id)))
    original.is_current = False
    values = {
        column.name: getattr(original, column.name)
        for column in Signal.__table__.columns
        if column.name not in {"id", "recorded_at"}
    }
    values.update(
        {
            "idempotency_key": "cross-version-signal-key",
            "code_version": "different-code-version",
            "is_current": True,
        }
    )
    replay = Signal(**values)
    _seal_signal(replay)
    db_session.add(replay)
    db_session.commit()

    assert (
        simulate_candidate_fills(
            db_session,
            signals=[replay],
            full_frame=frame,
            settings=test_settings,
        )
        == []
    )
    assert db_session.scalar(select(func.count(SimulationDecision.id))) == decision_count
    assert db_session.scalar(select(func.count(SimulationFill.id))) == fill_count
    assert db_session.scalar(select(func.count(SimulationLedger.id))) == ledger_count
    replay_audit = db_session.scalar(
        select(AuditLog).where(
            AuditLog.event_type == "SIMULATION_FILL_BLOCKED",
            AuditLog.object_id == replay.id,
        )
    )
    assert replay_audit is not None
    assert replay_audit.details["rule"] == "CROSS_VERSION_DECISION_REPLAY_BLOCKED"


@pytest.mark.parametrize("mutation", ["modify", "delete"])
def test_opening_cash_mutation_or_deletion_is_detected_from_seal_and_audit(
    db_session, mutation: str
) -> None:
    opening = ensure_opening_cash(db_session)
    db_session.commit()
    assert portfolio_state(db_session).cash == pytest.approx(1_000_000.0)

    if mutation == "modify":
        opening.cash_delta += 1.0
    else:
        db_session.delete(opening)
    db_session.commit()

    with pytest.raises(SimulationLedgerIntegrityError, match="模拟"):
        portfolio_state(db_session)


@pytest.mark.parametrize("entry_type", ["DIVIDEND", "SPLIT"])
@pytest.mark.parametrize("mutation", ["modify", "delete"])
def test_corporate_action_ledger_mutation_or_deletion_is_detected(
    db_session, test_settings, entry_type: str, mutation: str
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)
    assert simulate_candidate_fills(
        db_session,
        signals=[signal],
        full_frame=frame,
        settings=test_settings,
    )
    event_time = datetime(2025, 1, 6, 7, 0, tzinfo=UTC)
    learned_at = event_time + timedelta(minutes=10)
    action = pd.DataFrame(
        [
            {
                "instrument_id": signal.instrument_id,
                "event_time": event_time,
                "published_at": learned_at,
                "first_seen_at": learned_at,
                "available_at": learned_at,
                "as_of": event_time,
                "revision": "integrity-action-v1",
                "split_factor": 2.0,
                "dividend": 1.0,
                "fx_rate": 1.0,
            }
        ]
    )
    created = apply_corporate_actions(db_session, action, through=learned_at)
    db_session.commit()
    row = next(item for item in created if item.entry_type == entry_type)

    if mutation == "delete":
        db_session.delete(row)
    elif entry_type == "DIVIDEND":
        row.cash_delta += 1.0
    else:
        row.quantity_delta += 1.0
    db_session.commit()

    with pytest.raises(SimulationLedgerIntegrityError, match="模拟"):
        portfolio_state(db_session)


@pytest.mark.parametrize("deletion_scope", ["buy_ledger", "entire_fill_chain"])
def test_buy_chain_deletion_is_detected_even_when_its_audit_remains(
    db_session, test_settings, deletion_scope: str
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)
    [fill] = simulate_candidate_fills(
        db_session,
        signals=[signal],
        full_frame=frame,
        settings=test_settings,
    )
    decision = db_session.get(SimulationDecision, fill.decision_id)
    buy_ledger = db_session.scalar(
        select(SimulationLedger).where(SimulationLedger.fill_id == fill.id)
    )
    assert decision is not None and buy_ledger is not None
    db_session.delete(buy_ledger)
    db_session.flush()
    if deletion_scope == "entire_fill_chain":
        db_session.delete(fill)
        db_session.flush()
        db_session.delete(decision)
    db_session.commit()

    with pytest.raises(SimulationLedgerIntegrityError, match="模拟"):
        portfolio_state(db_session)


@pytest.mark.parametrize("tampered_object", ["ledger", "fill", "decision"])
def test_persisted_paper_ledger_chain_tampering_blocks_all_portfolio_reads(
    db_session, test_settings, tampered_object: str
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)
    [fill] = simulate_candidate_fills(
        db_session,
        signals=[signal],
        full_frame=frame,
        settings=test_settings,
    )
    decision = db_session.get(SimulationDecision, fill.decision_id)
    ledger_row = db_session.scalar(
        select(SimulationLedger).where(SimulationLedger.fill_id == fill.id)
    )
    assert decision is not None and ledger_row is not None
    if tampered_object == "ledger":
        ledger_row.cash_delta += 1.0
    elif tampered_object == "fill":
        fill.gross_amount += 1.0
    else:
        decision.target_weight += 0.01
    db_session.commit()

    with pytest.raises(SimulationLedgerIntegrityError, match="模拟"):
        portfolio_state(db_session)
    with pytest.raises(SimulationLedgerIntegrityError, match="模拟"):
        portfolio_analytics(db_session, frame)


def test_missing_next_bar_records_evaluated_decision_but_never_fabricates_fill(
    db_session, test_settings
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)
    no_next_bar = frame.iloc[[0]].copy()
    snapshot = db_session.get(DataSnapshot, signal.data_snapshot_id)
    assert snapshot is not None
    snapshot.snapshot_hash = canonical_frame_hash(no_next_bar)
    snapshot.row_count = len(no_next_bar)
    signal.feature_values = {
        **signal.feature_values,
        "decision_policy_hash": _policy_hash(db_session, snapshot, test_settings),
    }
    _seal_signal(signal)
    db_session.commit()

    fills = simulate_candidate_fills(
        db_session,
        signals=[signal],
        full_frame=no_next_bar,
        settings=test_settings,
    )

    assert fills == []
    decision = db_session.scalar(select(SimulationDecision))
    assert decision is not None
    assert decision.signal_id == signal.id
    assert decision.status == "SKIPPED_NO_NEXT_BAR"
    assert db_session.scalar(select(func.count(SimulationFill.id))) == 0


def test_changed_risk_policy_invalidates_previously_generated_candidate(
    db_session, test_settings
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)
    changed_settings = test_settings.model_copy(update={"single_etf_cap": 0.09})

    assert (
        simulate_candidate_fills(
            db_session,
            signals=[signal],
            full_frame=frame,
            settings=changed_settings,
        )
        == []
    )
    assert db_session.scalar(select(func.count(SimulationDecision.id))) == 0
    assert db_session.scalar(select(func.count(SimulationFill.id))) == 0
    ledger_rows = list(db_session.scalars(select(SimulationLedger)))
    assert [row.entry_type for row in ledger_rows] == ["OPENING_CASH"]


@pytest.mark.parametrize(
    ("attribute", "blocked_value"),
    [("leveraged", True), ("active", False)],
)
def test_instrument_master_risk_change_after_signal_blocks_execution(
    db_session,
    test_settings,
    attribute: str,
    blocked_value: bool,
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)
    instrument = db_session.get(EtfInstrument, signal.instrument_id)
    assert instrument is not None
    setattr(instrument, attribute, blocked_value)
    db_session.commit()

    assert (
        simulate_candidate_fills(
            db_session,
            signals=[signal],
            full_frame=frame,
            settings=test_settings,
        )
        == []
    )
    assert db_session.scalar(select(func.count(SimulationDecision.id))) == 0
    assert db_session.scalar(select(func.count(SimulationFill.id))) == 0
    blocked = list(
        db_session.scalars(
            select(AuditLog.details).where(AuditLog.event_type == "SIMULATION_FILL_BLOCKED")
        )
    )
    assert any(
        details.get("rule") == "INSTRUMENT_MASTER_RISK_BLOCKED_AT_EXECUTION" for details in blocked
    )


def test_final_execution_governance_rolls_back_fill_if_master_changes_mid_task(
    db_session, test_settings, monkeypatch
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)
    instrument = db_session.get(EtfInstrument, signal.instrument_id)
    assert instrument is not None
    original_append_audit = ledger_service.append_audit

    def mutate_master_before_final_gate(session, *, event_type: str, **kwargs):
        if event_type == "SIMULATION_FILL_RECORDED":
            instrument.leveraged = True
        return original_append_audit(session, event_type=event_type, **kwargs)

    monkeypatch.setattr(ledger_service, "append_audit", mutate_master_before_final_gate)

    with pytest.raises(SnapshotIntegrityError, match="提交前数据、模型或标的风险门禁"):
        simulate_candidate_fills(
            db_session,
            signals=[signal],
            full_frame=frame,
            settings=test_settings,
        )

    db_session.rollback()
    assert db_session.scalar(select(func.count(SimulationDecision.id))) == 0
    assert db_session.scalar(select(func.count(SimulationFill.id))) == 0
    assert db_session.scalar(select(func.count(SimulationLedger.id))) == 0


@pytest.mark.parametrize(
    ("attribute", "tampered_value"),
    [
        ("state", SignalState.WATCH.value),
        ("risk_rules_hit", ["TAMPERED_AFTER_GENERATION"]),
        ("suggested_risk_budget_max", 0.01),
    ],
)
def test_tampered_signal_record_blocks_execution_before_any_decision(
    db_session,
    test_settings,
    attribute: str,
    tampered_value,
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)
    setattr(signal, attribute, tampered_value)
    db_session.commit()

    assert (
        simulate_candidate_fills(
            db_session,
            signals=[signal],
            full_frame=frame,
            settings=test_settings,
        )
        == []
    )
    assert db_session.scalar(select(func.count(SimulationDecision.id))) == 0
    assert db_session.scalar(select(func.count(SimulationFill.id))) == 0
    assert db_session.scalar(select(func.count(SimulationLedger.id))) == 0
    audit = db_session.scalar(
        select(AuditLog)
        .where(
            AuditLog.event_type == "SIMULATION_FILL_BLOCKED",
            AuditLog.object_id == signal.id,
        )
        .order_by(AuditLog.occurred_at.desc())
    )
    assert audit is not None
    assert audit.details["rule"] == "SIGNAL_RECORD_INTEGRITY_FAILED"


def test_provider_expiring_after_signal_generation_blocks_execution_with_audit(
    db_session, test_settings
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)
    registry = db_session.scalar(
        select(ProviderRegistry).where(ProviderRegistry.provider_code == "demo_fixture")
    )
    snapshot = db_session.get(DataSnapshot, signal.data_snapshot_id)
    assert registry is not None and snapshot is not None
    registry.expires_at = datetime(2025, 1, 3, tzinfo=UTC)
    signal.feature_values = {
        **signal.feature_values,
        "decision_policy_hash": _policy_hash(db_session, snapshot, test_settings),
    }
    _seal_signal(signal)
    db_session.commit()

    assert (
        simulate_candidate_fills(
            db_session,
            signals=[signal],
            full_frame=frame,
            settings=test_settings,
        )
        == []
    )
    assert db_session.scalar(select(func.count(SimulationFill.id))) == 0
    blocked_rules = list(
        db_session.scalars(
            select(AuditLog.details).where(AuditLog.event_type == "SIMULATION_FILL_BLOCKED")
        )
    )
    assert any(
        details.get("rule") == "PROVIDER_LICENSE_BLOCKED_AT_EXECUTION" for details in blocked_rules
    )


def test_shanghai_duckdb_bar_and_sqlite_roundtrip_do_not_grant_pre_open_corporate_action(
    db_session, test_settings
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)
    time_columns = ["event_time", "published_at", "first_seen_at", "available_at", "as_of"]
    for column in time_columns:
        frame[column] = pd.to_datetime(frame[column], utc=True).dt.tz_convert("Asia/Shanghai")
    frame.loc[frame.index[-1], "split_factor"] = 2.0
    frame.loc[frame.index[-1], "dividend"] = 1.0
    snapshot = db_session.get(DataSnapshot, signal.data_snapshot_id)
    assert snapshot is not None
    snapshot.snapshot_hash = canonical_frame_hash(frame)
    signal.feature_values = {
        **signal.feature_values,
        "decision_policy_hash": _policy_hash(db_session, snapshot, test_settings),
    }
    _seal_signal(signal)
    db_session.commit()

    [fill] = simulate_candidate_fills(
        db_session,
        signals=[signal],
        full_frame=frame,
        settings=test_settings,
    )
    fill_id = fill.id
    db_session.expire_all()
    stored_fill = db_session.get(SimulationFill, fill_id)
    assert stored_fill is not None
    stored_time = pd.Timestamp(stored_fill.filled_at)
    stored_time = (
        stored_time.tz_localize("UTC")
        if stored_time.tzinfo is None
        else stored_time.tz_convert("UTC")
    )
    assert stored_time == pd.Timestamp("2025-01-03T01:30:00Z")
    before = portfolio_state(db_session)

    created = apply_corporate_actions(
        db_session,
        frame,
        through=datetime(2025, 1, 3, 7, 10, tzinfo=UTC),
    )
    after = portfolio_state(db_session)

    assert created == []
    assert after == before


def test_same_batch_candidates_cannot_reuse_unflushed_cash_or_breach_cash_floor(
    db_session, test_settings, monkeypatch
) -> None:
    constrained = test_settings.model_copy(
        update={
            "cash_floor": 0.80,
            "max_turnover": 0.50,
            "asset_class_cap": 0.50,
            "industry_cap": 0.50,
            "region_cap": 0.50,
        }
    )
    first_signal, first_frame = _seed_candidate(db_session, constrained)
    # Isolate the execution-level accounting gate: even if an upstream
    # concentration pre-check is unavailable, same-batch rows must not reuse cash.
    monkeypatch.setattr(ledger_service, "concentration_rules", lambda *_args, **_kwargs: [])
    base_instrument = db_session.get(EtfInstrument, first_signal.instrument_id)
    snapshot = db_session.get(DataSnapshot, first_signal.data_snapshot_id)
    assert base_instrument is not None and snapshot is not None
    signals = [first_signal]
    frames = [first_frame]
    for ordinal in (2, 3):
        instrument_values = {
            column.name: getattr(base_instrument, column.name)
            for column in EtfInstrument.__table__.columns
            if column.name != "created_at"
        }
        instrument_values.update(
            {
                "id": f"instrument-{ordinal}",
                "name_zh": f"测试ETF-{ordinal}",
                "figi": f"TESTFIGI000{ordinal}",
                "isin": f"DM000000000{ordinal}",
                "provider_symbol": f"DEMO-00{ordinal}",
            }
        )
        instrument = EtfInstrument(**instrument_values)
        db_session.add(instrument)
        signal_values = {
            column.name: getattr(first_signal, column.name)
            for column in Signal.__table__.columns
            if column.name not in {"id", "recorded_at"}
        }
        signal_values.update(
            {
                "idempotency_key": f"candidate-signal-key-{ordinal}",
                "instrument_id": instrument.id,
                "composite_score": first_signal.composite_score - ordinal / 100,
            }
        )
        signal = Signal(**signal_values)
        db_session.add(signal)
        signals.append(signal)
        instrument_frame = first_frame.copy()
        instrument_frame["instrument_id"] = instrument.id
        frames.append(instrument_frame)
    full_frame = pd.concat(frames, ignore_index=True)
    snapshot.snapshot_hash = canonical_frame_hash(full_frame)
    snapshot.row_count = len(full_frame)
    policy_hash = _policy_hash(db_session, snapshot, constrained)
    for signal in signals:
        signal.feature_values = {
            **signal.feature_values,
            "decision_policy_hash": policy_hash,
        }
        _seal_signal(signal)
    db_session.commit()

    fills = simulate_candidate_fills(
        db_session,
        signals=signals,
        full_frame=full_frame,
        settings=constrained,
        initial_cash=1_000_000.0,
    )
    state = portfolio_state(db_session)

    assert len(fills) == 2
    assert state.cash >= 800_000.0 - 1e-6
    assert sum(fill.gross_amount + fill.fee for fill in fills) <= 200_000.0 + 1e-6
    assert db_session.scalar(select(func.count(SimulationDecision.id))) == 3
    statuses = set(db_session.scalars(select(SimulationDecision.status)))
    assert "FILLED" in statuses
    assert "SKIPPED_CASH_FLOOR" in statuses


def test_tampered_market_frame_blocks_opening_fill_before_any_ledger_mutation(
    db_session, test_settings
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)
    tampered = frame.copy()
    tampered.loc[tampered.index[-1], "open"] = 9_999.0

    with pytest.raises(SnapshotIntegrityError, match="哈希"):
        simulate_candidate_fills(
            db_session,
            signals=[signal],
            full_frame=tampered,
            settings=test_settings,
        )

    assert db_session.scalar(select(func.count(SimulationDecision.id))) == 0
    assert db_session.scalar(select(func.count(SimulationFill.id))) == 0
    assert db_session.scalar(select(func.count(SimulationLedger.id))) == 0


def test_provider_pollution_cannot_bypass_execution_gate_by_rewriting_snapshot_hash(
    db_session, test_settings
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)
    polluted = frame.copy()
    polluted["provider"] = "unregistered-provider"
    snapshot = db_session.get(DataSnapshot, signal.data_snapshot_id)
    assert snapshot is not None
    snapshot.snapshot_hash = canonical_frame_hash(polluted)
    snapshot.row_count = len(polluted)
    signal.feature_values = {
        **signal.feature_values,
        "decision_policy_hash": _policy_hash(db_session, snapshot, test_settings),
    }
    _seal_signal(signal)
    db_session.commit()

    with pytest.raises(SnapshotIntegrityError, match="供应商"):
        simulate_candidate_fills(
            db_session,
            signals=[signal],
            full_frame=polluted,
            settings=test_settings,
        )

    assert db_session.scalar(select(func.count(SimulationDecision.id))) == 0
    assert db_session.scalar(select(func.count(SimulationFill.id))) == 0
    assert db_session.scalar(select(func.count(SimulationLedger.id))) == 0


def test_unclear_snapshot_license_blocks_execution_even_when_frame_and_hash_match(
    db_session, test_settings
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)
    unclear = frame.copy()
    unclear["license_scope"] = "UNCLEAR"
    snapshot = db_session.get(DataSnapshot, signal.data_snapshot_id)
    assert snapshot is not None
    snapshot.license_scope = "UNCLEAR"
    snapshot.snapshot_hash = canonical_frame_hash(unclear)
    snapshot.row_count = len(unclear)
    signal.feature_values = {
        **signal.feature_values,
        "decision_policy_hash": _policy_hash(db_session, snapshot, test_settings),
    }
    _seal_signal(signal)
    db_session.commit()

    with pytest.raises(SnapshotIntegrityError, match="许可范围"):
        simulate_candidate_fills(
            db_session,
            signals=[signal],
            full_frame=unclear,
            settings=test_settings,
        )

    assert db_session.scalar(select(func.count(SimulationDecision.id))) == 0
    assert db_session.scalar(select(func.count(SimulationFill.id))) == 0


def test_portfolio_analytics_reconciles_nav_and_exposes_complete_finite_holdings(
    db_session, test_settings
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings, fx_rate=7.2)
    fills = simulate_candidate_fills(
        db_session,
        signals=[signal],
        full_frame=frame,
        settings=test_settings,
    )
    assert fills

    analytics = portfolio_analytics(db_session, frame)

    assert analytics.nav is not None and analytics.nav > 0
    assert analytics.max_drawdown is not None
    assert analytics.max_drawdown == pytest.approx(float(analytics.max_drawdown))
    assert analytics.valuation_time is not None
    assert analytics.holdings
    market_value = sum(float(item["market_value"]) for item in analytics.holdings)
    assert analytics.cash + market_value == pytest.approx(analytics.nav)
    assert sum(float(item["weight"]) for item in analytics.holdings) <= 1.0 + 1e-9
    for holding in analytics.holdings:
        assert holding["instrument_id"]
        assert float(holding["quantity"]) > 0
        assert float(holding["average_cost"]) > 0
        assert float(holding["reference_price"]) > 0
        assert float(holding["market_value"]) > 0
        assert 0 <= float(holding["weight"]) <= 1
        assert holding["data_mode"] == DataMode.DEMO_FIXTURE.value


def test_portfolio_valuation_fails_closed_when_one_holding_lacks_the_common_latest_bar(
    db_session, test_settings
) -> None:
    instrument_a, instrument_b = _seed_two_held_instruments(db_session, test_settings)
    first_close = datetime(2025, 1, 2, 7, 0, tzinfo=UTC)
    latest_close = datetime(2025, 1, 3, 7, 0, tzinfo=UTC)
    frame = pd.DataFrame(
        [
            _valuation_fact(instrument_a, first_close, 100.0),
            _valuation_fact(instrument_b, first_close, 100.0),
            # A stale B price must not be forward-filled and presented as a
            # synchronized portfolio valuation at A's later close.
            _valuation_fact(instrument_a, latest_close, 101.0),
        ]
    )
    evaluation_time = latest_close + timedelta(minutes=20)

    analytics = portfolio_analytics(
        db_session,
        frame,
        evaluation_time=evaluation_time,
        stale_after_minutes=60,
    )
    gate = assess_portfolio_risk_gate(
        db_session,
        frame,
        settings=test_settings,
        evaluation_time=evaluation_time,
    )

    assert analytics.nav is None
    assert analytics.max_drawdown is None
    assert analytics.holdings == []
    assert analytics.valuation_time is None
    assert gate.rules == ["PORTFOLIO_VALUATION_UNAVAILABLE"]


def test_portfolio_valuation_fails_closed_when_every_held_source_is_stale(
    db_session, test_settings
) -> None:
    instrument_a, instrument_b = _seed_two_held_instruments(db_session, test_settings)
    latest_close = datetime(2025, 1, 3, 7, 0, tzinfo=UTC)
    frame = pd.DataFrame(
        [
            _valuation_fact(instrument_a, latest_close, 101.0),
            _valuation_fact(instrument_b, latest_close, 99.0),
        ]
    )
    strict_settings = test_settings.model_copy(update={"data_stale_after_minutes": 60})
    evaluation_time = latest_close + timedelta(minutes=61)

    analytics = portfolio_analytics(
        db_session,
        frame,
        evaluation_time=evaluation_time,
        stale_after_minutes=strict_settings.data_stale_after_minutes,
    )
    gate = assess_portfolio_risk_gate(
        db_session,
        frame,
        settings=strict_settings,
        evaluation_time=evaluation_time,
    )

    assert analytics.nav is None
    assert analytics.max_drawdown is None
    assert analytics.holdings == []
    assert analytics.valuation_time is None
    assert gate.rules == ["PORTFOLIO_VALUATION_UNAVAILABLE"]


def test_portfolio_risk_gate_blocks_a_position_above_the_current_single_etf_cap(
    db_session, test_settings
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)
    assert simulate_candidate_fills(
        db_session,
        signals=[signal],
        full_frame=frame,
        settings=test_settings,
    )
    strict_settings = test_settings.model_copy(update={"single_etf_cap": 0.000001})

    gate = assess_portfolio_risk_gate(
        db_session,
        frame,
        settings=strict_settings,
        evaluation_time=datetime(2025, 1, 3, 7, 20, tzinfo=UTC),
    )

    assert gate.nav is not None
    assert gate.max_drawdown is not None
    assert gate.rules == [f"PORTFOLIO_SINGLE_ETF_CAP:{signal.instrument_id}"]


def test_portfolio_loss_gate_still_applies_after_all_positions_are_closed(
    db_session, test_settings
) -> None:
    ensure_opening_cash(db_session)
    realized_loss = SimulationLedger(
        idempotency_key="closed-portfolio-realized-loss",
        fill_id=None,
        instrument_id=None,
        entry_type="REALIZED_LOSS",
        occurred_at=datetime(2025, 1, 3, 7, 0, tzinfo=UTC),
        cash_delta=-200_000.0,
        quantity_delta=0.0,
        fee_amount=0.0,
        memo="已清仓组合的已实现模拟亏损",
    )
    db_session.add(realized_loss)
    seal_simulation_ledger_entry(db_session, realized_loss)
    db_session.commit()

    gate = assess_portfolio_risk_gate(
        db_session,
        None,
        settings=test_settings,
        evaluation_time=datetime(2025, 1, 3, 8, 0, tzinfo=UTC),
    )

    assert gate.nav == pytest.approx(800_000.0)
    assert gate.rules == ["PORTFOLIO_MAX_SIMULATION_LOSS"]


def test_corporate_actions_adjust_quantity_and_cash_once_and_reject_silent_revision(
    db_session, test_settings
) -> None:
    signal, frame = _seed_candidate(db_session, test_settings)
    [fill] = simulate_candidate_fills(
        db_session,
        signals=[signal],
        full_frame=frame,
        settings=test_settings,
    )
    before = portfolio_state(db_session)
    event_time = datetime(2025, 1, 6, 7, 0, tzinfo=UTC)
    learned_at = datetime(2025, 1, 6, 7, 10, tzinfo=UTC)
    action = pd.DataFrame(
        [
            {
                "instrument_id": signal.instrument_id,
                "event_time": event_time,
                "published_at": learned_at,
                "first_seen_at": learned_at,
                "available_at": learned_at,
                "as_of": event_time,
                "revision": "action-v1",
                "split_factor": 2.0,
                "dividend": 1.0,
                "fx_rate": fill.fx_rate,
            }
        ]
    )

    created = apply_corporate_actions(db_session, action, through=learned_at)
    db_session.commit()
    after = portfolio_state(db_session)

    assert {row.entry_type for row in created} == {"SPLIT", "DIVIDEND"}
    assert after.positions[signal.instrument_id] == pytest.approx(
        before.positions[signal.instrument_id] * 2
    )
    assert after.cash == pytest.approx(
        before.cash + before.positions[signal.instrument_id] * fill.fx_rate
    )
    assert apply_corporate_actions(db_session, action, through=learned_at) == []
    assert db_session.scalar(select(func.count(SimulationLedger.id))) == 4

    revised = action.copy()
    revised["revision"] = "action-v2"
    with pytest.raises(ValueError, match="修订"):
        apply_corporate_actions(db_session, revised, through=learned_at)


@pytest.mark.parametrize("non_candidate_state", [SignalState.WATCH, SignalState.NO_ACTION])
def test_existing_position_receives_corporate_actions_without_entry_candidate(
    db_session,
    test_settings,
    non_candidate_state: SignalState,
) -> None:
    candidate, frame = _seed_candidate(db_session, test_settings)
    [fill] = simulate_candidate_fills(
        db_session,
        signals=[candidate],
        full_frame=frame,
        settings=test_settings,
    )
    before = portfolio_state(db_session)
    action_time = datetime(2025, 1, 6, 7, 0, tzinfo=UTC)
    learned_at = datetime(2025, 1, 6, 7, 10, tzinfo=UTC)
    action_row = frame.iloc[[-1]].copy()
    action_row["event_time"] = action_time
    action_row["published_at"] = learned_at
    action_row["first_seen_at"] = learned_at
    action_row["available_at"] = learned_at
    action_row["as_of"] = action_time
    action_row["revision"] = "action-v1"
    action_row["split_factor"] = 2.0
    action_row["dividend"] = 1.0
    action_frame = pd.concat([frame, action_row], ignore_index=True)
    snapshot = db_session.get(DataSnapshot, candidate.data_snapshot_id)
    assert snapshot is not None
    snapshot.snapshot_hash = canonical_frame_hash(action_frame)
    snapshot.row_count = len(action_frame)
    snapshot.as_of = action_time
    snapshot.available_at = learned_at
    candidate.is_current = False
    signal_values = {
        column.name: getattr(candidate, column.name)
        for column in Signal.__table__.columns
        if column.name not in {"id", "recorded_at"}
    }
    signal_values.update(
        {
            "idempotency_key": f"non-candidate-{non_candidate_state.value}",
            "state": non_candidate_state.value,
            "data_as_of": action_time,
            "available_at": learned_at,
            "generated_at": datetime(2025, 1, 6, 8, 0, tzinfo=UTC),
            "is_current": True,
            "risk_rules_hit": [],
        }
    )
    non_candidate = Signal(**signal_values)
    portfolio_gate = assess_portfolio_risk_gate(
        db_session,
        action_frame,
        settings=test_settings,
        evaluation_time=signal_values["generated_at"],
    )
    non_candidate.feature_values = {
        **non_candidate.feature_values,
        "decision_policy_hash": _policy_hash(
            db_session,
            snapshot,
            test_settings,
            portfolio_context_hash=portfolio_gate.context_hash,
        ),
        "portfolio_risk_context_hash": portfolio_gate.context_hash,
        "portfolio_risk_rules": portfolio_gate.rules,
    }
    _seal_signal(non_candidate)
    db_session.add(non_candidate)
    db_session.commit()

    assert (
        simulate_candidate_fills(
            db_session,
            signals=[non_candidate],
            full_frame=action_frame,
            settings=test_settings,
        )
        == []
    )
    after = portfolio_state(db_session)
    assert after.positions[candidate.instrument_id] == pytest.approx(
        before.positions[candidate.instrument_id] * 2
    )
    assert after.cash == pytest.approx(
        before.cash + before.positions[candidate.instrument_id] * fill.fx_rate
    )
    ledger_count = db_session.scalar(select(func.count(SimulationLedger.id)))

    assert (
        simulate_candidate_fills(
            db_session,
            signals=[non_candidate],
            full_frame=action_frame,
            settings=test_settings,
        )
        == []
    )
    assert db_session.scalar(select(func.count(SimulationLedger.id))) == ledger_count

from __future__ import annotations

import hashlib
import hmac
import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pandas as pd
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from etf_sentinel.audit import append_audit
from etf_sentinel.config import Settings
from etf_sentinel.enums import DataMode, SignalState
from etf_sentinel.models import (
    AuditLog,
    DataSnapshot,
    EtfInstrument,
    ModelVersion,
    Signal,
    SimulationDecision,
    SimulationFill,
    SimulationLedger,
)
from etf_sentinel.providers.base import (
    LicenseGateError,
    authorize_provider,
    content_hash,
    validate_fact_frame,
)
from etf_sentinel.services.ingestion import SnapshotIntegrityError, verify_snapshot_frame
from etf_sentinel.services.risk import concentration_rules
from etf_sentinel.services.signals import (
    decision_policy_hash,
    rule_model_governance_rules,
    verify_signal_news_lineage,
    verify_signal_record,
)


@dataclass(frozen=True)
class PortfolioState:
    cash: float
    positions: dict[str, float]
    total_fees: float


@dataclass(frozen=True)
class PortfolioAnalytics:
    cash: float
    nav: float | None
    max_drawdown: float | None
    total_fees: float
    positions: dict[str, float]
    holdings: list[dict[str, float | str]]
    valuation_time: datetime | None


@dataclass(frozen=True)
class PortfolioRiskGate:
    rules: list[str]
    context_hash: str
    nav: float | None
    max_drawdown: float | None


class SimulationLedgerIntegrityError(SnapshotIntegrityError):
    """Raised when the persisted paper ledger no longer reconciles."""


_LEDGER_INTEGRITY_MARKER = "\nINTEGRITY_SHA256="
_OPENING_CASH_KEY = "portfolio:demo:opening-cash:v1"
_OPENING_CASH_MEMO = "固定演示模拟组合期初现金"
_BUY_MEMO = "模拟成交；使用下一可成交 bar 的开盘价并计入滑点和费用"
_DECISION_RATIONALE = "综合评分 + 逆波动率建议区间 + 配置化集中度上限"
_FINAL_DECISION_STATUSES = {
    "EVALUATED_NO_ALLOCATION",
    "SKIPPED_NO_NEXT_BAR",
    "SKIPPED_INVALID_FX",
    "SKIPPED_BELOW_LOT",
    "SKIPPED_CASH",
    "SKIPPED_CASH_FLOOR",
    "SKIPPED_SINGLE_ETF_CAP",
    "FILLED",
}


def _utc_iso(value: datetime) -> str:
    return _as_utc_timestamp(value).isoformat()


def _split_integrity_memo(memo: str) -> tuple[str, str | None]:
    if _LEDGER_INTEGRITY_MARKER not in memo:
        return memo, None
    base, marker, digest = memo.rpartition(_LEDGER_INTEGRITY_MARKER)
    if (
        not marker
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest.lower())
    ):
        raise SimulationLedgerIntegrityError("模拟账本完整性封印格式无效。")
    if _LEDGER_INTEGRITY_MARKER in base:
        raise SimulationLedgerIntegrityError("模拟账本存在重复完整性封印。")
    return base, digest.lower()


def _finite_number(value: float, *, field: str) -> float:
    parsed = float(value)
    if not math.isfinite(parsed):
        raise SimulationLedgerIntegrityError(f"模拟账本 {field} 不是有限数值。")
    return parsed


def _ledger_integrity_payload(
    row: SimulationLedger,
    *,
    memo_base: str,
    fill: SimulationFill | None = None,
    decision: SimulationDecision | None = None,
    signal: Signal | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "ledger": {
            "id": row.id,
            "idempotency_key": row.idempotency_key,
            "fill_id": row.fill_id,
            "instrument_id": row.instrument_id,
            "entry_type": row.entry_type,
            "occurred_at": _utc_iso(row.occurred_at),
            "cash_delta": float(row.cash_delta),
            "quantity_delta": float(row.quantity_delta),
            "fee_amount": float(row.fee_amount),
            "memo": memo_base,
        }
    }
    if fill is not None:
        payload["fill"] = {
            "id": fill.id,
            "idempotency_key": fill.idempotency_key,
            "decision_id": fill.decision_id,
            "instrument_id": fill.instrument_id,
            "filled_at": _utc_iso(fill.filled_at),
            "side": fill.side,
            "quantity": float(fill.quantity),
            "executable_price": float(fill.executable_price),
            "gross_amount": float(fill.gross_amount),
            "fee": float(fill.fee),
            "slippage": float(fill.slippage),
            "fx_rate": float(fill.fx_rate),
        }
    if decision is not None:
        payload["decision"] = {
            "id": decision.id,
            "idempotency_key": decision.idempotency_key,
            "signal_id": decision.signal_id,
            "instrument_id": decision.instrument_id,
            "decision_type": decision.decision_type,
            "target_weight": float(decision.target_weight),
            "decided_at": _utc_iso(decision.decided_at),
            "earliest_fill_at": _utc_iso(decision.earliest_fill_at),
            "status": decision.status,
            "rationale": decision.rationale,
        }
    if signal is not None:
        payload["signal"] = {
            "id": signal.id,
            "idempotency_key": signal.idempotency_key,
            "record_hash": (signal.feature_values or {}).get("signal_record_hash"),
        }
    return payload


def _ledger_integrity_hash(
    row: SimulationLedger,
    *,
    memo_base: str,
    fill: SimulationFill | None = None,
    decision: SimulationDecision | None = None,
    signal: Signal | None = None,
) -> str:
    return content_hash(
        _ledger_integrity_payload(
            row,
            memo_base=memo_base,
            fill=fill,
            decision=decision,
            signal=signal,
        )
    )


def _financially_equal(left: float, right: float) -> bool:
    return math.isclose(float(left), float(right), rel_tol=1e-9, abs_tol=1e-6)


def _decision_idempotency_key(
    instrument_id: str,
    decided_at: datetime,
    decision_type: str = "PAPER_ENTRY_CANDIDATE",
) -> str:
    raw_key = f"decision:{instrument_id}:{_utc_iso(decided_at)}:{decision_type}"
    return hashlib.sha256(raw_key.encode()).hexdigest()


def _assert_buy_reconciliation(
    row: SimulationLedger,
    *,
    fill: SimulationFill,
    decision: SimulationDecision,
    signal: Signal,
) -> None:
    quantity = _finite_number(fill.quantity, field="fill.quantity")
    executable_price = _finite_number(fill.executable_price, field="fill.executable_price")
    gross = _finite_number(fill.gross_amount, field="fill.gross_amount")
    fee = _finite_number(fill.fee, field="fill.fee")
    slippage = _finite_number(fill.slippage, field="fill.slippage")
    fx_rate = _finite_number(fill.fx_rate, field="fill.fx_rate")
    cash_delta = _finite_number(row.cash_delta, field="cash_delta")
    quantity_delta = _finite_number(row.quantity_delta, field="quantity_delta")
    fee_amount = _finite_number(row.fee_amount, field="fee_amount")
    target_weight = _finite_number(decision.target_weight, field="decision.target_weight")

    if (
        fill.side != "BUY"
        or quantity <= 0
        or executable_price <= 0
        or gross <= 0
        or fee < 0
        or slippage < 0
        or fx_rate <= 0
    ):
        raise SimulationLedgerIntegrityError("模拟 BUY 成交的数值或方向约束无效。")
    if (
        decision.status != "FILLED"
        or decision.decision_type != "PAPER_ENTRY_CANDIDATE"
        or not 0 < target_weight <= 1
    ):
        raise SimulationLedgerIntegrityError("模拟成交关联决策的状态或风险权重无效。")
    if not verify_signal_record(signal):
        raise SimulationLedgerIntegrityError("模拟成交关联信号完整性校验失败。")
    if (
        row.fill_id != fill.id
        or fill.decision_id != decision.id
        or decision.signal_id != signal.id
        or row.instrument_id != fill.instrument_id
        or fill.instrument_id != decision.instrument_id
        or decision.instrument_id != signal.instrument_id
    ):
        raise SimulationLedgerIntegrityError("模拟账本、成交、决策与信号的关联不一致。")

    filled_at = _utc_iso(fill.filled_at)
    expected_decision_key = _decision_idempotency_key(
        signal.instrument_id,
        signal.generated_at,
    )
    expected_fill_key = hashlib.sha256(
        f"fill:{expected_decision_key}:{filled_at}".encode()
    ).hexdigest()
    if (
        decision.idempotency_key != expected_decision_key
        or fill.idempotency_key != expected_fill_key
        or row.idempotency_key != f"ledger:{expected_fill_key}"
    ):
        raise SimulationLedgerIntegrityError("模拟成交幂等键链路不一致。")
    if (
        _utc_iso(row.occurred_at) != filled_at
        or _utc_iso(decision.earliest_fill_at) != filled_at
        or _utc_iso(decision.decided_at) != _utc_iso(signal.generated_at)
        or _as_utc_timestamp(fill.filled_at)
        <= max(
            _as_utc_timestamp(signal.available_at),
            _as_utc_timestamp(signal.generated_at),
        )
    ):
        raise SimulationLedgerIntegrityError("模拟成交时间链路不满足下一可成交时钟约束。")
    if not _financially_equal(gross, quantity * executable_price * fx_rate):
        raise SimulationLedgerIntegrityError("模拟成交 gross 与数量、价格及汇率不守恒。")
    if (
        not _financially_equal(quantity_delta, quantity)
        or not _financially_equal(fee_amount, fee)
        or not _financially_equal(cash_delta, -(gross + fee))
    ):
        raise SimulationLedgerIntegrityError("模拟 BUY 账本与成交现金、数量或费用不守恒。")


def _assert_non_buy_shape(row: SimulationLedger, *, memo_base: str) -> None:
    cash_delta = _finite_number(row.cash_delta, field="cash_delta")
    quantity_delta = _finite_number(row.quantity_delta, field="quantity_delta")
    fee_amount = _finite_number(row.fee_amount, field="fee_amount")
    if row.entry_type == "OPENING_CASH":
        if (
            row.idempotency_key != _OPENING_CASH_KEY
            or row.fill_id is not None
            or row.instrument_id is not None
            or memo_base != _OPENING_CASH_MEMO
            or cash_delta <= 0
            or _utc_iso(row.occurred_at) != datetime(2025, 1, 1, tzinfo=UTC).isoformat()
            or not _financially_equal(quantity_delta, 0)
            or not _financially_equal(fee_amount, 0)
        ):
            raise SimulationLedgerIntegrityError("模拟组合期初现金约束无效。")
        return
    if row.entry_type in {"DIVIDEND", "SPLIT"}:
        if (
            row.fill_id is not None
            or row.instrument_id is None
            or not memo_base.startswith("模拟组合公司行动；来自不可变数据快照；")
            or "revision=" not in memo_base
            or "recognized_at=" not in memo_base
            or not _financially_equal(fee_amount, 0)
        ):
            raise SimulationLedgerIntegrityError("模拟组合公司行动账本形态无效。")
        event_time = _as_utc_timestamp(row.occurred_at) + pd.Timedelta(hours=5, minutes=31)
        raw_key = f"corporate-action:{row.instrument_id}:{event_time.isoformat()}:{row.entry_type}"
        if row.idempotency_key != hashlib.sha256(raw_key.encode()).hexdigest():
            raise SimulationLedgerIntegrityError("模拟组合公司行动幂等键与经济时钟不一致。")
        if row.entry_type == "DIVIDEND" and (
            cash_delta <= 0 or not _financially_equal(quantity_delta, 0)
        ):
            raise SimulationLedgerIntegrityError("模拟分红账本现金或数量约束无效。")
        if row.entry_type == "SPLIT" and (
            not _financially_equal(cash_delta, 0) or _financially_equal(quantity_delta, 0)
        ):
            raise SimulationLedgerIntegrityError("模拟拆并份额账本现金或数量约束无效。")
        return
    if row.entry_type == "REALIZED_LOSS":
        if (
            row.fill_id is not None
            or row.instrument_id is not None
            or cash_delta >= 0
            or not _financially_equal(quantity_delta, 0)
            or not _financially_equal(fee_amount, 0)
        ):
            raise SimulationLedgerIntegrityError("模拟已实现亏损调整约束无效。")
        return
    raise SimulationLedgerIntegrityError(f"不受支持的模拟账本类型: {row.entry_type}")


def _assert_or_write_integrity_seal(
    row: SimulationLedger,
    *,
    fill: SimulationFill | None = None,
    decision: SimulationDecision | None = None,
    signal: Signal | None = None,
    allow_write: bool = False,
) -> None:
    memo_base, stored_digest = _split_integrity_memo(row.memo)
    if row.entry_type == "BUY":
        if fill is None or decision is None or signal is None:
            raise SimulationLedgerIntegrityError("模拟 BUY 账本缺少完整关联链路。")
        if memo_base != _BUY_MEMO:
            raise SimulationLedgerIntegrityError("模拟 BUY 账本用途说明异常。")
        _assert_buy_reconciliation(row, fill=fill, decision=decision, signal=signal)
    else:
        _assert_non_buy_shape(row, memo_base=memo_base)

    expected_digest = _ledger_integrity_hash(
        row,
        memo_base=memo_base,
        fill=fill,
        decision=decision,
        signal=signal,
    )
    if stored_digest is not None:
        if not hmac.compare_digest(stored_digest, expected_digest):
            raise SimulationLedgerIntegrityError("模拟账本持久化完整性封印不匹配。")
        return
    if allow_write:
        row.memo = f"{memo_base}{_LEDGER_INTEGRITY_MARKER}{expected_digest}"
        return
    # The original MVP opening row had no seal. Its complete value is fixed and
    # therefore independently recomputable; no other unsealed row is accepted.
    if row.entry_type == "OPENING_CASH" and _financially_equal(row.cash_delta, 1_000_000.0):
        return
    raise SimulationLedgerIntegrityError("模拟账本记录缺少持久化完整性封印。")


def seal_simulation_ledger_entry(session: Session, row: SimulationLedger) -> None:
    """Seal a trusted ledger mutation after all linked rows have been flushed."""
    session.flush()
    _, existing_digest = _split_integrity_memo(row.memo)
    if row.entry_type == "BUY":
        if row.fill_id is None:
            raise SimulationLedgerIntegrityError("模拟 BUY 账本必须关联成交。")
        fill = session.get(SimulationFill, row.fill_id, populate_existing=True)
        decision = (
            session.get(SimulationDecision, fill.decision_id, populate_existing=True)
            if fill is not None
            else None
        )
        signal = (
            session.get(Signal, decision.signal_id, populate_existing=True)
            if decision is not None
            else None
        )
        _assert_or_write_integrity_seal(
            row,
            fill=fill,
            decision=decision,
            signal=signal,
            allow_write=True,
        )
    else:
        _assert_or_write_integrity_seal(row, allow_write=True)
    session.flush()
    if existing_digest is None:
        _, integrity_hash = _split_integrity_memo(row.memo)
        append_audit(
            session,
            event_type="SIMULATION_LEDGER_SEALED",
            object_type="SimulationLedger",
            object_id=row.id,
            details={
                "idempotency_key": row.idempotency_key,
                "entry_type": row.entry_type,
                "ledger_integrity_hash": integrity_hash,
            },
        )


def _decision_business_key(decision: SimulationDecision) -> tuple[str, str, str]:
    return (
        decision.instrument_id,
        _utc_iso(decision.decided_at),
        decision.decision_type,
    )


def _assert_decision_reconciliation(
    decision: SimulationDecision,
    signal: Signal,
) -> None:
    target_weight = _finite_number(decision.target_weight, field="decision.target_weight")
    if (
        decision.signal_id != signal.id
        or decision.instrument_id != signal.instrument_id
        or decision.decision_type != "PAPER_ENTRY_CANDIDATE"
        or decision.rationale != _DECISION_RATIONALE
        or decision.status not in _FINAL_DECISION_STATUSES
        or not 0 <= target_weight <= 1
        or not verify_signal_record(signal)
    ):
        raise SimulationLedgerIntegrityError("模拟决策内容或信号血缘约束无效。")
    if decision.idempotency_key != _decision_idempotency_key(
        signal.instrument_id,
        signal.generated_at,
        decision.decision_type,
    ):
        raise SimulationLedgerIntegrityError("模拟决策业务幂等键不一致。")
    if _utc_iso(decision.decided_at) != _utc_iso(signal.generated_at) or _as_utc_timestamp(
        decision.earliest_fill_at
    ) < max(
        _as_utc_timestamp(signal.available_at),
        _as_utc_timestamp(signal.generated_at),
    ):
        raise SimulationLedgerIntegrityError("模拟决策时钟不满足因果约束。")


def _decision_integrity_hash(decision: SimulationDecision, signal: Signal) -> str:
    return content_hash(
        {
            "decision": {
                "id": decision.id,
                "idempotency_key": decision.idempotency_key,
                "signal_id": decision.signal_id,
                "instrument_id": decision.instrument_id,
                "decision_type": decision.decision_type,
                "target_weight": float(decision.target_weight),
                "decided_at": _utc_iso(decision.decided_at),
                "earliest_fill_at": _utc_iso(decision.earliest_fill_at),
                "status": decision.status,
                "rationale": decision.rationale,
            },
            "signal": {
                "id": signal.id,
                "idempotency_key": signal.idempotency_key,
                "record_hash": (signal.feature_values or {}).get("signal_record_hash"),
            },
        }
    )


def _record_simulation_decision_audit(
    session: Session,
    decision: SimulationDecision,
    signal: Signal,
) -> None:
    _assert_decision_reconciliation(decision, signal)
    decision_hash = _decision_integrity_hash(decision, signal)
    append_audit(
        session,
        event_type="SIMULATION_DECISION_RECORDED",
        object_type="SimulationDecision",
        object_id=decision.id,
        details={
            "idempotency_key": decision.idempotency_key,
            "signal_id": signal.id,
            "business_key": list(_decision_business_key(decision)),
            "decision_hash": decision_hash,
        },
    )


def _verify_reconciliation_audits(
    session: Session,
    *,
    ledger_rows: list[SimulationLedger],
    fills: list[SimulationFill],
    decisions: list[SimulationDecision],
    signals: list[Signal],
) -> None:
    event_types = {
        "SIMULATION_LEDGER_SEALED",
        "SIMULATION_OPENING_CASH_RECORDED",
        "SIMULATION_DECISION_RECORDED",
        "SIMULATION_FILL_RECORDED",
        "SIMULATION_CORPORATE_ACTION_RECORDED",
    }
    audit_rows = list(
        session.scalars(
            select(AuditLog)
            .where(AuditLog.event_type.in_(event_types))
            .execution_options(populate_existing=True)
        )
    )
    ledger_by_id = {row.id: row for row in ledger_rows}
    fill_by_id = {fill.id: fill for fill in fills}
    decision_by_id = {decision.id: decision for decision in decisions}
    signal_by_id = {signal.id: signal for signal in signals}
    ledger_seal_audits: dict[str, int] = {}
    decision_audits: dict[str, int] = {}
    opening_audits: dict[str, int] = {}
    fill_audits: dict[str, int] = {}
    corporate_action_audits: dict[str, int] = {}
    for audit in audit_rows:
        expected_record_hash = content_hash(
            {
                "event_type": audit.event_type,
                "object_type": audit.object_type,
                "object_id": audit.object_id,
                "actor": audit.actor,
                "trace_id": audit.trace_id,
                "details": audit.details,
                "previous_hash": audit.previous_hash,
            }
        )
        if not hmac.compare_digest(audit.record_hash, expected_record_hash):
            raise SimulationLedgerIntegrityError("模拟账本关联审计记录完整性校验失败。")
        details = audit.details if isinstance(audit.details, dict) else {}
        if audit.event_type == "SIMULATION_LEDGER_SEALED":
            audit_ledger = ledger_by_id.get(audit.object_id or "")
            if audit_ledger is None:
                raise SimulationLedgerIntegrityError("封印审计指向已缺失的模拟账本记录。")
            _, integrity_hash = _split_integrity_memo(audit_ledger.memo)
            if (
                details.get("idempotency_key") != audit_ledger.idempotency_key
                or details.get("entry_type") != audit_ledger.entry_type
                or details.get("ledger_integrity_hash") != integrity_hash
            ):
                raise SimulationLedgerIntegrityError("账本封印审计与持久化记录不一致。")
            ledger_seal_audits[audit_ledger.id] = ledger_seal_audits.get(audit_ledger.id, 0) + 1
        elif audit.event_type == "SIMULATION_DECISION_RECORDED":
            decision = decision_by_id.get(audit.object_id or "")
            signal = signal_by_id.get(decision.signal_id) if decision is not None else None
            if decision is None or signal is None:
                raise SimulationLedgerIntegrityError("决策审计指向已缺失的模拟决策或信号。")
            if (
                details.get("idempotency_key") != decision.idempotency_key
                or details.get("signal_id") != signal.id
                or details.get("business_key") != list(_decision_business_key(decision))
                or details.get("decision_hash") != _decision_integrity_hash(decision, signal)
            ):
                raise SimulationLedgerIntegrityError("决策审计与持久化决策内容不一致。")
            decision_audits[decision.id] = decision_audits.get(decision.id, 0) + 1
        elif audit.event_type == "SIMULATION_FILL_RECORDED":
            fill = fill_by_id.get(audit.object_id or "")
            if fill is None:
                raise SimulationLedgerIntegrityError("审计留痕指向已缺失的模拟成交。")
            linked_ledgers = [row for row in ledger_rows if row.fill_id == fill.id]
            if len(linked_ledgers) != 1:
                raise SimulationLedgerIntegrityError("成交审计无法解析唯一模拟账本。")
            ledger = linked_ledgers[0]
            _, integrity_hash = _split_integrity_memo(ledger.memo)
            if (
                details.get("idempotency_key") != fill.idempotency_key
                or details.get("ledger_id") != ledger.id
                or details.get("ledger_integrity_hash") != integrity_hash
            ):
                raise SimulationLedgerIntegrityError("成交审计与模拟账本血缘不一致。")
            fill_audits[fill.id] = fill_audits.get(fill.id, 0) + 1
        elif audit.event_type in {
            "SIMULATION_OPENING_CASH_RECORDED",
            "SIMULATION_CORPORATE_ACTION_RECORDED",
        }:
            audit_ledger = ledger_by_id.get(audit.object_id or "")
            if audit_ledger is None:
                raise SimulationLedgerIntegrityError("审计留痕指向已缺失的模拟账本记录。")
            _, integrity_hash = _split_integrity_memo(audit_ledger.memo)
            if (
                details.get("idempotency_key") != audit_ledger.idempotency_key
                or details.get("ledger_integrity_hash") != integrity_hash
            ):
                raise SimulationLedgerIntegrityError("审计留痕与模拟账本封印不一致。")
            audit_counts = (
                opening_audits
                if audit.event_type == "SIMULATION_OPENING_CASH_RECORDED"
                else corporate_action_audits
            )
            audit_counts[audit_ledger.id] = audit_counts.get(audit_ledger.id, 0) + 1
    for row in ledger_rows:
        _, integrity_hash = _split_integrity_memo(row.memo)
        if integrity_hash is not None and ledger_seal_audits.get(row.id) != 1:
            raise SimulationLedgerIntegrityError("模拟账本缺少唯一封印审计留痕。")
        if (
            row.entry_type == "OPENING_CASH"
            and integrity_hash is not None
            and opening_audits.get(row.id) != 1
        ):
            raise SimulationLedgerIntegrityError("期初现金缺少唯一业务审计留痕。")
        if row.entry_type in {"DIVIDEND", "SPLIT"} and corporate_action_audits.get(row.id) != 1:
            raise SimulationLedgerIntegrityError("公司行动缺少唯一业务审计留痕。")
        if row.entry_type == "BUY" and (row.fill_id is None or fill_audits.get(row.fill_id) != 1):
            raise SimulationLedgerIntegrityError("BUY 账本缺少唯一成交审计留痕。")
    if any(decision_audits.get(decision.id) != 1 for decision in decisions):
        raise SimulationLedgerIntegrityError("模拟决策缺少唯一内容封印审计留痕。")


def assert_simulation_ledger_integrity(session: Session) -> None:
    """Reconcile every persisted accounting row before valuation or mutation."""
    session.flush()
    ledger_rows = list(
        session.scalars(
            select(SimulationLedger)
            .order_by(SimulationLedger.occurred_at, SimulationLedger.id)
            .execution_options(populate_existing=True)
        )
    )
    fills = list(
        session.scalars(
            select(SimulationFill)
            .order_by(SimulationFill.filled_at, SimulationFill.id)
            .execution_options(populate_existing=True)
        )
    )
    decisions = list(
        session.scalars(
            select(SimulationDecision)
            .order_by(SimulationDecision.decided_at, SimulationDecision.id)
            .execution_options(populate_existing=True)
        )
    )
    signals = list(session.scalars(select(Signal).execution_options(populate_existing=True)))
    fill_by_id = {fill.id: fill for fill in fills}
    decision_by_id = {decision.id: decision for decision in decisions}
    signal_by_id = {signal.id: signal for signal in signals}

    opening_rows = [row for row in ledger_rows if row.entry_type == "OPENING_CASH"]
    if len(opening_rows) > 1 or (ledger_rows and len(opening_rows) != 1):
        raise SimulationLedgerIntegrityError("模拟账本必须且只能包含一条期初现金记录。")

    buy_ledger_by_fill: dict[str, SimulationLedger] = {}
    for row in ledger_rows:
        if row.entry_type != "BUY":
            _assert_or_write_integrity_seal(row)
            continue
        if row.fill_id is None or row.fill_id in buy_ledger_by_fill:
            raise SimulationLedgerIntegrityError("模拟成交与 BUY 账本不是一一对应。")
        fill = fill_by_id.get(row.fill_id)
        decision = decision_by_id.get(fill.decision_id) if fill is not None else None
        signal = signal_by_id.get(decision.signal_id) if decision is not None else None
        if fill is None or decision is None or signal is None:
            raise SimulationLedgerIntegrityError("模拟 BUY 账本存在孤立关联。")
        _assert_or_write_integrity_seal(
            row,
            fill=fill,
            decision=decision,
            signal=signal,
        )
        buy_ledger_by_fill[row.fill_id] = row

    fills_by_decision: dict[str, list[SimulationFill]] = {}
    for fill in fills:
        fills_by_decision.setdefault(fill.decision_id, []).append(fill)
        if fill.id not in buy_ledger_by_fill:
            raise SimulationLedgerIntegrityError("模拟成交缺少唯一 BUY 账本记录。")
    decisions_by_signal: dict[str, list[SimulationDecision]] = {}
    business_keys: set[tuple[str, str, str]] = set()
    for decision in decisions:
        signal = signal_by_id.get(decision.signal_id)
        if signal is None:
            raise SimulationLedgerIntegrityError("模拟决策关联信号不存在。")
        _assert_decision_reconciliation(decision, signal)
        linked_fills = fills_by_decision.get(decision.id, [])
        if len(linked_fills) > 1 or (decision.status == "FILLED") != (len(linked_fills) == 1):
            raise SimulationLedgerIntegrityError("模拟决策状态与成交数量不一致。")
        decisions_by_signal.setdefault(decision.signal_id, []).append(decision)
        business_key = _decision_business_key(decision)
        if business_key in business_keys:
            raise SimulationLedgerIntegrityError("同一标的与决策时点存在重复模拟决策。")
        business_keys.add(business_key)
    if any(len(items) > 1 for items in decisions_by_signal.values()):
        raise SimulationLedgerIntegrityError("单一信号关联了多个模拟决策。")
    _verify_reconciliation_audits(
        session,
        ledger_rows=ledger_rows,
        fills=fills,
        decisions=decisions,
        signals=signals,
    )


def ensure_opening_cash(session: Session, amount: float = 1_000_000.0) -> SimulationLedger:
    assert_simulation_ledger_integrity(session)
    key = _OPENING_CASH_KEY
    existing = session.scalar(
        select(SimulationLedger)
        .where(SimulationLedger.idempotency_key == key)
        .execution_options(populate_existing=True)
    )
    if existing is not None:
        if not _financially_equal(existing.cash_delta, amount):
            raise SimulationLedgerIntegrityError("期初现金与本次模拟配置不一致。")
        _, existing_integrity_hash = _split_integrity_memo(existing.memo)
        if existing_integrity_hash is None:
            seal_simulation_ledger_entry(session, existing)
            _, existing_integrity_hash = _split_integrity_memo(existing.memo)
            append_audit(
                session,
                event_type="SIMULATION_OPENING_CASH_RECORDED",
                object_type="SimulationLedger",
                object_id=existing.id,
                details={
                    "idempotency_key": existing.idempotency_key,
                    "cash_delta": float(existing.cash_delta),
                    "ledger_integrity_hash": existing_integrity_hash,
                },
            )
        return existing
    row = SimulationLedger(
        idempotency_key=key,
        fill_id=None,
        instrument_id=None,
        entry_type="OPENING_CASH",
        occurred_at=datetime(2025, 1, 1, tzinfo=UTC),
        cash_delta=amount,
        quantity_delta=0,
        fee_amount=0,
        memo=_OPENING_CASH_MEMO,
    )
    session.add(row)
    session.flush()
    seal_simulation_ledger_entry(session, row)
    _, integrity_hash = _split_integrity_memo(row.memo)
    append_audit(
        session,
        event_type="SIMULATION_OPENING_CASH_RECORDED",
        object_type="SimulationLedger",
        object_id=row.id,
        details={
            "idempotency_key": row.idempotency_key,
            "cash_delta": float(row.cash_delta),
            "ledger_integrity_hash": integrity_hash,
        },
    )
    return row


def portfolio_state(session: Session, *, through: datetime | None = None) -> PortfolioState:
    assert_simulation_ledger_integrity(session)
    cutoff = _as_aware(through) if through is not None else None
    cash_query = select(func.coalesce(func.sum(SimulationLedger.cash_delta), 0))
    fee_query = select(func.coalesce(func.sum(SimulationLedger.fee_amount), 0))
    position_query = (
        select(SimulationLedger.instrument_id, func.sum(SimulationLedger.quantity_delta))
        .where(SimulationLedger.instrument_id.is_not(None))
        .group_by(SimulationLedger.instrument_id)
    )
    if cutoff is not None:
        cash_query = cash_query.where(SimulationLedger.occurred_at <= cutoff)
        fee_query = fee_query.where(SimulationLedger.occurred_at <= cutoff)
        position_query = position_query.where(SimulationLedger.occurred_at <= cutoff)
    cash = float(session.scalar(cash_query) or 0)
    fees = float(session.scalar(fee_query) or 0)
    rows = session.execute(position_query).all()
    positions = {
        instrument_id: float(quantity)
        for instrument_id, quantity in rows
        if instrument_id and abs(float(quantity)) > 1e-9
    }
    return PortfolioState(cash=cash, positions=positions, total_fees=fees)


def portfolio_analytics(
    session: Session,
    market_frame: pd.DataFrame | None,
    *,
    evaluation_time: datetime | None = None,
    stale_after_minutes: int | None = None,
) -> PortfolioAnalytics:
    """Value the paper ledger from immutable bars; never guesses prices when data is absent."""
    state = portfolio_state(session, through=evaluation_time)
    if not state.positions:
        return PortfolioAnalytics(
            cash=state.cash,
            nav=state.cash,
            max_drawdown=0.0,
            total_fees=state.total_fees,
            positions=state.positions,
            holdings=[],
            valuation_time=None,
        )
    if market_frame is None or market_frame.empty:
        return PortfolioAnalytics(
            cash=state.cash,
            nav=None,
            max_drawdown=None,
            total_fees=state.total_fees,
            positions=state.positions,
            holdings=[],
            valuation_time=None,
        )
    required = {
        "instrument_id",
        "event_time",
        "published_at",
        "first_seen_at",
        "available_at",
        "as_of",
        "close",
        "fx_rate",
    }
    if not required.issubset(market_frame.columns):
        raise ValueError("组合估值快照缺少必要字段。")
    bars = market_frame.loc[market_frame["instrument_id"].isin(state.positions)].copy()
    for column in ("event_time", "published_at", "first_seen_at", "available_at", "as_of"):
        bars[column] = pd.to_datetime(bars[column], utc=True, errors="raise")
    bars = bars.sort_values(["event_time", "instrument_id"])
    latest_event_time = bars["event_time"].max()
    latest_rows = bars.loc[bars["event_time"] == latest_event_time].copy()
    if "revision" in latest_rows.columns:
        latest_rows = latest_rows.sort_values(
            ["instrument_id", "available_at", "first_seen_at", "revision"]
        ).drop_duplicates("instrument_id", keep="last")
    if set(latest_rows["instrument_id"].astype(str)) != set(state.positions):
        return PortfolioAnalytics(
            cash=state.cash,
            nav=None,
            max_drawdown=None,
            total_fees=state.total_fees,
            positions=state.positions,
            holdings=[],
            valuation_time=None,
        )
    if evaluation_time is not None and stale_after_minutes is not None:
        current = pd.Timestamp(_as_aware(evaluation_time)).tz_convert("UTC")
        held_freshness_clock = latest_rows[["event_time", "available_at", "as_of"]].min(axis=1)
        if current - held_freshness_clock.min() > pd.Timedelta(minutes=stale_after_minutes):
            return PortfolioAnalytics(
                cash=state.cash,
                nav=None,
                max_drawdown=None,
                total_fees=state.total_fees,
                positions=state.positions,
                holdings=[],
                valuation_time=None,
            )
    numeric_latest = latest_rows[["close", "fx_rate"]].apply(pd.to_numeric, errors="coerce")
    if (
        numeric_latest.isna().any().any()
        or not all(math.isfinite(float(value)) for value in numeric_latest.to_numpy().ravel())
        or (numeric_latest <= 0).any().any()
    ):
        return PortfolioAnalytics(
            cash=state.cash,
            nav=None,
            max_drawdown=None,
            total_fees=state.total_fees,
            positions=state.positions,
            holdings=[],
            valuation_time=None,
        )
    latest_knowledge_time = latest_rows[
        ["published_at", "first_seen_at", "available_at", "as_of"]
    ].max(axis=1)
    if (latest_knowledge_time < latest_event_time).any():
        # This should already be rejected by fact-schema validation.  Keep the
        # valuation path independently fail-closed for direct callers.
        return PortfolioAnalytics(
            cash=state.cash,
            nav=None,
            max_drawdown=None,
            total_fees=state.total_fees,
            positions=state.positions,
            holdings=[],
            valuation_time=None,
        )
    if "revision" in bars.columns:
        bars = bars.sort_values(
            ["instrument_id", "event_time", "available_at", "first_seen_at", "revision"]
        ).drop_duplicates(["instrument_id", "event_time"], keep="last")
    close = bars.pivot(index="event_time", columns="instrument_id", values="close").ffill()
    fx = bars.pivot(index="event_time", columns="instrument_id", values="fx_rate").ffill()
    if close.empty:
        return PortfolioAnalytics(
            cash=state.cash,
            nav=None,
            max_drawdown=None,
            total_fees=state.total_fees,
            positions=state.positions,
            holdings=[],
            valuation_time=None,
        )
    fills = list(session.scalars(select(SimulationFill).order_by(SimulationFill.filled_at)))
    ledger_rows = list(
        session.scalars(select(SimulationLedger).order_by(SimulationLedger.occurred_at))
    )
    nav_values: list[float] = []
    for valuation_time in close.index:
        current_cash = sum(
            float(row.cash_delta)
            for row in ledger_rows
            if _as_utc_timestamp(row.occurred_at) <= valuation_time
        )
        current_positions: dict[str, float] = {}
        for row in ledger_rows:
            if row.instrument_id and _as_utc_timestamp(row.occurred_at) <= valuation_time:
                current_positions[row.instrument_id] = current_positions.get(
                    row.instrument_id, 0.0
                ) + float(row.quantity_delta)
        marked = sum(
            quantity
            * float(close.at[valuation_time, instrument_id])
            * float(fx.at[valuation_time, instrument_id])
            for instrument_id, quantity in current_positions.items()
            if instrument_id in close.columns
            and pd.notna(close.at[valuation_time, instrument_id])
            and pd.notna(fx.at[valuation_time, instrument_id])
        )
        nav_values.append(current_cash + marked)
    nav_series = pd.Series(nav_values, index=close.index, dtype=float)
    drawdown = nav_series.div(nav_series.cummax()).sub(1)
    nav = float(nav_series.iloc[-1])
    valuation_time = close.index[-1].to_pydatetime()
    holdings: list[dict[str, float | str]] = []
    for instrument_id, quantity in sorted(state.positions.items()):
        instrument_fills = [fill for fill in fills if fill.instrument_id == instrument_id]
        total_cost = sum(float(fill.gross_amount) + float(fill.fee) for fill in instrument_fills)
        latest_row = latest_rows.loc[latest_rows["instrument_id"] == instrument_id].iloc[-1]
        reference_price = float(latest_row["close"])
        reference_fx = float(latest_row["fx_rate"])
        market_value = quantity * reference_price * reference_fx
        holdings.append(
            {
                "instrument_id": instrument_id,
                "quantity": quantity,
                "average_cost": total_cost / quantity if quantity else 0.0,
                "reference_price": reference_price,
                "market_value": market_value,
                "weight": market_value / nav if nav > 0 else 0.0,
                "data_mode": str(
                    bars.loc[bars["instrument_id"] == instrument_id, "data_mode"].iloc[-1]
                )
                if "data_mode" in bars.columns
                else "UNKNOWN",
            }
        )
    return PortfolioAnalytics(
        cash=state.cash,
        nav=nav,
        max_drawdown=float(drawdown.min()),
        total_fees=state.total_fees,
        positions=state.positions,
        holdings=holdings,
        valuation_time=valuation_time,
    )


def assess_portfolio_risk_gate(
    session: Session,
    market_frame: pd.DataFrame | None,
    *,
    settings: Settings,
    evaluation_time: datetime | None = None,
    initial_cash: float = 1_000_000.0,
) -> PortfolioRiskGate:
    state = portfolio_state(session, through=evaluation_time)
    if not state.positions:
        rules: list[str] = []
        opening_cash_value = session.scalar(
            select(func.sum(SimulationLedger.cash_delta)).where(
                SimulationLedger.entry_type == "OPENING_CASH",
                *(
                    [SimulationLedger.occurred_at <= _as_aware(evaluation_time)]
                    if evaluation_time is not None
                    else []
                ),
            )
        )
        if opening_cash_value is not None:
            opening_cash = float(opening_cash_value)
            if opening_cash <= 0:
                rules.append("PORTFOLIO_OPENING_CAPITAL_INVALID")
            elif state.cash / opening_cash - 1 <= -settings.max_simulation_loss:
                rules.append("PORTFOLIO_MAX_SIMULATION_LOSS")
        return PortfolioRiskGate(
            rules=rules,
            context_hash=content_hash(
                {
                    "rules": rules,
                    "max_simulation_loss": settings.max_simulation_loss,
                    "max_drawdown": settings.max_drawdown,
                }
            ),
            nav=state.cash,
            max_drawdown=0.0,
        )
    analytics = portfolio_analytics(
        session,
        market_frame,
        evaluation_time=evaluation_time,
        stale_after_minutes=(
            settings.data_stale_after_minutes if evaluation_time is not None else None
        ),
    )
    rules = []
    if analytics.nav is None or analytics.max_drawdown is None:
        rules.append("PORTFOLIO_VALUATION_UNAVAILABLE")
    else:
        opening_cash = float(
            session.scalar(
                select(func.coalesce(func.sum(SimulationLedger.cash_delta), 0)).where(
                    SimulationLedger.entry_type == "OPENING_CASH"
                )
            )
            or initial_cash
        )
        if opening_cash <= 0:
            rules.append("PORTFOLIO_OPENING_CAPITAL_INVALID")
        elif analytics.nav / opening_cash - 1 <= -settings.max_simulation_loss:
            rules.append("PORTFOLIO_MAX_SIMULATION_LOSS")
        if analytics.max_drawdown <= -settings.max_drawdown:
            rules.append("PORTFOLIO_MAX_DRAWDOWN")
        instruments = {
            instrument.id: instrument
            for instrument in session.scalars(
                select(EtfInstrument).where(EtfInstrument.id.in_(set(state.positions)))
            )
        }
        if set(instruments) != set(state.positions):
            rules.append("PORTFOLIO_INSTRUMENT_MASTER_MISSING")
        else:
            weights = {
                str(item["instrument_id"]): float(item["weight"]) for item in analytics.holdings
            }
            rules.extend(
                f"PORTFOLIO_{rule}" for rule in concentration_rules(weights, instruments, settings)
            )
    rules = list(dict.fromkeys(rules))
    return PortfolioRiskGate(
        rules=rules,
        context_hash=content_hash(
            {
                "rules": rules,
                "max_simulation_loss": settings.max_simulation_loss,
                "max_drawdown": settings.max_drawdown,
                "single_etf_cap": settings.single_etf_cap,
                "asset_class_cap": settings.asset_class_cap,
                "industry_cap": settings.industry_cap,
                "region_cap": settings.region_cap,
                "cash_floor": settings.cash_floor,
            }
        ),
        nav=analytics.nav,
        max_drawdown=analytics.max_drawdown,
    )


def apply_corporate_actions(
    session: Session, market_frame: pd.DataFrame, *, through: datetime
) -> list[SimulationLedger]:
    required = {
        "instrument_id",
        "event_time",
        "published_at",
        "first_seen_at",
        "available_at",
        "as_of",
        "revision",
        "split_factor",
        "dividend",
        "fx_rate",
    }
    if market_frame.empty:
        return []
    missing = required.difference(market_frame.columns)
    if missing:
        raise ValueError(f"公司行动缺少因果时间字段: {sorted(missing)}")
    assert_simulation_ledger_integrity(session)
    cutoff = pd.Timestamp(_as_aware(through)).tz_convert("UTC")
    actions = market_frame.copy()
    for column in ("event_time", "published_at", "first_seen_at", "available_at", "as_of"):
        actions[column] = pd.to_datetime(actions[column], utc=True, errors="raise")
    actions = actions.loc[
        (actions["event_time"] <= cutoff)
        & (actions["published_at"] <= cutoff)
        & (actions["first_seen_at"] <= cutoff)
        & (actions["available_at"] <= cutoff)
        & (actions["as_of"] <= cutoff)
        & (
            actions["split_factor"].astype(float).ne(1.0)
            | actions["dividend"].astype(float).ne(0.0)
        )
    ].sort_values(["instrument_id", "event_time", "available_at", "first_seen_at", "revision"])
    # A point-in-time run consumes exactly the latest vintage that was known by the
    # cutoff.  Older revisions must never be accumulated as additional actions.
    actions = actions.drop_duplicates(
        subset=["instrument_id", "event_time"], keep="last"
    ).sort_values(["event_time", "instrument_id"])
    created: list[SimulationLedger] = []
    for action in actions.to_dict(orient="records"):
        event_time = pd.Timestamp(action["event_time"])
        event_time = (
            event_time.tz_localize("UTC")
            if event_time.tzinfo is None
            else event_time.tz_convert("UTC")
        )
        effective_at = event_time - timedelta(hours=5, minutes=31)
        recognized_at = max(
            effective_at,
            pd.Timestamp(action["published_at"]),
            pd.Timestamp(action["first_seen_at"]),
            pd.Timestamp(action["available_at"]),
            pd.Timestamp(action["as_of"]),
        )
        instrument_id = str(action["instrument_id"])
        revision = str(action.get("revision", "unknown"))
        position = float(
            session.scalar(
                select(func.coalesce(func.sum(SimulationLedger.quantity_delta), 0)).where(
                    SimulationLedger.instrument_id == instrument_id,
                    SimulationLedger.occurred_at <= effective_at.to_pydatetime(),
                )
            )
            or 0
        )
        if position <= 0:
            continue
        split_factor = float(action["split_factor"])
        dividend = float(action["dividend"])
        fx_rate = float(action["fx_rate"])
        if (
            not all(math.isfinite(value) for value in (position, split_factor, dividend, fx_rate))
            or split_factor <= 0
            or dividend < 0
            or fx_rate <= 0
        ):
            raise SimulationLedgerIntegrityError("公司行动数量、分红、拆并因子或汇率无效。")
        action_specs = []
        if dividend != 0:
            action_specs.append(("DIVIDEND", position * dividend * fx_rate, 0.0))
        if split_factor != 1:
            action_specs.append(("SPLIT", 0.0, position * (split_factor - 1)))
        for action_type, cash_delta, quantity_delta in action_specs:
            raw_key = f"corporate-action:{instrument_id}:{event_time.isoformat()}:{action_type}"
            idempotency_key = hashlib.sha256(raw_key.encode()).hexdigest()
            existing = session.scalar(
                select(SimulationLedger).where(SimulationLedger.idempotency_key == idempotency_key)
            )
            if existing is not None:
                if f"revision={revision}" not in existing.memo:
                    raise ValueError("公司行动修订需要显式冲正，已失败关闭。")
                continue
            row = SimulationLedger(
                idempotency_key=idempotency_key,
                fill_id=None,
                instrument_id=instrument_id,
                entry_type=action_type,
                # Economic entitlement is booked at the effective market time,
                # but only after the fact has passed the `through` knowledge gate.
                # `recognized_at` remains in the audit trail to separate the two clocks.
                occurred_at=effective_at.to_pydatetime(),
                cash_delta=cash_delta,
                quantity_delta=quantity_delta,
                fee_amount=0.0,
                memo=(
                    "模拟组合公司行动；来自不可变数据快照；"
                    f"revision={revision};recognized_at={recognized_at.isoformat()}"
                ),
            )
            session.add(row)
            session.flush()
            seal_simulation_ledger_entry(session, row)
            _, integrity_hash = _split_integrity_memo(row.memo)
            append_audit(
                session,
                event_type="SIMULATION_CORPORATE_ACTION_RECORDED",
                object_type="SimulationLedger",
                object_id=row.id,
                details={
                    "instrument_id": instrument_id,
                    "action_type": action_type,
                    "event_time": event_time.isoformat(),
                    "recognized_at": recognized_at.isoformat(),
                    "revision": revision,
                    "cash_delta": cash_delta,
                    "quantity_delta": quantity_delta,
                    "idempotency_key": row.idempotency_key,
                    "ledger_integrity_hash": integrity_hash,
                },
            )
            created.append(row)
    assert_simulation_ledger_integrity(session)
    return created


def simulate_candidate_fills(
    session: Session,
    *,
    signals: list[Signal],
    full_frame: pd.DataFrame,
    settings: Settings,
    initial_cash: float = 1_000_000.0,
    fee_bps: float = 2.5,
    slippage_bps: float = 3.0,
    commit: bool = True,
) -> list[SimulationFill]:
    assert_simulation_ledger_integrity(session)
    if settings.global_kill_switch:
        append_audit(
            session,
            event_type="SIMULATION_FILL_BLOCKED",
            object_type="GlobalRiskGate",
            details={"rule": "GLOBAL_KILL_SWITCH"},
        )
        if commit:
            session.commit()
        return []
    session.flush()
    refreshed_signals: list[Signal] = []
    for signal in signals:
        current_signal = session.get(Signal, signal.id, populate_existing=True)
        if current_signal is None:
            raise SnapshotIntegrityError("模拟成交关联信号不存在。")
        refreshed_signals.append(current_signal)
    signals = refreshed_signals
    invalid_signals = [signal for signal in signals if not verify_signal_record(signal)]
    if invalid_signals:
        for signal in invalid_signals:
            append_audit(
                session,
                event_type="SIMULATION_FILL_BLOCKED",
                object_type="Signal",
                object_id=signal.id,
                details={"rule": "SIGNAL_RECORD_INTEGRITY_FAILED"},
            )
        if commit:
            session.commit()
        else:
            session.flush()
        return []
    execution_snapshot_ids = {signal.data_snapshot_id for signal in signals}
    if len(execution_snapshot_ids) > 1:
        raise SnapshotIntegrityError("单次模拟成交不得混用多个行情快照。")
    execution_snapshot: DataSnapshot | None = None
    if execution_snapshot_ids:
        execution_snapshot = session.get(
            DataSnapshot,
            next(iter(execution_snapshot_ids)),
            populate_existing=True,
        )
        if execution_snapshot is None or execution_snapshot.dataset_type != "MARKET_BARS":
            raise SnapshotIntegrityError("模拟成交的行情快照不存在或类型无效。")
        if (
            not execution_snapshot.license_scope
            or "UNCLEAR" in execution_snapshot.license_scope.upper()
        ):
            raise SnapshotIntegrityError("模拟成交的行情快照许可范围为空或不明确。")
        verify_snapshot_frame(
            full_frame,
            expected_hash=execution_snapshot.snapshot_hash,
            expected_rows=execution_snapshot.row_count,
        )
        if set(full_frame["provider"].dropna().astype(str).unique()) != {
            execution_snapshot.provider_code
        }:
            raise SnapshotIntegrityError("模拟成交事实供应商与快照登记不一致。")
        if set(full_frame["license_scope"].dropna().astype(str).unique()) != {
            execution_snapshot.license_scope
        }:
            raise SnapshotIntegrityError("模拟成交事实许可范围与快照登记不一致。")
        if set(full_frame["data_mode"].dropna().astype(str).unique()) != {
            execution_snapshot.data_mode
        }:
            raise SnapshotIntegrityError("模拟成交事实 data_mode 与快照登记不一致。")
        try:
            authorize_provider(
                session,
                execution_snapshot.provider_code,
                purposes={"display", "algorithm", "derivative", "cache"},
                at=datetime.now(UTC),
            )
            held_ids = set(portfolio_state(session).positions)
            for instrument_id in held_ids:
                held_instrument = session.get(EtfInstrument, instrument_id, populate_existing=True)
                if (
                    held_instrument is None
                    or held_instrument.provider_code != execution_snapshot.provider_code
                ):
                    raise SnapshotIntegrityError("持仓标的与公司行动快照血缘不一致。")
                authorize_provider(
                    session,
                    execution_snapshot.provider_code,
                    purposes={"display", "algorithm", "derivative", "cache"},
                    at=datetime.now(UTC),
                    market=held_instrument.mic,
                    region=held_instrument.region,
                )
        except LicenseGateError:
            append_audit(
                session,
                event_type="SIMULATION_FILL_BLOCKED",
                object_type="DataSnapshot",
                object_id=execution_snapshot.id,
                details={"rule": "PROVIDER_LICENSE_BLOCKED_AT_EXECUTION"},
            )
            if commit:
                session.commit()
            else:
                session.flush()
            return []
        evaluation_time = max(
            (_as_aware(signal.generated_at) for signal in signals),
            default=_as_aware(execution_snapshot.available_at),
        )
        ensure_opening_cash(session, initial_cash)
        apply_corporate_actions(session, full_frame, through=evaluation_time)
        portfolio_point_in_time = full_frame.loc[
            (pd.to_datetime(full_frame["event_time"], utc=True) <= evaluation_time)
            & (pd.to_datetime(full_frame["published_at"], utc=True) <= evaluation_time)
            & (pd.to_datetime(full_frame["first_seen_at"], utc=True) <= evaluation_time)
            & (pd.to_datetime(full_frame["available_at"], utc=True) <= evaluation_time)
            & (pd.to_datetime(full_frame["as_of"], utc=True) <= evaluation_time)
        ].copy()
        portfolio_gate = assess_portfolio_risk_gate(
            session,
            portfolio_point_in_time,
            settings=settings,
            evaluation_time=evaluation_time,
            initial_cash=initial_cash,
        )
        recorded_context_hashes = {
            str((signal.feature_values or {}).get("portfolio_risk_context_hash", ""))
            for signal in signals
        }
        context_changed = recorded_context_hashes != {portfolio_gate.context_hash}
        if portfolio_gate.rules or context_changed:
            append_audit(
                session,
                event_type="SIMULATION_FILL_BLOCKED",
                object_type="PortfolioRiskGate",
                details={
                    "rules": portfolio_gate.rules,
                    "context_changed": context_changed,
                },
            )
            if commit:
                session.commit()
            else:
                session.flush()
            return []
    eligible: list[Signal] = []
    for signal in signals:
        if (
            signal.state != SignalState.ENTRY_CANDIDATE.value
            or not signal.is_current
            or signal.latency_status != "ON_TIME"
            or signal.data_mode != DataMode.DEMO_FIXTURE.value
            or bool(signal.risk_rules_hit)
        ):
            continue
        snapshot = session.get(DataSnapshot, signal.data_snapshot_id, populate_existing=True)
        if snapshot is None or snapshot.data_mode != signal.data_mode:
            continue
        session.flush()
        instrument = session.get(EtfInstrument, signal.instrument_id, populate_existing=True)
        if (
            instrument is None
            or not instrument.active
            or instrument.leveraged
            or instrument.inverse
            or instrument.liquidity_tier > 2
        ):
            append_audit(
                session,
                event_type="SIMULATION_FILL_BLOCKED",
                object_type="Signal",
                object_id=signal.id,
                details={"rule": "INSTRUMENT_MASTER_RISK_BLOCKED_AT_EXECUTION"},
            )
            continue
        model = session.get(ModelVersion, signal.model_version_id, populate_existing=True)
        model_rules = rule_model_governance_rules(model, data_mode=signal.data_mode)
        if model_rules:
            append_audit(
                session,
                event_type="SIMULATION_FILL_BLOCKED",
                object_type="Signal",
                object_id=signal.id,
                details={"rule": "MODEL_GOVERNANCE_BLOCKED", "model_rules": model_rules},
            )
            continue
        news_lineage_ok, news_lineage_hash = verify_signal_news_lineage(
            session, signal, snapshot=snapshot
        )
        if not news_lineage_ok or news_lineage_hash is None:
            append_audit(
                session,
                event_type="SIMULATION_FILL_BLOCKED",
                object_type="Signal",
                object_id=signal.id,
                details={"rule": "NEWS_LINEAGE_BLOCKED_AT_EXECUTION"},
            )
            continue
        try:
            authorize_provider(
                session,
                snapshot.provider_code,
                purposes={"display", "algorithm", "derivative", "cache"},
                at=datetime.now(UTC),
                market=instrument.mic,
                region=instrument.region,
            )
        except LicenseGateError:
            append_audit(
                session,
                event_type="SIMULATION_FILL_BLOCKED",
                object_type="Signal",
                object_id=signal.id,
                details={"rule": "PROVIDER_LICENSE_BLOCKED_AT_EXECUTION"},
            )
            continue
        try:
            macro_snapshot = _macro_snapshot_from_lineage(session, signal)
        except (LicenseGateError, SnapshotIntegrityError, ValueError):
            append_audit(
                session,
                event_type="SIMULATION_FILL_BLOCKED",
                object_type="Signal",
                object_id=signal.id,
                details={"rule": "MACRO_LINEAGE_BLOCKED_AT_EXECUTION"},
            )
            continue
        expected_policy_hash = decision_policy_hash(
            session,
            snapshot=snapshot,
            settings=settings,
            macro_snapshot=macro_snapshot,
            model=model,
            news_lineage_hash=news_lineage_hash,
            portfolio_risk_context_hash=(signal.feature_values or {}).get(
                "portfolio_risk_context_hash"
            ),
        )
        if (signal.feature_values or {}).get("decision_policy_hash") != expected_policy_hash:
            continue
        eligible.append(signal)
    if not eligible:
        append_audit(
            session,
            event_type="SIMULATION_FILL_BLOCKED",
            object_type="SignalRiskGate",
            details={"rule": "NO_EXECUTION_ELIGIBLE_CURRENT_SIGNAL"},
        )
        if commit:
            session.commit()
        return []
    existing_business_decisions = {
        _decision_business_key(decision): decision
        for decision in session.scalars(
            select(SimulationDecision).execution_options(populate_existing=True)
        )
    }
    replay_blocked: list[Signal] = []
    for signal in eligible:
        business_key = (
            signal.instrument_id,
            _utc_iso(signal.generated_at),
            "PAPER_ENTRY_CANDIDATE",
        )
        existing_decision = existing_business_decisions.get(business_key)
        if existing_decision is not None and existing_decision.signal_id != signal.id:
            replay_blocked.append(signal)
            append_audit(
                session,
                event_type="SIMULATION_FILL_BLOCKED",
                object_type="Signal",
                object_id=signal.id,
                details={
                    "rule": "CROSS_VERSION_DECISION_REPLAY_BLOCKED",
                    "existing_decision_id": existing_decision.id,
                    "decided_at": business_key[1],
                },
            )
    if replay_blocked:
        replay_blocked_ids = {signal.id for signal in replay_blocked}
        eligible = [signal for signal in eligible if signal.id not in replay_blocked_ids]
    if not eligible:
        if commit:
            session.commit()
        else:
            session.flush()
        return []
    all_eligible_ids = {signal.id for signal in eligible}
    processed_ids = set(
        session.scalars(
            select(SimulationDecision.signal_id).where(
                SimulationDecision.signal_id.in_(all_eligible_ids)
            )
        )
    )
    if processed_ids == all_eligible_ids:
        return list(
            session.scalars(
                select(SimulationFill)
                .join(
                    SimulationDecision,
                    SimulationFill.decision_id == SimulationDecision.id,
                )
                .where(SimulationDecision.signal_id.in_(all_eligible_ids))
                .order_by(SimulationFill.filled_at, SimulationFill.id)
            )
        )
    eligible = [signal for signal in eligible if signal.id not in processed_ids]
    eligible.sort(key=lambda item: item.composite_score, reverse=True)
    evaluation_time = max(
        (_as_aware(signal.generated_at) for signal in signals),
        default=datetime.now(UTC),
    )
    current_state = portfolio_state(session)
    point_in_time_frame = full_frame.loc[
        (pd.to_datetime(full_frame["event_time"], utc=True) <= evaluation_time)
        & (pd.to_datetime(full_frame["available_at"], utc=True) <= evaluation_time)
        & (pd.to_datetime(full_frame["as_of"], utc=True) <= evaluation_time)
    ].copy()
    analytics = portfolio_analytics(session, point_in_time_frame)
    if analytics.nav is None or analytics.max_drawdown is None or analytics.nav <= 0:
        return []
    if analytics.nav / initial_cash - 1 <= -settings.max_simulation_loss:
        return []
    if analytics.max_drawdown <= -settings.max_drawdown:
        return []
    current_values = {
        str(holding["instrument_id"]): float(holding["market_value"])
        for holding in analytics.holdings
    }
    current_weights = {
        instrument_id: value / analytics.nav for instrument_id, value in current_values.items()
    }
    relevant_ids = set(current_state.positions) | {signal.instrument_id for signal in eligible}
    instruments = {
        instrument.id: instrument
        for instrument in session.scalars(
            select(EtfInstrument).where(EtfInstrument.id.in_(relevant_ids or {""}))
        )
    }
    proposed = dict(current_weights)
    increments: dict[str, float] = {}
    remaining_turnover = settings.max_turnover
    for signal in eligible:
        if remaining_turnover <= 0:
            break
        if signal.news_score <= -settings.news_risk_reduction_threshold:
            continue
        current_weight = proposed.get(signal.instrument_id, 0.0)
        target_weight = min(signal.suggested_risk_budget_max, settings.single_etf_cap)
        increment = target_weight - current_weight
        if increment <= 1e-9 or increment > remaining_turnover + 1e-9:
            continue
        trial = {**proposed, signal.instrument_id: target_weight}
        if concentration_rules(trial, instruments, settings):
            continue
        proposed = trial
        increments[signal.instrument_id] = increment
        remaining_turnover -= increment
    fills: list[SimulationFill] = []
    new_decisions: list[tuple[SimulationDecision, Signal]] = []
    for signal in eligible:
        decision_target_weight = proposed.get(signal.instrument_id)
        incremental_weight = increments.get(signal.instrument_id)
        decision_key = _decision_idempotency_key(
            signal.instrument_id,
            signal.generated_at,
        )
        earliest_fill_after = max(_as_aware(signal.available_at), _as_aware(signal.generated_at))
        next_bar = next_executable_bar(
            full_frame,
            signal.instrument_id,
            signal.data_as_of,
            earliest_fill_after=earliest_fill_after,
        )
        filled_at = earliest_fill_after
        if next_bar is not None:
            bar_time = pd.Timestamp(next_bar["event_time"])
            if bar_time.tzinfo is None:
                bar_time = bar_time.tz_localize("UTC")
            # DuckDB materializes TIMESTAMPTZ in the host timezone.  Normalize to
            # UTC before SQL persistence because SQLite otherwise drops the offset
            # and stores the local wall clock, moving the simulated fill by 8 hours.
            filled_at = (
                (bar_time - pd.Timedelta(hours=5, minutes=30)).tz_convert("UTC").to_pydatetime()
            )
        decision = SimulationDecision(
            idempotency_key=decision_key,
            signal_id=signal.id,
            instrument_id=signal.instrument_id,
            decision_type="PAPER_ENTRY_CANDIDATE",
            target_weight=float(
                decision_target_weight or current_weights.get(signal.instrument_id, 0.0)
            ),
            decided_at=_as_aware(signal.generated_at),
            earliest_fill_at=filled_at,
            status="EVALUATED_NO_ALLOCATION",
            rationale=_DECISION_RATIONALE,
        )
        session.add(decision)
        session.flush()
        new_decisions.append((decision, signal))
        if not decision_target_weight or not incremental_weight:
            continue
        if next_bar is None:
            decision.status = "SKIPPED_NO_NEXT_BAR"
            continue
        if filled_at <= earliest_fill_after:
            raise ValueError("下一可成交 bar 必须晚于信号生成和可用时间。")
        decision.status = "PENDING"
        fill_key = hashlib.sha256(
            f"fill:{decision_key}:{filled_at.isoformat()}".encode()
        ).hexdigest()
        existing_fill = session.scalar(
            select(SimulationFill).where(SimulationFill.idempotency_key == fill_key)
        )
        if existing_fill is not None:
            fills.append(existing_fill)
            continue
        reference_price = float(next_bar["open"])
        executable_price = reference_price * (1 + slippage_bps / 10_000)
        fx_rate = float(next_bar.get("fx_rate", 1.0))
        if not math.isfinite(fx_rate) or fx_rate <= 0:
            decision.status = "SKIPPED_INVALID_FX"
            continue
        budget = analytics.nav * incremental_weight
        per_unit_cash = executable_price * fx_rate * (1 + fee_bps / 10_000)
        lot_quantity = math.floor((budget / per_unit_cash) / 100) * 100
        if lot_quantity <= 0:
            decision.status = "SKIPPED_BELOW_LOT"
            continue
        gross = lot_quantity * executable_price * fx_rate
        fee = gross * fee_bps / 10_000
        current_cash = float(
            session.scalar(select(func.coalesce(func.sum(SimulationLedger.cash_delta), 0))) or 0
        )
        if gross + fee > current_cash + 1e-6:
            decision.status = "SKIPPED_CASH"
            continue
        if current_cash - gross - fee < analytics.nav * settings.cash_floor - 1e-6:
            decision.status = "SKIPPED_CASH_FLOOR"
            continue
        projected_value = current_values.get(signal.instrument_id, 0.0) + gross
        if projected_value > analytics.nav * settings.single_etf_cap + 1e-6:
            decision.status = "SKIPPED_SINGLE_ETF_CAP"
            continue
        fill = SimulationFill(
            idempotency_key=fill_key,
            decision_id=decision.id,
            instrument_id=signal.instrument_id,
            filled_at=filled_at,
            side="BUY",
            quantity=lot_quantity,
            executable_price=executable_price,
            gross_amount=gross,
            fee=fee,
            slippage=reference_price * slippage_bps / 10_000 * lot_quantity * fx_rate,
            fx_rate=fx_rate,
        )
        session.add(fill)
        session.flush()
        decision.status = "FILLED"
        ledger_entry = SimulationLedger(
            idempotency_key=f"ledger:{fill_key}",
            fill_id=fill.id,
            instrument_id=signal.instrument_id,
            entry_type="BUY",
            occurred_at=filled_at,
            cash_delta=-(gross + fee),
            quantity_delta=lot_quantity,
            fee_amount=fee,
            memo=_BUY_MEMO,
        )
        session.add(ledger_entry)
        # SessionLocal disables autoflush.  Flush each cash/position entry before
        # evaluating the next candidate so same-batch orders cannot reuse cash.
        session.flush()
        seal_simulation_ledger_entry(session, ledger_entry)
        _, ledger_integrity_hash = _split_integrity_memo(ledger_entry.memo)
        append_audit(
            session,
            event_type="SIMULATION_FILL_RECORDED",
            object_type="SimulationFill",
            object_id=fill.id,
            details={
                "signal_id": signal.id,
                "filled_at": filled_at.isoformat(),
                "price_basis": "NEXT_BAR_OPEN",
                "fee": fee,
                "slippage": fill.slippage,
                "idempotency_key": fill_key,
                "ledger_id": ledger_entry.id,
                "ledger_integrity_hash": ledger_integrity_hash,
            },
        )
        current_values[signal.instrument_id] = projected_value
        fills.append(fill)
    session.flush()
    for decision, signal in new_decisions:
        _record_simulation_decision_audit(session, decision, signal)
    assert_current_execution_governance(
        session,
        signals=eligible,
        full_frame=full_frame,
        settings=settings,
    )
    if commit:
        session.commit()
    else:
        session.flush()
    return fills


def assert_current_execution_governance(
    session: Session,
    *,
    signals: list[Signal],
    full_frame: pd.DataFrame,
    settings: Settings,
) -> None:
    """Revalidate mutable governance state immediately before a commit boundary."""
    session.flush()
    assert_simulation_ledger_integrity(session)
    verified_snapshots: set[str] = set()
    for signal in signals:
        current_signal = session.get(Signal, signal.id, populate_existing=True)
        if (
            current_signal is None
            or not verify_signal_record(current_signal)
            or not current_signal.is_current
            or current_signal.state != SignalState.ENTRY_CANDIDATE.value
            or current_signal.latency_status != "ON_TIME"
            or bool(current_signal.risk_rules_hit)
        ):
            raise SnapshotIntegrityError("提交前候选信号状态或完整性已变化。")
        snapshot = session.get(
            DataSnapshot, current_signal.data_snapshot_id, populate_existing=True
        )
        instrument = session.get(
            EtfInstrument, current_signal.instrument_id, populate_existing=True
        )
        model = session.get(ModelVersion, current_signal.model_version_id, populate_existing=True)
        if (
            snapshot is None
            or instrument is None
            or snapshot.dataset_type != "MARKET_BARS"
            or snapshot.data_mode != current_signal.data_mode
            or not snapshot.license_scope
            or "UNCLEAR" in snapshot.license_scope.upper()
            or not instrument.active
            or instrument.leveraged
            or instrument.inverse
            or instrument.liquidity_tier > 2
            or rule_model_governance_rules(model, data_mode=current_signal.data_mode)
        ):
            raise SnapshotIntegrityError("提交前数据、模型或标的风险门禁已变化。")
        if snapshot.id not in verified_snapshots:
            verify_snapshot_frame(
                full_frame,
                expected_hash=snapshot.snapshot_hash,
                expected_rows=snapshot.row_count,
            )
            if set(full_frame["provider"].dropna().astype(str).unique()) != {
                snapshot.provider_code
            } or set(full_frame["license_scope"].dropna().astype(str).unique()) != {
                snapshot.license_scope
            }:
                raise SnapshotIntegrityError("提交前行情事实血缘或许可范围已变化。")
            verified_snapshots.add(snapshot.id)
        authorize_provider(
            session,
            snapshot.provider_code,
            purposes={"display", "algorithm", "derivative", "cache"},
            at=datetime.now(UTC),
            market=instrument.mic,
            region=instrument.region,
        )
        news_lineage_ok, news_lineage_hash = verify_signal_news_lineage(
            session, current_signal, snapshot=snapshot
        )
        if not news_lineage_ok or news_lineage_hash is None:
            raise SnapshotIntegrityError("提交前新闻集合或来源血缘已变化。")
        macro_snapshot = _macro_snapshot_from_lineage(session, current_signal)
        expected_policy_hash = decision_policy_hash(
            session,
            snapshot=snapshot,
            settings=settings,
            macro_snapshot=macro_snapshot,
            model=model,
            news_lineage_hash=news_lineage_hash,
            portfolio_risk_context_hash=(current_signal.feature_values or {}).get(
                "portfolio_risk_context_hash"
            ),
        )
        if (current_signal.feature_values or {}).get(
            "decision_policy_hash"
        ) != expected_policy_hash:
            raise SnapshotIntegrityError("提交前决策策略已变化。")


def _macro_snapshot_from_lineage(session: Session, signal: Signal) -> DataSnapshot | None:
    lineage = (signal.feature_values or {}).get("macro_lineage")
    if not isinstance(lineage, dict) or lineage.get("status") != "AVAILABLE":
        return None
    snapshot_id = lineage.get("snapshot_id")
    if not isinstance(snapshot_id, str):
        return None
    session.flush()
    snapshot = session.get(DataSnapshot, snapshot_id, populate_existing=True)
    expected_hash = lineage.get("snapshot_hash")
    market_snapshot = session.get(DataSnapshot, signal.data_snapshot_id, populate_existing=True)
    if (
        snapshot is None
        or market_snapshot is None
        or snapshot.snapshot_hash != expected_hash
        or snapshot.dataset_type != "MACRO_FACTS"
        or snapshot.provider_code != market_snapshot.provider_code
        or snapshot.data_mode != signal.data_mode
        or not snapshot.license_scope
        or "UNCLEAR" in snapshot.license_scope.upper()
        or {
            "HASH_MISMATCH",
            "LICENSE_UNCLEAR",
            "SCHEMA_UNVERIFIED",
            "TIME_VALIDATION_FAILED",
        }.intersection(set(snapshot.quality_flags or []))
    ):
        raise SnapshotIntegrityError("宏观快照血缘、许可或质量门禁未通过。")
    authorize_provider(
        session,
        snapshot.provider_code,
        purposes={"algorithm", "derivative", "cache"},
        at=datetime.now(UTC),
    )
    try:
        frame = pd.read_parquet(snapshot.parquet_uri)
    except (OSError, ValueError) as exc:
        raise SnapshotIntegrityError("宏观快照无法读取。") from exc
    verify_snapshot_frame(
        frame,
        expected_hash=snapshot.snapshot_hash,
        expected_rows=snapshot.row_count,
    )
    validate_fact_frame(frame)
    return snapshot


def next_executable_bar(
    frame: pd.DataFrame,
    instrument_id: str,
    signal_data_as_of: datetime,
    *,
    earliest_fill_after: datetime | None = None,
    verification_delay_minutes: int = 30,
) -> pd.Series | None:
    cutoff = pd.Timestamp(signal_data_as_of)
    if cutoff.tzinfo is None:
        cutoff = cutoff.tz_localize("UTC")
    else:
        cutoff = cutoff.tz_convert("UTC")
    instrument = frame.loc[frame["instrument_id"] == instrument_id].copy()
    required_times = {"event_time", "published_at", "first_seen_at", "available_at", "as_of"}
    missing_times = required_times - set(instrument.columns)
    if missing_times:
        raise SnapshotIntegrityError(f"下一可成交 bar 缺少因果时间字段：{sorted(missing_times)}")
    event_times = pd.to_datetime(instrument["event_time"], utc=True)
    verification_cutoff = event_times + pd.Timedelta(minutes=verification_delay_minutes)
    eligible = event_times > cutoff
    for column in required_times:
        values = pd.to_datetime(instrument[column], utc=True, errors="raise")
        eligible &= values <= verification_cutoff
    if earliest_fill_after is not None:
        earliest = pd.Timestamp(_as_aware(earliest_fill_after)).tz_convert("UTC")
        derived_fill_times = event_times - pd.Timedelta(hours=5, minutes=30)
        eligible &= derived_fill_times > earliest
    later = instrument.loc[eligible].sort_values("event_time")
    return None if later.empty else later.iloc[0]


def _as_aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def _as_utc_timestamp(value: datetime) -> pd.Timestamp:
    timestamp = pd.Timestamp(_as_aware(value))
    return timestamp.tz_convert("UTC")

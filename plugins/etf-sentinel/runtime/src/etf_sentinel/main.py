from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import desc, select, text
from sqlalchemy.orm import Session

from etf_sentinel.config import get_settings
from etf_sentinel.database import get_db
from etf_sentinel.enums import DISCLAIMER, DataMode
from etf_sentinel.models import (
    Alert,
    AuditLog,
    BacktestExperiment,
    DataSnapshot,
    EtfInstrument,
    ModelVersion,
    ProviderRegistry,
    ScheduledEvent,
    Signal,
    SimulationFill,
    TaskRun,
)
from etf_sentinel.providers.base import LicenseGateError, authorize_provider
from etf_sentinel.schemas import ApiEnvelope, HealthResponse, SignalResponse
from etf_sentinel.services.alerts import (
    AlertIntegrityError,
    acknowledge_alert,
    eligible_scheduled_events,
    verify_alert_content,
)
from etf_sentinel.services.backtest import (
    BACKTEST_ARTIFACT_INTEGRITY_KEY,
    verify_backtest_artifact,
)
from etf_sentinel.services.fact_integrity import verify_scheduled_event_record
from etf_sentinel.services.ingestion import SnapshotIntegrityError
from etf_sentinel.services.ledger import (
    SimulationLedgerIntegrityError,
    assess_portfolio_risk_gate,
    portfolio_analytics,
    portfolio_state,
)
from etf_sentinel.services.logistic_model import (
    LOGISTIC_ARTIFACT_INTEGRITY_KEY,
    verify_logistic_model_artifact,
)
from etf_sentinel.services.monitoring import MONITOR_TASK_NAME, monitoring_window, schedule_state
from etf_sentinel.services.pipeline import read_snapshot_with_duckdb
from etf_sentinel.services.signals import (
    code_version,
    decision_policy_hash,
    eligible_news_events,
    rule_model_governance_rules,
    verify_signal_news_lineage,
    verify_signal_record,
)

settings = get_settings()
PACKAGE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(PACKAGE_DIR / "templates"))
DbSession = Annotated[Session, Depends(get_db)]


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Instantiating settings above is intentional: unsafe configuration refuses startup.
    yield


app = FastAPI(
    title="ETF Sentinel",
    description="企业自有资金内部研究和风险监测；不提供交易功能。",
    version="0.1.0",
    docs_url="/api/docs" if settings.app_env != "production" else None,
    redoc_url=None,
    lifespan=lifespan,
)
static_dir = PACKAGE_DIR / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.middleware("http")
async def security_headers(request: Request, call_next):
    server = request.scope.get("server")
    server_host = server[0] if isinstance(server, (list, tuple)) and server else None
    if settings.app_host in {"127.0.0.1", "localhost"} and server_host not in {
        "127.0.0.1",
        "localhost",
        "::1",
        "testserver",  # Starlette's in-process ASGI test transport.
        None,  # TestClient has no real listening socket.
    }:
        return JSONResponse(status_code=403, content={"detail": "非 localhost 请求已阻断"})
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; style-src 'self'; "
        "script-src 'self' https://cdn.jsdelivr.net; "
        "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'"
    )
    response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/health", response_model=HealthResponse)
def health(db: DbSession) -> HealthResponse:
    db.execute(text("SELECT 1"))
    return HealthResponse(
        status="ok",
        trading_mode=settings.trading_mode,
        data_mode=_current_data_mode(db),
        database="ok",
    )


@app.get("/health/live")
def health_live() -> dict[str, str]:
    return {"status": "alive"}


@app.get("/health/ready", response_model=HealthResponse)
def health_ready(db: DbSession) -> HealthResponse:
    response = health(db)
    fail_closed, _reason = _candidate_output_health(db)
    if fail_closed:
        response.status = "degraded_fail_closed"
    return response


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request, db: DbSession):
    signals = _visible_signals(db, limit=100)
    alert_rows = list(db.scalars(select(Alert).order_by(desc(Alert.created_at)).limit(100)))
    signal_output_health = _candidate_output_health(db)
    alerts = [
        _alert_display_view(db, row, signal_output_health=signal_output_health)
        for row in alert_rows
    ]
    providers = list(db.scalars(select(ProviderRegistry).order_by(ProviderRegistry.provider_code)))
    models = [
        _model_display_view(db, row)
        for row in db.scalars(select(ModelVersion).order_by(desc(ModelVersion.created_at)))
    ]
    backtest_row = db.scalar(
        select(BacktestExperiment).order_by(desc(BacktestExperiment.created_at)).limit(1)
    )
    backtest = _backtest_display_view(db, backtest_row)
    etfs = _visible_etfs(db)
    market_snapshot = db.scalar(
        select(DataSnapshot)
        .where(DataSnapshot.dataset_type == "MARKET_BARS")
        .order_by(desc(DataSnapshot.ingested_at))
        .limit(1)
    )
    display_clock = (
        settings.demo_evaluation_time
        if market_snapshot is not None and market_snapshot.data_mode == DataMode.DEMO_FIXTURE.value
        else datetime.now(UTC)
    )
    news_events = (
        eligible_news_events(
            db,
            snapshot=market_snapshot,
            evaluation_time=display_clock,
            purposes={"display", "cache"},
            require_exposure=False,
        )
        if market_snapshot is not None and not signal_output_health[0]
        else []
    )
    news_events = sorted(news_events, key=lambda row: row.first_seen_at, reverse=True)[:30]
    audit_logs = list(db.scalars(select(AuditLog).order_by(desc(AuditLog.occurred_at)).limit(50)))
    upcoming_events = eligible_scheduled_events(
        db,
        list(db.scalars(select(ScheduledEvent).order_by(ScheduledEvent.scheduled_at).limit(30))),
        now=display_clock,
    )
    task_runs = list(db.scalars(select(TaskRun).order_by(desc(TaskRun.started_at)).limit(20)))
    portfolio = _portfolio_view(db)
    fills = (
        []
        if portfolio["valuation_error"] == "LEDGER_INTEGRITY_BLOCKED"
        else list(db.scalars(select(SimulationFill).order_by(desc(SimulationFill.filled_at))))
    )
    summary = {
        "etf_count": len(etfs),
        "signal_count": len(signals),
        "candidate_count": sum("CANDIDATE" in row.state for row in signals),
        "blocked_count": sum(row.state in {"BLOCKED_BY_RISK", "DATA_STALE"} for row in signals),
        "unacknowledged_alerts": sum(row["status"] != "ACKNOWLEDGED" for row in alerts),
        "fill_count": len(fills),
    }
    portfolio["fills"] = fills
    settings_view = {
        "trading_mode": settings.trading_mode,
        "global_kill_switch": settings.global_kill_switch,
        "single_etf_cap": settings.single_etf_cap,
        "cash_floor": settings.cash_floor,
        "max_turnover": settings.max_turnover,
        "portfolio_volatility_target": settings.portfolio_volatility_target,
        "public_service": settings.enable_public_service,
        "live_trading": settings.enable_live_trading,
        "order_entry": settings.enable_order_entry,
    }
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={
            "data_mode": _current_data_mode(db),
            "disclaimer": DISCLAIMER,
            "summary": summary,
            "signals": signals,
            "alerts": alerts,
            "providers": providers,
            "models": models,
            "backtest": backtest,
            "portfolio": portfolio,
            "audit_logs": audit_logs,
            "etfs": etfs,
            "news_events": news_events,
            "upcoming_events": upcoming_events,
            "task_runs": task_runs,
            "settings_view": settings_view,
            "chart_data": _chart_data(signals, backtest),
        },
    )


@app.get("/signals/{signal_id}", response_class=HTMLResponse)
def signal_detail(signal_id: str, request: Request, db: DbSession):
    signal = db.get(Signal, signal_id)
    if signal is None:
        raise HTTPException(status_code=404, detail="信号不存在")
    if not signal.is_current:
        raise HTTPException(
            status_code=410,
            detail="该历史信号已失效，仅保留于审计记录，不可用于研究决策。",
        )
    if not verify_signal_record(signal):
        raise HTTPException(status_code=503, detail="信号记录完整性校验失败，已失败关闭。")
    fail_closed, fail_closed_reason = _candidate_output_health(db)
    if fail_closed:
        raise HTTPException(
            status_code=503,
            detail=f"信号详情已失败关闭：{fail_closed_reason}",
        )
    return templates.TemplateResponse(
        request=request,
        name="signal_detail.html",
        context={
            "data_mode": signal.data_mode,
            "disclaimer": DISCLAIMER,
            "signal": signal,
            "instrument": db.get(EtfInstrument, signal.instrument_id),
            "snapshot": db.get(DataSnapshot, signal.data_snapshot_id),
            "model": db.get(ModelVersion, signal.model_version_id),
        },
    )


@app.get("/api/v1/status", response_model=ApiEnvelope)
def api_status(db: DbSession) -> ApiEnvelope:
    latest_task = _latest_research_task(db)
    fail_closed, fail_closed_reason = _candidate_output_health(db)
    return _envelope(
        db,
        {
            "trading_mode": settings.trading_mode,
            "global_kill_switch": settings.global_kill_switch,
            "public_service": False,
            "live_trading": False,
            "order_entry": False,
            "latest_pipeline_status": latest_task.status if latest_task else "NOT_RUN",
            "candidate_output_fail_closed": fail_closed,
            "candidate_output_fail_closed_reason": fail_closed_reason,
        },
    )


@app.get("/api/v1/monitoring", response_model=ApiEnvelope)
def api_monitoring(db: DbSession) -> ApiEnvelope:
    # Never return a persisted report as current: licensing can expire between runs.
    return _envelope(db, build_monitoring_report(db))


def build_monitoring_report(db: Session) -> dict[str, Any]:
    """Shared read-only projection used by HTTP and the Celery monitoring job.

    Keeping this alongside existing display projections gives both consumers the
    same source, license, integrity, model and portfolio gates; no shadow signals.
    """
    now = datetime.now(UTC)
    _start, next_check = monitoring_window(now, settings.monitoring_interval_hours)
    task = db.scalar(
        select(TaskRun)
        .where(TaskRun.task_name == MONITOR_TASK_NAME)
        .order_by(desc(TaskRun.started_at))
        .limit(1)
    )
    blocked, reason = _candidate_output_health(db)
    mode = _current_data_mode(db)
    # Hourly real-data ingestion is intentionally unavailable until separately approved.
    if mode != DataMode.DEMO_FIXTURE.value:
        blocked, reason = True, "LIVE_MONITORING_NOT_APPROVED"
    rows = [] if blocked else list(db.scalars(select(Signal).where(Signal.is_current.is_(True))))
    counts = {
        "signals": len(rows),
        "blocked": sum(row.state in {"BLOCKED_BY_RISK", "DATA_STALE"} for row in rows),
        "watch": sum(row.state == "WATCH" for row in rows),
        "candidates": sum("CANDIDATE" in row.state for row in rows),
    }
    cutoff = max((row.data_as_of for row in rows), default=None)
    if cutoff is not None and cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=UTC)
    findings = [
        "固定 DEMO_FIXTURE 只用于流程演示；定时检查不会生成新的市场价格。",
        "真实行情接入尚被许可与流水线验收门禁阻断；本次未向真实供应商发起采集。",
    ]
    if blocked:
        findings.append("数据或风险门禁未通过，停止展示候选统计与派生分析。")
    elif rows:
        findings.append(
            f"当前演示信号 {len(rows):,} 条，其中观察 {counts['watch']:,} 条、"
            f"风险阻断或过期 {counts['blocked']:,} 条；不代表实际市场机会。"
        )
        findings.append(
            f"市场、宏观、新闻、流动性分项均值分别为 "
            f"{sum(row.market_score for row in rows) / len(rows):.2f}、"
            f"{sum(row.macro_score for row in rows) / len(rows):.2f}、"
            f"{sum(row.news_score for row in rows) / len(rows):.2f}、"
            f"{sum(row.liquidity_score for row in rows) / len(rows):.2f}；"
            "仅为已有收盘信号的描述统计，不是新预测。"
        )
        rejected = db.scalar(
            select(ModelVersion.id).where(ModelVersion.status == "REJECTED").limit(1)
        )
        if rejected:
            findings.append("存在 REJECTED 模型：未优于基线的结果仍被保留，不作生产预测。")
    last = task.completed_at if task else None
    if last is not None and last.tzinfo is None:
        last = last.replace(tzinfo=UTC)
    return {
        "data_mode": mode,
        "interval_hours": settings.monitoring_interval_hours,
        "checked_at": now.isoformat(),
        "next_check_at": next_check.isoformat(),
        "last_scheduled_at": last.isoformat() if last else None,
        "schedule_status": schedule_state(task, now, settings.monitoring_interval_hours),
        "market_data_as_of": cutoff.isoformat() if cutoff else None,
        "market_data_age_hours": round((now - cutoff).total_seconds() / 3600, 2)
        if cutoff
        else None,
        "live_data_status": "COMPLIANCE_BLOCKED",
        "analysis_status": "BLOCKED" if blocked else "DEMO_ANALYSIS",
        "blocked_reason": reason,
        "headline": "数据门禁阻断" if blocked else "演示数据检查完成，真实行情未启用",
        "findings": findings,
        "counts": counts,
        "source_links": sorted({link for row in rows for link in row.source_links}),
        "data_snapshot_ids": sorted({row.data_snapshot_id for row in rows}),
        "code_version": code_version(),
        "scheduler_note": "每 1 或 2 小时按北京时间整点检查；收盘候选仍每日生成。"
        "页面轮询只读取当前分析，不产生候选或模拟成交。",
    }


@app.get("/api/v1/signals", response_model=ApiEnvelope)
def api_signals(db: DbSession) -> ApiEnvelope:
    rows = _visible_signals(db, limit=200)
    return _envelope(
        db, [SignalResponse.model_validate(row).model_dump(mode="json") for row in rows]
    )


@app.get("/api/v1/etfs", response_model=ApiEnvelope)
def api_etfs(db: DbSession) -> ApiEnvelope:
    rows = _visible_etfs(db)
    return _envelope(
        db,
        [
            {
                "id": row.id,
                "name_zh": row.name_zh,
                "provider_symbol": row.provider_symbol,
                "figi": row.figi,
                "isin": row.isin,
                "mic": row.mic,
                "currency": row.currency,
                "asset_class": row.asset_class,
                "industry": row.industry,
                "region": row.region,
                "leveraged": row.leveraged,
                "inverse": row.inverse,
            }
            for row in rows
        ],
    )


@app.get("/api/v1/alerts", response_model=ApiEnvelope)
def api_alerts(db: DbSession) -> ApiEnvelope:
    rows = list(db.scalars(select(Alert).order_by(desc(Alert.created_at)).limit(200)))
    signal_output_health = _candidate_output_health(db)
    views = [
        _alert_display_view(db, row, signal_output_health=signal_output_health) for row in rows
    ]
    return _envelope(
        db,
        [
            {
                "id": row["id"],
                "type": row["alert_type"],
                "severity": row["severity"],
                "status": row["status"],
                "title": row["title"],
                "message": row["message"],
                "data_as_of": row["data_as_of"].isoformat(),
                "latency_status": row["latency_status"],
                "trigger_reason": row["trigger_reason"],
                "confidence": row["confidence"],
                "sources": row["source_links"],
                "invalidation_conditions": row["invalidation_conditions"],
                "display_blocked_reason": row["display_blocked_reason"],
            }
            for row in views
        ],
    )


@app.post("/api/v1/alerts/{alert_id}/acknowledge", response_model=ApiEnvelope)
def api_acknowledge_alert(
    alert_id: str,
    db: DbSession,
    x_etf_sentinel_intent: str | None = Header(default=None),
) -> ApiEnvelope:
    if x_etf_sentinel_intent != "acknowledge":
        raise HTTPException(status_code=400, detail="缺少明确的确认意图头")
    try:
        alert = acknowledge_alert(db, alert_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except AlertIntegrityError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _envelope(db, {"id": alert.id, "status": alert.status})


@app.get("/api/v1/portfolio", response_model=ApiEnvelope)
def api_portfolio(db: DbSession) -> ApiEnvelope:
    return _envelope(db, _portfolio_view(db))


@app.get("/api/v1/backtests/latest", response_model=ApiEnvelope)
def api_backtest(db: DbSession) -> ApiEnvelope:
    row = db.scalar(
        select(BacktestExperiment).order_by(desc(BacktestExperiment.created_at)).limit(1)
    )
    view = _backtest_display_view(db, row)
    data = None if view is None else dict(view)
    if data is not None:
        data["baselines"] = data.pop("baseline_metrics")
    return _envelope(db, data)


@app.get("/api/v1/providers", response_model=ApiEnvelope)
def api_providers(db: DbSession) -> ApiEnvelope:
    rows = list(db.scalars(select(ProviderRegistry).order_by(ProviderRegistry.provider_code)))
    return _envelope(
        db,
        [
            {
                "provider_code": row.provider_code,
                "purpose": row.purpose,
                "review_status": row.review_status,
                "expires_at": row.expires_at.isoformat() if row.expires_at else None,
                "rights": {
                    "display": row.display_right,
                    "algorithm": row.non_display_algorithm_right,
                    "derivative": row.derivative_right,
                    "cache": row.cache_right,
                    "training": row.training_right,
                    "redistribution": row.redistribution_right,
                },
            }
            for row in rows
        ],
    )


@app.get("/api/v1/models", response_model=ApiEnvelope)
def api_models(db: DbSession) -> ApiEnvelope:
    rows = list(db.scalars(select(ModelVersion).order_by(desc(ModelVersion.created_at))))
    return _envelope(db, [_model_display_view(db, row) for row in rows])


@app.get("/api/v1/audit", response_model=ApiEnvelope)
def api_audit(db: DbSession) -> ApiEnvelope:
    rows = list(db.scalars(select(AuditLog).order_by(desc(AuditLog.occurred_at)).limit(200)))
    return _envelope(
        db,
        [
            {
                "event_type": row.event_type,
                "object_type": row.object_type,
                "object_id": row.object_id,
                "occurred_at": row.occurred_at.isoformat(),
                "trace_id": row.trace_id,
                "details": row.details,
                "previous_hash": row.previous_hash,
                "record_hash": row.record_hash,
            }
            for row in rows
        ],
    )


def _current_data_mode(db: Session) -> str:
    mode = db.scalar(
        select(DataSnapshot.data_mode)
        .where(DataSnapshot.dataset_type == "MARKET_BARS")
        .order_by(desc(DataSnapshot.ingested_at))
        .limit(1)
    )
    return mode or DataMode.DEMO_FIXTURE.value


def _envelope(db: Session, data: Any) -> ApiEnvelope:
    return ApiEnvelope(data_mode=_current_data_mode(db), disclaimer=DISCLAIMER, data=data)


def _visible_etfs(db: Session) -> list[EtfInstrument]:
    rows = list(db.scalars(select(EtfInstrument).order_by(EtfInstrument.provider_symbol)))
    decisions: dict[tuple[str, str, str], bool] = {}
    visible: list[EtfInstrument] = []
    for row in rows:
        scope = (row.provider_code, row.mic, row.region)
        allowed = decisions.get(scope)
        if allowed is None:
            try:
                authorize_provider(
                    db,
                    row.provider_code,
                    purposes={"display", "cache"},
                    at=datetime.now(UTC),
                    market=row.mic,
                    region=row.region,
                )
            except LicenseGateError:
                allowed = False
            else:
                allowed = True
            decisions[scope] = allowed
        if allowed:
            visible.append(row)
    return visible


def _alert_display_view(
    db: Session,
    row: Alert,
    *,
    signal_output_health: tuple[bool, str | None] | None = None,
) -> dict[str, Any]:
    view = {
        "id": row.id,
        "alert_type": row.alert_type,
        "severity": row.severity,
        "status": row.status,
        "title": row.title,
        "message": row.message,
        "instrument_id": row.instrument_id,
        "signal_id": row.signal_id,
        "data_as_of": row.data_as_of,
        "latency_status": row.latency_status,
        "trigger_reason": row.trigger_reason,
        "confidence": row.confidence,
        "source_links": row.source_links,
        "invalidation_conditions": row.invalidation_conditions,
        "disclaimer": row.disclaimer,
        "channel": row.channel,
        "delivery_attempts": row.delivery_attempts,
        "created_at": row.created_at,
        "sent_at": row.sent_at,
        "acknowledged_at": row.acknowledged_at,
        "acknowledged_by": row.acknowledged_by,
        "display_blocked_reason": None,
    }
    blocked_reason = _alert_lineage_display_reason(
        db, row, signal_output_health=signal_output_health
    )
    if blocked_reason:
        safe_trigger_reason = (
            "SIGNAL_DERIVED_DISPLAY_BLOCKED"
            if row.source_object_type == "Signal"
            and blocked_reason != "ALERT_CONTENT_INTEGRITY_FAILED"
            else "ALERT_DERIVED_DISPLAY_BLOCKED"
        )
        view.update(
            {
                "title": "历史预警（派生内容展示已阻断）",
                "message": "关联来源的当前许可、模型、血缘或完整性门禁未通过。",
                "latency_status": "UNAVAILABLE",
                "trigger_reason": safe_trigger_reason,
                "confidence": None,
                "source_links": [],
                "invalidation_conditions": ["完成数据权利、模型与快照完整性复核"],
                "display_blocked_reason": blocked_reason,
            }
        )
    return view


def _alert_lineage_display_reason(
    db: Session,
    row: Alert,
    *,
    signal_output_health: tuple[bool, str | None] | None = None,
) -> str | None:
    try:
        if not verify_alert_content(row):
            return "ALERT_CONTENT_INTEGRITY_FAILED"
        if row.source_object_type == "System":
            if row.alert_type not in {"SYSTEM_HEALTH", "PORTFOLIO_RISK"} or (
                row.provider_code is not None
            ):
                return "ALERT_SYSTEM_LINEAGE_INVALID"
            return None
        if row.source_object_type == "Signal":
            output_health = signal_output_health or _candidate_output_health(db)
            if output_health[0]:
                return f"CURRENT_CANDIDATE_OUTPUT_BLOCKED:{output_health[1]}"
            if not row.source_object_id or row.source_object_id != row.signal_id:
                return "ALERT_SIGNAL_LINEAGE_INVALID"
            signal = db.get(Signal, row.source_object_id, populate_existing=True)
            if signal is None or not verify_signal_record(signal):
                return "SIGNAL_RECORD_INTEGRITY_FAILED"
            snapshot = db.get(DataSnapshot, signal.data_snapshot_id, populate_existing=True)
            instrument = db.get(EtfInstrument, signal.instrument_id, populate_existing=True)
            model = db.get(ModelVersion, signal.model_version_id, populate_existing=True)
            if (
                snapshot is None
                or instrument is None
                or row.provider_code != snapshot.provider_code
                or snapshot.dataset_type != "MARKET_BARS"
                or snapshot.data_mode != signal.data_mode
                or not snapshot.license_scope
                or "UNCLEAR" in snapshot.license_scope.upper()
            ):
                return "ALERT_SIGNAL_LINEAGE_INVALID"
            model_rules = rule_model_governance_rules(model, data_mode=signal.data_mode)
            if model_rules:
                return "MODEL_GOVERNANCE_BLOCKED"
            news_lineage_ok, _news_hash = verify_signal_news_lineage(db, signal, snapshot=snapshot)
            if not news_lineage_ok:
                return "NEWS_LINEAGE_BLOCKED"
            authorize_provider(
                db,
                snapshot.provider_code,
                purposes={"display", "derivative", "cache"},
                at=datetime.now(UTC),
                market=instrument.mic,
                region=instrument.region,
            )
            read_snapshot_with_duckdb(
                snapshot.parquet_uri,
                expected_hash=snapshot.snapshot_hash,
                expected_rows=snapshot.row_count,
            )
            return None
        if row.source_object_type == "ScheduledEvent":
            if not row.source_object_id:
                return "ALERT_EVENT_LINEAGE_INVALID"
            event = db.get(ScheduledEvent, row.source_object_id, populate_existing=True)
            if (
                event is None
                or not verify_scheduled_event_record(event)
                or row.provider_code != event.provider_code
                or not event.is_current
                or event.status != "SCHEDULED"
                or not event.license_scope
                or "UNCLEAR" in event.license_scope.upper()
                or len(event.raw_payload_hash) != 64
            ):
                return "ALERT_EVENT_LINEAGE_INVALID"
            blocked_flags = {
                "HASH_MISMATCH",
                "LICENSE_UNCLEAR",
                "MIGRATED_LINEAGE_REVIEW_REQUIRED",
                "SCHEMA_UNVERIFIED",
                "TIME_VALIDATION_FAILED",
            }
            if blocked_flags.intersection(set(event.quality_flags or [])):
                return "ALERT_EVENT_QUALITY_BLOCKED"
            authorize_provider(
                db,
                event.provider_code,
                purposes={"display", "cache"},
                at=datetime.now(UTC),
            )
            for region in event.regions or []:
                authorize_provider(
                    db,
                    event.provider_code,
                    purposes={"display", "cache"},
                    at=datetime.now(UTC),
                    region=str(region),
                )
            return None
        return "ALERT_LINEAGE_UNVERIFIED"
    except LicenseGateError:
        return "PROVIDER_LICENSE_BLOCKED"
    except (OSError, RuntimeError, TypeError, ValueError):
        return "ALERT_SOURCE_INTEGRITY_BLOCKED"


def _visible_signals(db: Session, *, limit: int) -> list[Signal]:
    fail_closed, _reason = _candidate_output_health(db)
    if fail_closed:
        return []
    query = select(Signal).where(Signal.is_current.is_(True))
    rows = list(db.scalars(query.order_by(desc(Signal.recorded_at)).limit(limit)))
    return [row for row in rows if verify_signal_record(row)]


def _latest_research_task(db: Session) -> TaskRun | None:
    # Monitoring success must never mask a failed or missing research pipeline.
    return db.scalar(
        select(TaskRun)
        .where(TaskRun.task_name == "daily_demo_pipeline")
        .order_by(desc(TaskRun.started_at))
        .limit(1)
    )


def _candidate_output_health(db: Session) -> tuple[bool, str | None]:
    if settings.global_kill_switch:
        return True, "GLOBAL_KILL_SWITCH"
    latest_task = _latest_research_task(db)
    if latest_task is None:
        return True, "PIPELINE_NOT_RUN"
    if latest_task.status != "SUCCEEDED":
        return True, f"PIPELINE_{latest_task.status}"
    current_signals = list(
        db.scalars(
            select(Signal)
            .where(Signal.is_current.is_(True))
            .execution_options(populate_existing=True)
        )
    )
    if not current_signals:
        return True, "NO_CURRENT_SIGNALS"
    verified_snapshots: dict[str, DataSnapshot] = {}
    verified_frames: dict[str, Any] = {}
    expected_policies: dict[tuple[str, str | None, str, str | None], str] = {}

    def verify_snapshot(snapshot_id: str) -> DataSnapshot | None:
        if snapshot_id in verified_snapshots:
            return verified_snapshots[snapshot_id]
        snapshot = db.get(DataSnapshot, snapshot_id, populate_existing=True)
        if snapshot is None:
            return None
        try:
            if not snapshot.license_scope or "UNCLEAR" in snapshot.license_scope.upper():
                return None
            authorize_provider(
                db,
                snapshot.provider_code,
                purposes={"display", "algorithm", "derivative", "cache"},
                at=datetime.now(UTC),
            )
            verified_frames[snapshot_id] = read_snapshot_with_duckdb(
                snapshot.parquet_uri,
                expected_hash=snapshot.snapshot_hash,
                expected_rows=snapshot.row_count,
            )
        except (LicenseGateError, OSError, RuntimeError, TypeError, ValueError):
            return None
        verified_snapshots[snapshot_id] = snapshot
        return snapshot

    for signal in current_signals:
        if not verify_signal_record(signal):
            return True, "SIGNAL_RECORD_INTEGRITY_FAILED"
        model = db.get(ModelVersion, signal.model_version_id, populate_existing=True)
        model_rules = rule_model_governance_rules(model, data_mode=signal.data_mode)
        if model_rules:
            return True, f"MODEL_GOVERNANCE_BLOCKED:{','.join(model_rules)}"
        instrument = db.get(EtfInstrument, signal.instrument_id, populate_existing=True)
        if instrument is None:
            return True, "INSTRUMENT_MASTER_MISSING"
        market_snapshot = verify_snapshot(signal.data_snapshot_id)
        if (
            market_snapshot is None
            or market_snapshot.dataset_type != "MARKET_BARS"
            or market_snapshot.data_mode != signal.data_mode
        ):
            return True, "MARKET_SNAPSHOT_INTEGRITY_FAILED"
        try:
            authorize_provider(
                db,
                market_snapshot.provider_code,
                purposes={"display", "algorithm", "derivative", "cache"},
                at=datetime.now(UTC),
                market=instrument.mic,
                region=instrument.region,
            )
        except LicenseGateError:
            return True, "MARKET_OR_REGION_LICENSE_BLOCKED"
        try:
            news_lineage_ok, news_lineage_hash = verify_signal_news_lineage(
                db, signal, snapshot=market_snapshot
            )
        except (SnapshotIntegrityError, RuntimeError, TypeError, ValueError):
            return True, "NEWS_LINEAGE_INTEGRITY_FAILED"
        if not news_lineage_ok or news_lineage_hash is None:
            return True, "NEWS_LINEAGE_INTEGRITY_FAILED"
        lineage = (signal.feature_values or {}).get("macro_lineage")
        if not isinstance(lineage, dict):
            return True, "MACRO_LINEAGE_MISSING"
        macro_snapshot = None
        if lineage.get("status") == "AVAILABLE":
            snapshot_id = lineage.get("snapshot_id")
            if not isinstance(snapshot_id, str):
                return True, "MACRO_LINEAGE_INVALID"
            macro_snapshot = verify_snapshot(snapshot_id)
            if (
                macro_snapshot is None
                or macro_snapshot.dataset_type != "MACRO_FACTS"
                or macro_snapshot.snapshot_hash != lineage.get("snapshot_hash")
                or macro_snapshot.data_mode != signal.data_mode
            ):
                return True, "MACRO_SNAPSHOT_INTEGRITY_FAILED"
        elif lineage.get("status") != "UNAVAILABLE_REWEIGHTED":
            return True, "MACRO_LINEAGE_INVALID"
        portfolio_context_hash = (signal.feature_values or {}).get("portfolio_risk_context_hash")
        policy_key = (
            market_snapshot.id,
            macro_snapshot.id if macro_snapshot else None,
            news_lineage_hash,
            portfolio_context_hash,
        )
        expected_policy = expected_policies.get(policy_key)
        if expected_policy is None:
            expected_policy = decision_policy_hash(
                db,
                snapshot=market_snapshot,
                settings=settings,
                macro_snapshot=macro_snapshot,
                model=model,
                news_lineage_hash=news_lineage_hash,
                portfolio_risk_context_hash=portfolio_context_hash,
            )
            expected_policies[policy_key] = expected_policy
        if (signal.feature_values or {}).get("decision_policy_hash") != expected_policy:
            return True, "DECISION_POLICY_CHANGED"
    market_snapshot_ids = {signal.data_snapshot_id for signal in current_signals}
    if len(market_snapshot_ids) != 1:
        return True, "CURRENT_SIGNAL_SNAPSHOT_SET_INVALID"
    portfolio_snapshot = verified_snapshots.get(next(iter(market_snapshot_ids)))
    portfolio_frame = verified_frames.get(next(iter(market_snapshot_ids)))
    if portfolio_snapshot is None or portfolio_frame is None:
        return True, "PORTFOLIO_MARKET_DATA_UNAVAILABLE"
    try:
        portfolio_gate = assess_portfolio_risk_gate(
            db,
            portfolio_frame,
            settings=settings,
            evaluation_time=(
                None
                if portfolio_snapshot.data_mode == DataMode.DEMO_FIXTURE.value
                else datetime.now(UTC)
            ),
        )
    except SimulationLedgerIntegrityError:
        return True, "PORTFOLIO_LEDGER_INTEGRITY_FAILED"
    if portfolio_gate.rules:
        return True, f"PORTFOLIO_RISK_BLOCKED:{','.join(portfolio_gate.rules)}"
    return False, None


def _portfolio_view(db: Session) -> dict[str, Any]:
    snapshot = db.scalar(
        select(DataSnapshot)
        .where(DataSnapshot.dataset_type == "MARKET_BARS")
        .order_by(desc(DataSnapshot.ingested_at))
        .limit(1)
    )
    frame = None
    valuation_error = None
    if snapshot is not None:
        try:
            if not snapshot.license_scope or "UNCLEAR" in snapshot.license_scope.upper():
                raise LicenseGateError("行情快照许可范围不明确。")
            DataMode(snapshot.data_mode)
            authorize_provider(
                db,
                snapshot.provider_code,
                purposes={"display", "cache"},
                at=datetime.now(UTC),
            )
            # A portfolio may span multiple licensed markets or regions.  Recheck
            # every held instrument at display time so a later scope reduction or
            # natural expiry cannot leave a stale valuation visible.
            for instrument_id in portfolio_state(db).positions:
                instrument = db.get(EtfInstrument, instrument_id)
                if instrument is None:
                    raise LicenseGateError("组合持仓缺少标的主数据。")
                authorize_provider(
                    db,
                    snapshot.provider_code,
                    purposes={"display", "cache"},
                    at=datetime.now(UTC),
                    market=instrument.mic,
                    region=instrument.region,
                )
            frame = read_snapshot_with_duckdb(
                snapshot.parquet_uri,
                expected_hash=snapshot.snapshot_hash,
                expected_rows=snapshot.row_count,
            )
        except LicenseGateError:
            valuation_error = "PROVIDER_LICENSE_BLOCKED"
        except SimulationLedgerIntegrityError:
            return _ledger_integrity_blocked_portfolio_view()
        except (OSError, RuntimeError, TypeError, ValueError):
            valuation_error = "SNAPSHOT_UNAVAILABLE"
    try:
        analytics = portfolio_analytics(
            db,
            frame,
            evaluation_time=(
                None
                if snapshot is not None and snapshot.data_mode == DataMode.DEMO_FIXTURE.value
                else datetime.now(UTC)
            ),
            stale_after_minutes=settings.data_stale_after_minutes,
        )
    except SimulationLedgerIntegrityError:
        return _ledger_integrity_blocked_portfolio_view()
    except (KeyError, RuntimeError, TypeError, ValueError):
        try:
            analytics = portfolio_analytics(db, None)
        except SimulationLedgerIntegrityError:
            return _ledger_integrity_blocked_portfolio_view()
        valuation_error = valuation_error or "MARKET_DATA_INVALID"
    if analytics.nav is None:
        valuation_error = valuation_error or "MARKET_DATA_UNAVAILABLE"
    return {
        "cash": analytics.cash,
        "nav": analytics.nav,
        "max_drawdown": analytics.max_drawdown,
        "positions": analytics.positions,
        "position_count": len(analytics.positions),
        "total_fees": analytics.total_fees,
        "holdings": analytics.holdings,
        "valuation_time": analytics.valuation_time.isoformat()
        if analytics.valuation_time
        else None,
        "valuation_error": valuation_error,
    }


def _ledger_integrity_blocked_portfolio_view() -> dict[str, Any]:
    return {
        "cash": None,
        "nav": None,
        "max_drawdown": None,
        "positions": {},
        "position_count": 0,
        "total_fees": None,
        "holdings": [],
        "fills": [],
        "valuation_time": None,
        "valuation_error": "LEDGER_INTEGRITY_BLOCKED",
    }


def _backtest_display_view(db: Session, row: BacktestExperiment | None) -> dict[str, Any] | None:
    if row is None:
        return None
    display_blocked_reason = None
    snapshot = db.get(DataSnapshot, row.snapshot_id)
    try:
        if snapshot is None or snapshot.dataset_type != "MARKET_BARS":
            raise ValueError("回测快照不存在或类型无效。")
        if not verify_backtest_artifact(row, snapshot_hash=snapshot.snapshot_hash):
            display_blocked_reason = "BACKTEST_ARTIFACT_INTEGRITY_BLOCKED"
            raise RuntimeError("回测工件完整性校验失败。")
        if not snapshot.license_scope or "UNCLEAR" in snapshot.license_scope.upper():
            raise LicenseGateError("回测快照许可范围不明确。")
        DataMode(snapshot.data_mode)
        authorize_provider(
            db,
            snapshot.provider_code,
            purposes={"display", "derivative", "cache"},
            at=datetime.now(UTC),
        )
        scopes = set(
            db.execute(
                select(EtfInstrument.mic, EtfInstrument.region).where(
                    EtfInstrument.provider_code == snapshot.provider_code
                )
            ).all()
        )
        for mic, region in scopes:
            authorize_provider(
                db,
                snapshot.provider_code,
                purposes={"display", "derivative", "cache"},
                at=datetime.now(UTC),
                market=mic,
                region=region,
            )
        read_snapshot_with_duckdb(
            snapshot.parquet_uri,
            expected_hash=snapshot.snapshot_hash,
            expected_rows=snapshot.row_count,
        )
    except LicenseGateError:
        display_blocked_reason = "PROVIDER_LICENSE_BLOCKED"
    except (OSError, RuntimeError, TypeError, ValueError):
        display_blocked_reason = display_blocked_reason or "SNAPSHOT_INTEGRITY_BLOCKED"
    if display_blocked_reason:
        return {
            "id": row.id,
            "name": row.name,
            "experiment_key": row.experiment_key,
            "status": (
                "ARTIFACT_INTEGRITY_BLOCKED_HISTORICAL_AUDIT_ONLY"
                if display_blocked_reason == "BACKTEST_ARTIFACT_INTEGRITY_BLOCKED"
                else "LICENSE_BLOCKED_HISTORICAL_AUDIT_ONLY"
            ),
            "metrics": {},
            "baseline_metrics": {},
            "periods": {},
            "leakage_checks": {},
            "config": {},
            "display_blocked_reason": display_blocked_reason,
        }
    display_metrics = dict(row.metrics or {})
    display_metrics.pop(BACKTEST_ARTIFACT_INTEGRITY_KEY, None)
    return {
        "id": row.id,
        "name": row.name,
        "experiment_key": row.experiment_key,
        "status": row.status,
        "metrics": display_metrics,
        "baseline_metrics": row.baseline_metrics,
        "periods": row.periods,
        "leakage_checks": row.leakage_checks,
        "config": row.config,
        "display_blocked_reason": None,
    }


def _model_display_view(db: Session, row: ModelVersion) -> dict[str, Any]:
    display_metrics = dict(row.metrics or {})
    display_metrics.pop(LOGISTIC_ARTIFACT_INTEGRITY_KEY, None)
    base = {
        "id": row.id,
        "name": row.model_name,
        "model_name": row.model_name,
        "version": row.version,
        "status": row.status,
        "feature_version": row.feature_version,
        "metrics": display_metrics,
        "limitations": row.limitations,
        "approved_by": row.approved_by,
        "approved_at": row.approved_at.isoformat() if row.approved_at else None,
        "display_blocked_reason": None,
    }
    if not row.training_snapshot_hash:
        # RuleBasedV1 has no fitted data artifact.  Its governance state remains
        # visible so rejected, rolled-back, or drift-blocked status is explicit.
        return base
    if row.model_name == "LogisticBaselineV1" and not verify_logistic_model_artifact(row):
        return {
            **base,
            "recorded_status": row.status,
            "status": "ARTIFACT_INTEGRITY_BLOCKED_HISTORICAL_AUDIT_ONLY",
            "metrics": {},
            "limitations": [],
            "approved_by": None,
            "approved_at": None,
            "display_blocked_reason": "MODEL_ARTIFACT_INTEGRITY_BLOCKED",
        }
    snapshot = db.scalar(
        select(DataSnapshot).where(DataSnapshot.snapshot_hash == row.training_snapshot_hash)
    )
    try:
        if snapshot is None or snapshot.dataset_type != "MARKET_BARS":
            raise ValueError("模型训练快照不存在或类型无效。")
        if not snapshot.license_scope or "UNCLEAR" in snapshot.license_scope.upper():
            raise LicenseGateError("模型训练快照许可范围不明确。")
        DataMode(snapshot.data_mode)
        authorize_provider(
            db,
            snapshot.provider_code,
            purposes={"display", "derivative", "cache", "training"},
            at=datetime.now(UTC),
        )
        scopes = set(
            db.execute(
                select(EtfInstrument.mic, EtfInstrument.region).where(
                    EtfInstrument.provider_code == snapshot.provider_code
                )
            ).all()
        )
        for mic, region in scopes:
            authorize_provider(
                db,
                snapshot.provider_code,
                purposes={"display", "derivative", "cache", "training"},
                at=datetime.now(UTC),
                market=mic,
                region=region,
            )
        read_snapshot_with_duckdb(
            snapshot.parquet_uri,
            expected_hash=snapshot.snapshot_hash,
            expected_rows=snapshot.row_count,
        )
    except LicenseGateError:
        blocked_reason = "PROVIDER_LICENSE_BLOCKED"
    except (OSError, RuntimeError, TypeError, ValueError):
        blocked_reason = "SNAPSHOT_INTEGRITY_BLOCKED"
    else:
        return base
    return {
        **base,
        "recorded_status": row.status,
        "status": "LICENSE_BLOCKED_HISTORICAL_AUDIT_ONLY",
        "metrics": {},
        "limitations": [],
        "approved_by": None,
        "approved_at": None,
        "display_blocked_reason": blocked_reason,
    }


def _chart_data(signals: list[Signal], backtest: dict[str, Any] | None) -> dict[str, Any]:
    state_counts: dict[str, int] = {}
    for signal in signals:
        state_counts[signal.state] = state_counts.get(signal.state, 0) + 1
    baseline_sharpe: dict[str, float] = {}
    if backtest is not None and not backtest.get("display_blocked_reason"):
        baseline_sharpe = {
            name: float(metrics.get("sharpe", 0.0))
            for name, metrics in (backtest.get("baseline_metrics") or {}).items()
        }
        baseline_sharpe["model_net"] = float((backtest.get("metrics") or {}).get("sharpe", 0.0))
    return {"signal_states": state_counts, "backtest_sharpe": baseline_sharpe}

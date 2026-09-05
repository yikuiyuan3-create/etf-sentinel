from __future__ import annotations

import hashlib
import html
import ipaddress
import json
import re
import socket
import time
from abc import ABC, abstractmethod
from datetime import UTC, date, datetime
from typing import Any, ClassVar
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator
from sqlalchemy import select
from sqlalchemy.orm import Session

from etf_sentinel.enums import DataMode, LicenseStatus
from etf_sentinel.models import ProviderRegistry

FACT_COLUMNS = {
    "provider",
    "source_uri",
    "event_time",
    "published_at",
    "first_seen_at",
    "available_at",
    "as_of",
    "ingested_at",
    "revision",
    "timezone",
    "currency",
    "latency_class",
    "license_scope",
    "data_mode",
    "quality_flags",
    "raw_payload_hash",
}


class ProviderError(RuntimeError):
    pass


class ProviderTimeoutError(ProviderError):
    pass


class ProviderRateLimitError(ProviderError):
    pass


class ProviderSchemaError(ProviderError):
    pass


class ProviderEmptyResponseError(ProviderError):
    pass


class LicenseGateError(PermissionError):
    pass


class IdentifierRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    internal_id: str
    figi: str | None = None
    isin: str | None = None
    mic: str
    currency: str = Field(min_length=3, max_length=3)
    exchange: str
    provider_code: str
    provider_symbol: str


class NewsRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider: str
    source_uri: HttpUrl
    title: str = Field(max_length=500)
    short_summary: str = Field(max_length=1500)
    source_name: str = Field(max_length=160)
    event_time: datetime
    published_at: datetime | None
    first_seen_at: datetime
    available_at: datetime
    as_of: datetime
    ingested_at: datetime
    revision: str
    timezone: str
    currency: str | None = None
    latency_class: str
    license_scope: str
    data_mode: DataMode
    quality_flags: list[str] = Field(default_factory=list)
    raw_payload_hash: str
    cluster_key: str
    entities: list[dict[str, Any]] = Field(default_factory=list)
    exposures: list[dict[str, Any]] = Field(default_factory=list)
    relevance: float = Field(ge=0, le=1)
    direction: float = Field(ge=-1, le=1)
    severity: float = Field(ge=0, le=1)
    novelty: float = Field(ge=0, le=1)
    source_grade: float = Field(ge=0, le=1)

    @field_validator("event_time", "first_seen_at", "available_at", "as_of", "ingested_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("事实时间必须包含时区。")
        return value

    @field_validator("published_at")
    @classmethod
    def optional_published_time(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("发布时间存在时必须包含时区。")
        return value


class MacroRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    series_code: str
    value: float
    event_time: datetime
    published_at: datetime
    first_seen_at: datetime
    available_at: datetime
    as_of: datetime
    ingested_at: datetime
    vintage: str
    provider: str
    source_uri: HttpUrl
    timezone: str
    currency: str | None = None
    latency_class: str
    license_scope: str
    data_mode: DataMode
    quality_flags: list[str] = Field(default_factory=list)
    raw_payload_hash: str

    @field_validator(
        "event_time",
        "published_at",
        "first_seen_at",
        "available_at",
        "as_of",
        "ingested_at",
    )
    @classmethod
    def require_macro_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("宏观事实时间必须包含时区。")
        return value


class CalendarEventRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_code: str
    name_zh: str
    scheduled_at: datetime
    event_time: datetime
    published_at: datetime
    first_announced_at: datetime
    first_seen_at: datetime
    available_at: datetime
    as_of: datetime
    ingested_at: datetime
    revision: str
    timezone: str
    currency: str | None = None
    latency_class: str
    regions: list[str]
    asset_classes: list[str]
    source_uri: HttpUrl
    provider: str
    license_scope: str
    data_mode: DataMode
    quality_flags: list[str] = Field(default_factory=list)
    raw_payload_hash: str = Field(min_length=64, max_length=64)
    status: str = "SCHEDULED"

    @field_validator(
        "scheduled_at",
        "event_time",
        "published_at",
        "first_announced_at",
        "first_seen_at",
        "available_at",
        "as_of",
        "ingested_at",
    )
    @classmethod
    def require_calendar_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("日历事实时间必须包含时区。")
        return value

    @model_validator(mode="after")
    def validate_calendar_causality(self) -> CalendarEventRecord:
        if self.event_time.astimezone(UTC) != self.scheduled_at.astimezone(UTC):
            raise ValueError("event_time 必须等于 scheduled_at。")
        if self.first_announced_at.astimezone(UTC) > self.first_seen_at.astimezone(UTC):
            raise ValueError("first_seen_at 不能早于 first_announced_at。")
        if self.published_at.astimezone(UTC) > self.first_seen_at.astimezone(UTC):
            raise ValueError("first_seen_at 不能早于 published_at。")
        if self.first_seen_at.astimezone(UTC) > self.available_at.astimezone(UTC):
            raise ValueError("available_at 不能早于 first_seen_at。")
        if self.as_of.astimezone(UTC) > self.available_at.astimezone(UTC):
            raise ValueError("available_at 不能早于 as_of。")
        if self.available_at.astimezone(UTC) > self.ingested_at.astimezone(UTC):
            raise ValueError("ingested_at 不能早于 available_at。")
        if self.status not in {"SCHEDULED", "CANCELLED", "POSTPONED"}:
            raise ValueError("事件状态不在允许枚举中。")
        return self


class IdentifierProvider(ABC):
    provider_code: ClassVar[str]

    @abstractmethod
    def resolve_identifier(self, symbol: str, *, mic: str | None = None) -> IdentifierRecord:
        raise NotImplementedError


class MarketDataProvider(ABC):
    provider_code: ClassVar[str]

    @abstractmethod
    def fetch_bars(
        self, identifiers: list[IdentifierRecord], start: date, end: date
    ) -> pd.DataFrame:
        raise NotImplementedError


class EtfDataProvider(ABC):
    provider_code: ClassVar[str]

    @abstractmethod
    def fetch_etf_metadata(self) -> list[dict[str, Any]]:
        raise NotImplementedError

    @abstractmethod
    def fetch_holdings(self, identifier: IdentifierRecord, as_of: date) -> pd.DataFrame:
        raise NotImplementedError


class NewsProvider(ABC):
    provider_code: ClassVar[str]

    @abstractmethod
    def fetch_news(self, query: str, start: datetime, end: datetime) -> list[NewsRecord]:
        raise NotImplementedError


class MacroProvider(ABC):
    provider_code: ClassVar[str]

    @abstractmethod
    def fetch_macro(self, series: list[str], start: date, end: date) -> list[MacroRecord]:
        raise NotImplementedError


class EventCalendarProvider(ABC):
    provider_code: ClassVar[str]

    @abstractmethod
    def fetch_events(self, start: datetime, end: datetime) -> list[CalendarEventRecord]:
        raise NotImplementedError


PURPOSE_TO_RIGHT = {
    "display": "display_right",
    "algorithm": "non_display_algorithm_right",
    "derivative": "derivative_right",
    "cache": "cache_right",
    "training": "training_right",
    "redistribution": "redistribution_right",
}


def ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ProviderSchemaError("时间字段必须包含时区。")
    return value.astimezone(UTC)


def authorize_provider(
    session: Session,
    provider_code: str,
    *,
    purposes: set[str],
    at: datetime | None = None,
    market: str | None = None,
    region: str | None = None,
) -> ProviderRegistry:
    # SessionLocal intentionally disables autoflush.  Flush first so a pending
    # same-transaction revoke/expiry is never overwritten by the forced refresh.
    session.flush()
    registry = session.scalar(
        select(ProviderRegistry)
        .where(ProviderRegistry.provider_code == provider_code)
        .execution_options(populate_existing=True)
    )
    if registry is None:
        raise LicenseGateError(f"数据源 {provider_code} 未登记，已阻断。")
    if registry.review_status != LicenseStatus.APPROVED:
        raise LicenseGateError(
            f"数据源 {provider_code} 审核状态为 {registry.review_status}，不是 APPROVED。"
        )
    check_time = ensure_utc(at or datetime.now(UTC))
    if registry.expires_at is not None:
        expiry = registry.expires_at
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=UTC)
        if expiry.astimezone(UTC) <= check_time:
            raise LicenseGateError(f"数据源 {provider_code} 授权已过期。")
    unknown = purposes - PURPOSE_TO_RIGHT.keys()
    if unknown:
        raise LicenseGateError(f"未知许可用途：{', '.join(sorted(unknown))}")
    denied = [purpose for purpose in purposes if not getattr(registry, PURPOSE_TO_RIGHT[purpose])]
    if denied:
        raise LicenseGateError(
            f"数据源 {provider_code} 未获得用途许可：{', '.join(sorted(denied))}。"
        )
    if market is not None and market not in (registry.markets or []):
        raise LicenseGateError(f"数据源 {provider_code} 未授权市场 {market}。")
    if region is not None and region not in (registry.regions or []):
        raise LicenseGateError(f"数据源 {provider_code} 未授权地域 {region}。")
    return registry


def validate_fact_frame(frame: pd.DataFrame) -> None:
    missing = sorted(FACT_COLUMNS - set(frame.columns))
    if missing:
        raise ProviderSchemaError(f"事实数据缺少字段：{', '.join(missing)}")
    if frame.empty:
        raise ProviderEmptyResponseError("供应商返回空数据。")
    valid_modes = {item.value for item in DataMode}
    invalid_modes = set(frame["data_mode"].dropna().astype(str).unique()) - valid_modes
    if invalid_modes:
        raise ProviderSchemaError(f"无效 data_mode：{', '.join(sorted(invalid_modes))}")
    parsed_times: dict[str, pd.Series] = {}
    for column in [
        "event_time",
        "published_at",
        "first_seen_at",
        "available_at",
        "as_of",
        "ingested_at",
    ]:
        parsed_times[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
        if parsed_times[column].isna().any():
            raise ProviderSchemaError(f"{column} 包含无效或缺失时间。")
    if (parsed_times["available_at"] < parsed_times["event_time"]).any():
        raise ProviderSchemaError("available_at 不得早于 event_time。")
    if (parsed_times["first_seen_at"] < parsed_times["event_time"]).any():
        raise ProviderSchemaError("first_seen_at 不得早于 event_time。")
    if (parsed_times["available_at"] < parsed_times["published_at"]).any():
        raise ProviderSchemaError("available_at 不得早于 published_at。")
    if (parsed_times["available_at"] < parsed_times["first_seen_at"]).any():
        raise ProviderSchemaError("available_at 不得早于 first_seen_at。")
    if (parsed_times["available_at"] < parsed_times["as_of"]).any():
        raise ProviderSchemaError("available_at 不得早于 as_of。")
    if (parsed_times["ingested_at"] < parsed_times["available_at"]).any():
        raise ProviderSchemaError("ingested_at 不得早于 available_at。")
    identity_columns = {"instrument_id", "event_time", "revision"}
    if (
        identity_columns.issubset(frame.columns)
        and frame.duplicated(["instrument_id", "event_time", "revision"]).any()
    ):
        raise ProviderSchemaError("事实数据包含重复标的-时间-版本。")
    price_columns = {"open", "high", "low", "close", "volume"}
    if price_columns.issubset(frame.columns):
        numeric = frame[list(price_columns)].apply(pd.to_numeric, errors="coerce")
        if numeric.isna().any().any() or not np.isfinite(numeric.to_numpy()).all():
            raise ProviderSchemaError("OHLCV 包含缺失、无穷或非数值。")
        if (numeric[["open", "high", "low", "close"]] <= 0).any().any():
            raise ProviderSchemaError("OHLC 必须为正数。")
        if (numeric["volume"] < 0).any():
            raise ProviderSchemaError("成交量不得为负。")
        invalid_range = (
            (numeric["low"] > numeric[["open", "close"]].min(axis=1))
            | (numeric["high"] < numeric[["open", "close"]].max(axis=1))
            | (numeric["low"] > numeric["high"])
        )
        if invalid_range.any():
            raise ProviderSchemaError("OHLC 价格区间关系无效。")


PROMPT_INJECTION_PATTERNS = re.compile(
    r"(?i)(ignore\s+(all|previous)|system\s+prompt|developer\s+message|越过.{0,8}指令|忽略.{0,8}指令)"
)
TAG_PATTERN = re.compile(r"<[^>]+>")
CONTROL_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def sanitize_external_text(value: str, *, max_length: int) -> tuple[str, list[str]]:
    decoded = html.unescape(value)
    plain = TAG_PATTERN.sub(" ", decoded)
    plain = CONTROL_PATTERN.sub("", plain)
    plain = " ".join(plain.split())[:max_length]
    flags: list[str] = []
    if PROMPT_INJECTION_PATTERNS.search(plain):
        flags.append("PROMPT_INJECTION_SUSPECTED")
    return plain, flags


def normalize_url(value: str) -> str:
    parsed = urlsplit(value.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ProviderSchemaError("来源 URL 必须使用 http/https 且包含主机名。")
    query = [
        (key, item) for key, item in parse_qsl(parsed.query) if not key.lower().startswith("utm_")
    ]
    return urlunsplit(
        (parsed.scheme.lower(), parsed.netloc.lower(), parsed.path or "/", urlencode(query), "")
    )


def content_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode()
    ).hexdigest()


class SafeHttpClient:
    """Small fail-closed HTTP client for fixed provider hosts."""

    def __init__(
        self,
        allowed_hosts: set[str],
        timeout_seconds: float = 10.0,
        *,
        min_request_interval_seconds: float = 0.05,
        circuit_failure_threshold: int = 3,
        circuit_cooldown_seconds: float = 30.0,
    ) -> None:
        self.allowed_hosts = {host.lower() for host in allowed_hosts}
        self.timeout = httpx.Timeout(timeout_seconds, connect=min(timeout_seconds, 5.0))
        self.min_request_interval_seconds = max(0.0, min_request_interval_seconds)
        self.circuit_failure_threshold = max(1, circuit_failure_threshold)
        self.circuit_cooldown_seconds = max(1.0, circuit_cooldown_seconds)
        self._last_request_at = 0.0
        self._consecutive_failures = 0
        self._circuit_open_until = 0.0

    def _validate_url(self, url: str) -> None:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or parsed.hostname not in self.allowed_hosts:
            raise ProviderError("外部请求目标不在 HTTPS allowlist。")
        try:
            addresses = socket.getaddrinfo(parsed.hostname, 443, proto=socket.IPPROTO_TCP)
        except socket.gaierror as exc:
            raise ProviderError("供应商主机 DNS 解析失败。") from exc
        for address in addresses:
            ip = ipaddress.ip_address(address[4][0])
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                raise ProviderError("供应商主机解析到非公网地址，SSRF 防护已阻断。")

    def get_json(
        self,
        url: str,
        *,
        params: dict[str, Any],
        authorization: str | None = None,
    ) -> dict[str, Any]:
        self._validate_url(url)
        if time.monotonic() < self._circuit_open_until:
            raise ProviderError("供应商连续失败，熔断器尚未恢复。")
        last_error: Exception | None = None
        for attempt in range(3):
            self._wait_for_rate_limit()
            try:
                with httpx.Client(timeout=self.timeout, follow_redirects=False) as client:
                    headers = {"Accept": "application/json"}
                    if authorization is not None:
                        headers["Authorization"] = authorization
                    response = client.get(
                        url,
                        params=params,
                        headers=headers,
                    )
                if response.status_code == 429:
                    raise ProviderRateLimitError("供应商限流（HTTP 429）。")
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise ProviderSchemaError("供应商 JSON 顶层必须是对象。")
                self._consecutive_failures = 0
                return payload
            except httpx.TimeoutException:
                last_error = ProviderTimeoutError("供应商请求超时。")
            except ProviderRateLimitError as exc:
                last_error = exc
            except (httpx.HTTPError, ValueError):
                self._record_failure()
                # httpx exceptions can contain the full request URL.  Never keep
                # their cause chain because some providers accept secrets in query
                # parameters and traceback formatters would reproduce that URL.
                raise ProviderError("供应商请求或 JSON 解析失败。") from None
            if attempt == 2:
                break
            time.sleep(0.05 * (2**attempt))
        assert last_error is not None
        self._record_failure()
        raise last_error

    def _wait_for_rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        remaining = self.min_request_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)
        self._last_request_at = time.monotonic()

    def _record_failure(self) -> None:
        self._consecutive_failures += 1
        if self._consecutive_failures >= self.circuit_failure_threshold:
            self._circuit_open_until = time.monotonic() + self.circuit_cooldown_seconds

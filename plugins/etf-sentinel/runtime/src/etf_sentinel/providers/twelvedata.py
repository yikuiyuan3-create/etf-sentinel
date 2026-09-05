from __future__ import annotations

import uuid
from datetime import UTC, date, datetime
from typing import Any

import pandas as pd

from etf_sentinel.enums import DataMode
from etf_sentinel.providers.base import (
    EtfDataProvider,
    IdentifierProvider,
    IdentifierRecord,
    MarketDataProvider,
    ProviderEmptyResponseError,
    ProviderSchemaError,
    SafeHttpClient,
    content_hash,
    validate_fact_frame,
)


class TwelveDataProvider(IdentifierProvider, MarketDataProvider, EtfDataProvider):
    """Twelve Data adapter. It remains blocked until API key and registry rights are approved."""

    provider_code = "twelve_data"
    base_url = "https://api.twelvedata.com"
    identifier_namespace = uuid.UUID("9322bacf-4361-4d18-861c-3a87b53536bc")

    def __init__(self, api_key: str, client: SafeHttpClient | None = None) -> None:
        if not api_key:
            raise ValueError("Twelve Data API key 必须来自环境变量。")
        self._api_key = api_key
        self.client = client or SafeHttpClient({"api.twelvedata.com"})

    def resolve_identifier(self, symbol: str, *, mic: str | None = None) -> IdentifierRecord:
        payload = self.client.get_json(
            f"{self.base_url}/symbol_search",
            params={"symbol": symbol, "outputsize": 10},
            authorization=f"apikey {self._api_key}",
        )
        candidates = payload.get("data")
        if not isinstance(candidates, list):
            raise ProviderSchemaError("Twelve Data symbol_search 响应结构变化。")
        chosen = next(
            (
                row
                for row in candidates
                if isinstance(row, dict)
                and row.get("symbol") == symbol
                and (mic is None or row.get("mic_code") == mic)
            ),
            None,
        )
        if chosen is None:
            raise ProviderEmptyResponseError("Twelve Data 未返回匹配标识。")
        provider_symbol = str(chosen["symbol"])
        exchange = str(chosen.get("exchange", "UNKNOWN"))
        mic_code = str(chosen.get("mic_code") or mic or "XUNK")
        currency = str(chosen.get("currency") or "USD")[:3]
        internal_id = str(uuid.uuid5(self.identifier_namespace, f"{mic_code}:{provider_symbol}"))
        return IdentifierRecord(
            internal_id=internal_id,
            figi=None,
            isin=None,
            mic=mic_code,
            currency=currency,
            exchange=exchange,
            provider_code=self.provider_code,
            provider_symbol=provider_symbol,
        )

    def fetch_bars(
        self, identifiers: list[IdentifierRecord], start: date, end: date
    ) -> pd.DataFrame:
        if len(identifiers) != 1:
            raise ValueError("MVP 适配器每次仅请求一个标识，以便明确限流和审计。")
        identifier = identifiers[0]
        payload = self.client.get_json(
            f"{self.base_url}/time_series",
            params={
                "symbol": identifier.provider_symbol,
                "interval": "1day",
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "timezone": "UTC",
                "order": "ASC",
                "outputsize": 5000,
                "adjust": "none",
            },
            authorization=f"apikey {self._api_key}",
        )
        values = payload.get("values")
        if not isinstance(values, list):
            raise ProviderSchemaError("Twelve Data time_series 响应缺少 values 数组。")
        if not values:
            raise ProviderEmptyResponseError("Twelve Data 返回空行情。")
        now = datetime.now(UTC)
        rows: list[dict[str, Any]] = []
        for value in values:
            if not isinstance(value, dict):
                raise ProviderSchemaError("Twelve Data bar 不是对象。")
            required = {"datetime", "open", "high", "low", "close", "volume"}
            if not required.issubset(value):
                raise ProviderSchemaError("Twelve Data bar 字段发生变化。")
            event_time = pd.Timestamp(value["datetime"], tz="UTC").to_pydatetime()
            rows.append(
                {
                    "instrument_id": identifier.internal_id,
                    "provider_symbol": identifier.provider_symbol,
                    "provider": self.provider_code,
                    "source_uri": f"{self.base_url}/time_series",
                    "event_time": event_time,
                    "published_at": now,
                    "first_seen_at": now,
                    "available_at": now,
                    "as_of": event_time,
                    "ingested_at": now,
                    "revision": "provider-current",
                    "timezone": "UTC",
                    "currency": identifier.currency,
                    "latency_class": "PLAN_DEPENDENT",
                    "license_scope": "REGISTRY_APPROVAL_REQUIRED",
                    "data_mode": DataMode.DELAYED.value,
                    "quality_flags": ["CORPORATE_ACTION_ADJUSTMENT_NOT_VERIFIED"],
                    "raw_payload_hash": content_hash(value),
                    "open": float(value["open"]),
                    "high": float(value["high"]),
                    "low": float(value["low"]),
                    "close": float(value["close"]),
                    "adjusted_close": float(value["close"]),
                    "total_return_close": float(value["close"]),
                    "volume": float(value["volume"]),
                    "bid_ask_spread_bps": float("nan"),
                    "fx_rate": 1.0,
                    "dividend": 0.0,
                    "split_factor": 1.0,
                    "expense_ratio": float("nan"),
                    "nav_premium_bps": float("nan"),
                    "tracking_error": float("nan"),
                }
            )
        frame = pd.DataFrame(rows)
        validate_fact_frame(frame)
        return frame

    def fetch_etf_metadata(self) -> list[dict[str, Any]]:
        raise PermissionError("ETF 元数据端点及使用权尚未完成内部许可审核。")

    def fetch_holdings(self, identifier: IdentifierRecord, as_of: date) -> pd.DataFrame:
        del identifier, as_of
        raise PermissionError("ETF 历史持仓许可和点时快照能力尚未确认。")

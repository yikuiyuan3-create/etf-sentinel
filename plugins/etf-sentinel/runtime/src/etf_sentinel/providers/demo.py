from __future__ import annotations

import hashlib
import math
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pydantic import HttpUrl

from etf_sentinel.enums import DataMode
from etf_sentinel.providers.base import (
    CalendarEventRecord,
    EtfDataProvider,
    EventCalendarProvider,
    IdentifierProvider,
    IdentifierRecord,
    MacroProvider,
    MacroRecord,
    MarketDataProvider,
    NewsProvider,
    NewsRecord,
    content_hash,
    validate_fact_frame,
)

DEMO_NAMESPACE = uuid.UUID("48cb08c2-1c42-4efd-9d91-3aec99b510fb")
DEMO_END_DATE = date(2025, 1, 31)
DEMO_TIMEZONE = ZoneInfo("Asia/Shanghai")


class DemoFixtureProvider(
    IdentifierProvider,
    MarketDataProvider,
    EtfDataProvider,
    NewsProvider,
    MacroProvider,
    EventCalendarProvider,
):
    provider_code = "demo_fixture"

    ASSET_SPECS = [
        ("股票", "宽基", "中国", 24),
        ("债券", "利率债", "中国", 12),
        ("商品", "商品", "全球", 8),
        ("地区", "区域市场", "亚太", 8),
        ("行业", "行业主题", "中国", 8),
    ]

    def fetch_etf_metadata(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        number = 1
        for asset_class, industry, region, count in self.ASSET_SPECS:
            for _ in range(count):
                symbol = f"DEMO-{number:03d}"
                internal_id = str(uuid.uuid5(DEMO_NAMESPACE, symbol))
                rows.append(
                    {
                        "id": internal_id,
                        "name_zh": f"演示{asset_class}ETF {number:03d}",
                        "figi": f"DEMOFIGI{number:012d}"[-20:],
                        "isin": f"DM{number:010d}",
                        "mic": "XDEM",
                        "currency": "CNY",
                        "exchange": "演示交易所（非真实）",
                        "provider_code": self.provider_code,
                        "provider_symbol": symbol,
                        "asset_class": asset_class,
                        "industry": industry
                        if asset_class != "行业"
                        else f"行业{(number % 6) + 1}",
                        "region": region,
                        "leveraged": False,
                        "inverse": False,
                        "active": True,
                        "inception_date": date(2018, 1, 2),
                        "termination_date": None,
                        "liquidity_tier": 1 if number <= 45 else 2,
                        "metadata_json": {
                            "fixture": True,
                            "description": "固定测试标的，不对应任何真实基金",
                        },
                    }
                )
                number += 1
        return rows

    def resolve_identifier(self, symbol: str, *, mic: str | None = None) -> IdentifierRecord:
        for row in self.fetch_etf_metadata():
            if row["provider_symbol"] == symbol and (mic is None or row["mic"] == mic):
                return IdentifierRecord(
                    internal_id=row["id"],
                    figi=row["figi"],
                    isin=row["isin"],
                    mic=row["mic"],
                    currency=row["currency"],
                    exchange=row["exchange"],
                    provider_code=self.provider_code,
                    provider_symbol=row["provider_symbol"],
                )
        raise KeyError(f"演示标识不存在：{symbol}")

    def fetch_bars(
        self, identifiers: list[IdentifierRecord], start: date, end: date
    ) -> pd.DataFrame:
        dates = pd.bdate_range(start=start, end=end, tz=DEMO_TIMEZONE)
        rows: list[dict[str, Any]] = []
        metadata_by_id = {row["id"]: row for row in self.fetch_etf_metadata()}
        ingested_at = datetime(2025, 2, 1, 0, 0, tzinfo=UTC)
        for item in identifiers:
            spec = metadata_by_id[item.internal_id]
            ordinal = int(item.provider_symbol.split("-")[-1])
            rng = np.random.default_rng(20_250_131 + ordinal)
            base_drift = {
                "股票": 0.00025,
                "债券": 0.00010,
                "商品": 0.00012,
                "地区": 0.00018,
                "行业": 0.00022,
            }[spec["asset_class"]]
            sigma = {
                "股票": 0.012,
                "债券": 0.0035,
                "商品": 0.014,
                "地区": 0.010,
                "行业": 0.015,
            }[spec["asset_class"]]
            shocks = rng.normal(base_drift, sigma, len(dates))
            cycle = np.sin(np.arange(len(dates)) / (25 + ordinal % 9)) * 0.0015
            total_returns = shocks + cycle
            total_return_close = 100 * np.exp(np.cumsum(total_returns))
            split_factor = np.ones(len(dates))
            dividend = np.zeros(len(dates))
            total_return_open = total_return_close * (1 + rng.normal(0, sigma / 5, len(dates)))
            raw_close = np.empty(len(dates), dtype=float)
            open_price = np.empty(len(dates), dtype=float)
            raw_close[0] = total_return_close[0]
            open_price[0] = total_return_open[0]
            for idx in range(1, len(dates)):
                if len(dates) > 170 and idx == 160:
                    dividend[idx] = round(float(raw_close[idx - 1]) * 0.006, 4)
                if ordinal == 1 and len(dates) > 220 and idx == 210:
                    split_factor[idx] = 2.0
                raw_close[idx] = (
                    raw_close[idx - 1] * (total_return_close[idx] / total_return_close[idx - 1])
                    - dividend[idx]
                ) / split_factor[idx]
                open_price[idx] = (
                    open_price[idx - 1] * (total_return_open[idx] / total_return_open[idx - 1])
                    - dividend[idx]
                ) / split_factor[idx]
            # Split-adjusted price excludes distributions, while total return
            # explicitly reinvests them.  Execution always uses the raw series.
            adjusted_close = raw_close * np.cumprod(split_factor)
            high = np.maximum(open_price, raw_close) * (
                1 + np.abs(rng.normal(0, sigma / 4, len(dates)))
            )
            low = np.minimum(open_price, raw_close) * (
                1 - np.abs(rng.normal(0, sigma / 4, len(dates)))
            )
            base_volume = 5_000_000 + ordinal * 37_000
            volume = np.maximum(50_000, rng.normal(base_volume, base_volume * 0.15, len(dates)))
            spread = np.clip(2.5 + ordinal / 40 + rng.normal(0, 0.35, len(dates)), 1.0, 12.0)
            for idx, trading_day in enumerate(dates):
                local_close = trading_day.replace(hour=15, minute=0, second=0)
                event_time = local_close.tz_convert(UTC).to_pydatetime()
                available_at = event_time + timedelta(minutes=10)
                payload = {
                    "symbol": item.provider_symbol,
                    "date": trading_day.date().isoformat(),
                    "open": round(float(open_price[idx]), 6),
                    "high": round(float(high[idx]), 6),
                    "low": round(float(low[idx]), 6),
                    "close": round(float(raw_close[idx]), 6),
                    "adjusted_close": round(float(adjusted_close[idx]), 6),
                    "total_return_close": round(float(total_return_close[idx]), 6),
                    "volume": round(float(volume[idx]), 2),
                    "dividend": float(dividend[idx]),
                    "split_factor": float(split_factor[idx]),
                    "fx_rate": 1.0,
                }
                rows.append(
                    {
                        "instrument_id": item.internal_id,
                        "provider_symbol": item.provider_symbol,
                        "asset_class": spec["asset_class"],
                        "industry": spec["industry"],
                        "region": spec["region"],
                        "provider": self.provider_code,
                        "source_uri": f"https://example.invalid/etf-sentinel/fixtures/{item.provider_symbol}",
                        "event_time": event_time,
                        "published_at": available_at,
                        "first_seen_at": available_at,
                        "available_at": available_at,
                        "as_of": event_time,
                        "ingested_at": ingested_at,
                        "revision": "fixture-v1",
                        "timezone": "Asia/Shanghai",
                        "currency": "CNY",
                        "latency_class": "FIXED_FIXTURE",
                        "license_scope": "INTERNAL_TEST_AND_DEMONSTRATION_ONLY",
                        "data_mode": DataMode.DEMO_FIXTURE.value,
                        "quality_flags": [],
                        "raw_payload_hash": content_hash(payload),
                        "open": payload["open"],
                        "high": payload["high"],
                        "low": payload["low"],
                        "close": payload["close"],
                        "adjusted_close": payload["adjusted_close"],
                        "total_return_close": payload["total_return_close"],
                        "volume": payload["volume"],
                        "bid_ask_spread_bps": round(float(spread[idx]), 4),
                        "fx_rate": payload["fx_rate"],
                        "dividend": payload["dividend"],
                        "split_factor": payload["split_factor"],
                        "expense_ratio": round(0.0015 + ordinal * 0.000015, 6),
                        "nav_premium_bps": round(float(rng.normal(0, 2.0)), 4),
                        "tracking_error": round(0.002 + ordinal * 0.00002, 6),
                    }
                )
        frame = pd.DataFrame(rows).sort_values(["instrument_id", "event_time"])
        validate_fact_frame(frame)
        return frame.reset_index(drop=True)

    def fetch_holdings(self, identifier: IdentifierRecord, as_of: date) -> pd.DataFrame:
        metadata = next(
            row for row in self.fetch_etf_metadata() if row["id"] == identifier.internal_id
        )
        return pd.DataFrame(
            [
                {
                    "instrument_id": identifier.internal_id,
                    "holding_id": f"DEMO-HOLDING-{index}",
                    "weight": weight,
                    "industry": metadata["industry"],
                    "region": metadata["region"],
                    "as_of": as_of,
                    "available_at": datetime.combine(as_of, datetime.min.time(), UTC)
                    + timedelta(days=1),
                    "data_mode": DataMode.DEMO_FIXTURE.value,
                }
                for index, weight in enumerate([0.35, 0.25, 0.20, 0.12, 0.08], start=1)
            ]
        )

    def fetch_news(self, query: str, start: datetime, end: datetime) -> list[NewsRecord]:
        del query
        fixture_time = datetime(2025, 1, 30, 2, 0, tzinfo=UTC)
        if fixture_time < start.astimezone(UTC) or fixture_time > end.astimezone(UTC):
            return []
        themes = [
            ("宏观流动性进入观察窗口", "利率", "债券", 0.72, -0.20, 0.55),
            ("区域市场波动率有所抬升", "地区", "地区", 0.76, -0.55, 0.68),
            ("商品库存数据出现阶段变化", "商品", "商品", 0.66, 0.25, 0.48),
            ("行业景气指标分化", "行业", "行业", 0.70, -0.15, 0.44),
            ("宽基市场成交活跃度变化", "市场", "股票", 0.62, 0.18, 0.35),
        ]
        output: list[NewsRecord] = []
        for index, (title, entity, exposure, relevance, direction, severity) in enumerate(themes):
            published = fixture_time - timedelta(hours=index * 3)
            uri = f"https://example.invalid/etf-sentinel/news/demo-{index + 1}"
            payload = {"title": title, "published": published.isoformat(), "uri": uri}
            output.append(
                NewsRecord(
                    provider=self.provider_code,
                    source_uri=HttpUrl(uri),
                    title=f"【固定演示】{title}",
                    short_summary="仅用于验证新闻去重、暴露映射和风险预警，不代表真实新闻。",
                    source_name="ETF Sentinel Demo Fixture",
                    event_time=published,
                    published_at=published,
                    first_seen_at=published + timedelta(minutes=2),
                    available_at=published + timedelta(minutes=2),
                    as_of=published,
                    ingested_at=datetime(2025, 2, 1, tzinfo=UTC),
                    revision="fixture-v1",
                    timezone="UTC",
                    latency_class="FIXED_FIXTURE",
                    license_scope="INTERNAL_TEST_AND_DEMONSTRATION_ONLY",
                    data_mode=DataMode.DEMO_FIXTURE,
                    quality_flags=[],
                    raw_payload_hash=content_hash(payload),
                    cluster_key=f"demo-cluster-{index + 1}",
                    entities=[{"type": "THEME", "name": entity, "confidence": 0.95}],
                    exposures=[
                        {
                            "type": "ASSET_CLASS",
                            "value": exposure,
                            "reason": "固定演示暴露映射",
                            "confidence": 0.95,
                        }
                    ],
                    relevance=relevance,
                    direction=direction,
                    severity=severity,
                    novelty=0.75,
                    source_grade=0.70,
                )
            )
        return output

    def fetch_macro(self, series: list[str], start: date, end: date) -> list[MacroRecord]:
        fixture_date = date(2025, 1, 29)
        if not (start <= fixture_date <= end):
            return []
        output: list[MacroRecord] = []
        for index, code in enumerate(series):
            event_time = datetime(2025, 1, 29, 2, 0, tzinfo=UTC)
            published_at = event_time + timedelta(hours=1)
            first_seen_at = event_time + timedelta(hours=1, minutes=2)
            value = round(math.sin(index + 1) * 0.25, 4)
            source_uri = f"https://example.invalid/etf-sentinel/macro/{code}"
            payload = {
                "series_code": code,
                "value": value,
                "event_time": event_time.isoformat(),
                "vintage": "fixture-v1",
            }
            output.append(
                MacroRecord(
                    series_code=code,
                    value=value,
                    event_time=event_time,
                    published_at=published_at,
                    first_seen_at=first_seen_at,
                    available_at=first_seen_at,
                    as_of=event_time,
                    ingested_at=datetime(2025, 2, 1, 0, 0, tzinfo=UTC),
                    vintage="fixture-v1",
                    provider=self.provider_code,
                    source_uri=HttpUrl(source_uri),
                    timezone="UTC",
                    currency=None,
                    latency_class="FIXED_FIXTURE",
                    license_scope="INTERNAL_TEST_AND_DEMONSTRATION_ONLY",
                    data_mode=DataMode.DEMO_FIXTURE,
                    quality_flags=[],
                    raw_payload_hash=content_hash(payload),
                )
            )
        return output

    def fetch_events(self, start: datetime, end: datetime) -> list[CalendarEventRecord]:
        scheduled = datetime(2025, 1, 31, 1, 30, tzinfo=UTC)
        if not (start.astimezone(UTC) <= scheduled <= end.astimezone(UTC)):
            return []
        published_at = datetime(2025, 1, 1, tzinfo=UTC)
        first_seen_at = published_at + timedelta(minutes=2)
        source_uri = "https://example.invalid/etf-sentinel/calendar/demo-1"
        raw_payload_hash = content_hash(
            {
                "event_code": "DEMO-MACRO-001",
                "scheduled_at": scheduled.isoformat(),
                "published_at": published_at.isoformat(),
                "source_uri": source_uri,
                "revision": "fixture-v1",
                "status": "SCHEDULED",
            }
        )
        return [
            CalendarEventRecord(
                event_code="DEMO-MACRO-001",
                name_zh="固定演示宏观数据发布窗口",
                scheduled_at=scheduled,
                event_time=scheduled,
                published_at=published_at,
                first_announced_at=published_at,
                first_seen_at=first_seen_at,
                available_at=first_seen_at,
                as_of=published_at,
                ingested_at=first_seen_at,
                revision="fixture-v1",
                timezone="UTC",
                currency=None,
                latency_class="FIXED_FIXTURE",
                regions=["中国"],
                asset_classes=["股票", "债券"],
                source_uri=HttpUrl(source_uri),
                provider=self.provider_code,
                license_scope="INTERNAL_TEST_AND_DEMONSTRATION_ONLY",
                data_mode=DataMode.DEMO_FIXTURE,
                quality_flags=[],
                raw_payload_hash=raw_payload_hash,
                status="SCHEDULED",
            )
        ]


def default_demo_identifiers(provider: DemoFixtureProvider) -> list[IdentifierRecord]:
    return [
        provider.resolve_identifier(row["provider_symbol"]) for row in provider.fetch_etf_metadata()
    ]


def dataframe_hash(frame: pd.DataFrame) -> str:
    normalized = frame.sort_values(["instrument_id", "event_time"]).reset_index(drop=True)
    digest = pd.util.hash_pandas_object(normalized.astype(str), index=True).values.tobytes()
    return hashlib.sha256(digest).hexdigest()

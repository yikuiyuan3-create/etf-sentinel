from __future__ import annotations

from datetime import date

import pandas as pd

from etf_sentinel.providers.base import IdentifierRecord, MarketDataProvider


class LicensePendingMarketDataProvider(MarketDataProvider):
    """Explicitly disabled skeleton for CN/HK or other commercial market data."""

    def __init__(self, provider_code: str, reason: str) -> None:
        self.__dict__["provider_code"] = provider_code
        self.reason = reason

    def fetch_bars(
        self, identifiers: list[IdentifierRecord], start: date, end: date
    ) -> pd.DataFrame:
        del identifiers, start, end
        raise PermissionError(f"{self.provider_code} 已禁用：{self.reason}")

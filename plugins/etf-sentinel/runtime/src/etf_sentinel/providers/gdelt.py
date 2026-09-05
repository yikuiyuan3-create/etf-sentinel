from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import HttpUrl

from etf_sentinel.enums import DataMode
from etf_sentinel.providers.base import (
    NewsProvider,
    NewsRecord,
    ProviderEmptyResponseError,
    ProviderSchemaError,
    SafeHttpClient,
    content_hash,
    normalize_url,
    sanitize_external_text,
)


class GDELTNewsProvider(NewsProvider):
    """Metadata-only GDELT DOC 2.0 adapter; registry approval is enforced upstream."""

    provider_code = "gdelt_doc_v2"
    endpoint = "https://api.gdeltproject.org/api/v2/doc/doc"

    def __init__(self, client: SafeHttpClient | None = None) -> None:
        self.client = client or SafeHttpClient({"api.gdeltproject.org"})

    def fetch_news(self, query: str, start: datetime, end: datetime) -> list[NewsRecord]:
        payload = self.client.get_json(
            self.endpoint,
            params={
                "query": query,
                "mode": "artlist",
                "format": "json",
                "maxrecords": 100,
                "startdatetime": start.strftime("%Y%m%d%H%M%S"),
                "enddatetime": end.strftime("%Y%m%d%H%M%S"),
                "sort": "datedesc",
            },
        )
        articles = payload.get("articles")
        if not isinstance(articles, list):
            raise ProviderSchemaError("GDELT 响应缺少 articles 数组。")
        if not articles:
            raise ProviderEmptyResponseError("GDELT 返回空文章列表。")
        records: list[NewsRecord] = []
        for article in articles:
            if not isinstance(article, dict) or not article.get("url") or not article.get("title"):
                continue
            normalized_url = normalize_url(str(article["url"]))
            title, flags = sanitize_external_text(str(article["title"]), max_length=500)
            source_name, source_flags = sanitize_external_text(
                str(article.get("domain", "unknown")), max_length=160
            )
            seen = _parse_gdelt_time(article.get("seendate"))
            if seen is None:
                continue
            raw_hash = content_hash(
                {"url": normalized_url, "title": title, "seendate": seen.isoformat()}
            )
            records.append(
                NewsRecord(
                    provider=self.provider_code,
                    source_uri=HttpUrl(normalized_url),
                    title=title,
                    short_summary="GDELT 仅提供事件发现元数据；未保存版权全文。",
                    source_name=source_name,
                    event_time=seen,
                    published_at=None,
                    first_seen_at=seen,
                    available_at=seen,
                    as_of=seen,
                    ingested_at=datetime.now(seen.tzinfo),
                    revision="gdelt-doc-v2",
                    timezone="UTC",
                    currency=None,
                    latency_class="PUBLIC_METADATA_BEST_EFFORT",
                    license_scope="PENDING_INTERNAL_LEGAL_REVIEW",
                    data_mode=DataMode.DELAYED,
                    quality_flags=flags
                    + source_flags
                    + ["ENTITY_MAPPING_REQUIRED", "PUBLISHED_AT_UNAVAILABLE"],
                    raw_payload_hash=raw_hash,
                    cluster_key=content_hash({"title": title.lower()})[:24],
                    entities=[],
                    exposures=[],
                    relevance=0.0,
                    direction=0.0,
                    severity=0.0,
                    novelty=0.0,
                    source_grade=0.0,
                )
            )
        if not records:
            raise ProviderEmptyResponseError("GDELT 响应没有通过 Schema/时间校验的记录。")
        return records


def _parse_gdelt_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    for pattern in ("%Y%m%dT%H%M%SZ", "%Y%m%d%H%M%S"):
        try:
            parsed = datetime.strptime(value, pattern)
            return parsed.replace(tzinfo=UTC)
        except ValueError:
            continue
    return None

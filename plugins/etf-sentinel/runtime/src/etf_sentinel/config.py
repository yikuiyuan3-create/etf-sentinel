from __future__ import annotations

from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, HttpUrl, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class UnsafeConfigurationError(ValueError):
    """Raised when configuration crosses a phase-one safety boundary."""


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_env: Literal["demo", "test", "production"] = "demo"
    app_host: str = "127.0.0.1"
    app_port: int = Field(default=8000, ge=1, le=65535)
    trading_mode: str = "paper"
    database_url: str = "sqlite:///./var/etf_sentinel.db"
    redis_url: str = "redis://127.0.0.1:6379/0"
    snapshot_dir: Path = Path("var/snapshots")
    export_dir: Path = Path("var/exports")
    market_data_provider: str = "demo_fixture"
    demo_evaluation_time: datetime = datetime.fromisoformat("2025-01-30T08:00:00+00:00")
    data_stale_after_minutes: int = Field(default=2880, ge=1)
    global_kill_switch: bool = False
    monitoring_interval_hours: int = Field(default=1, ge=1, le=2)

    enable_public_service: bool = False
    enable_paid_subscriptions: bool = False
    enable_purchase_redirect: bool = False
    enable_third_party_funds: bool = False
    enable_live_trading: bool = False
    enable_order_entry: bool = False

    twelve_data_enabled: bool = False
    twelve_data_api_key: str | None = None
    gdelt_enabled: bool = False
    email_alerts_enabled: bool = False
    webhook_alerts_enabled: bool = False
    alert_webhook_url: HttpUrl | None = None
    auth_enabled: bool = False
    container_localhost_bound: bool = False
    log_level: str = "INFO"

    single_etf_cap: float = Field(default=0.10, gt=0, le=1)
    asset_class_cap: float = Field(default=0.45, gt=0, le=1)
    industry_cap: float = Field(default=0.30, gt=0, le=1)
    region_cap: float = Field(default=0.60, gt=0, le=1)
    portfolio_volatility_target: float = Field(default=0.12, gt=0, le=1)
    cash_floor: float = Field(default=0.20, ge=0, lt=1)
    max_turnover: float = Field(default=0.25, gt=0, le=1)
    max_simulation_loss: float = Field(default=0.10, gt=0, le=1)
    max_drawdown: float = Field(default=0.15, gt=0, le=1)
    news_risk_reduction_threshold: float = Field(default=0.70, ge=0, le=1)
    news_factor_weight_cap: float = Field(default=0.15, ge=0, le=0.25)
    alert_cooldown_minutes: int = Field(default=240, ge=0)
    alert_daily_limit: int = Field(default=50, ge=1)
    quiet_hours_start: int = Field(default=22, ge=0, le=23)
    quiet_hours_end: int = Field(default=7, ge=0, le=23)

    @field_validator("trading_mode")
    @classmethod
    def only_paper_trading(cls, value: str) -> str:
        if value.strip().lower() != "paper":
            raise UnsafeConfigurationError("TRADING_MODE 只能为 paper；系统拒绝启动。")
        return "paper"

    @field_validator("twelve_data_api_key")
    @classmethod
    def normalize_empty_secret(cls, value: str | None) -> str | None:
        if value is None or not value.strip():
            return None
        return value.strip()

    @field_validator("alert_webhook_url", mode="before")
    @classmethod
    def normalize_empty_webhook_url(cls, value: object) -> object:
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @model_validator(mode="after")
    def enforce_phase_one_boundaries(self) -> Settings:
        forbidden = {
            "ENABLE_PUBLIC_SERVICE": self.enable_public_service,
            "ENABLE_PAID_SUBSCRIPTIONS": self.enable_paid_subscriptions,
            "ENABLE_PURCHASE_REDIRECT": self.enable_purchase_redirect,
            "ENABLE_THIRD_PARTY_FUNDS": self.enable_third_party_funds,
            "ENABLE_LIVE_TRADING": self.enable_live_trading,
            "ENABLE_ORDER_ENTRY": self.enable_order_entry,
        }
        enabled = [name for name, active in forbidden.items() if active]
        if enabled:
            raise UnsafeConfigurationError(
                f"第一阶段禁止启用以下功能：{', '.join(sorted(enabled))}"
            )
        if self.market_data_provider != "demo_fixture":
            raise UnsafeConfigurationError(
                "真实行情流水线尚未通过数据许可与生产验收；已拒绝启动，不会静默回退到 Demo。"
            )
        if self.twelve_data_enabled or self.gdelt_enabled:
            raise UnsafeConfigurationError(
                "Twelve Data/GDELT 适配器在第一阶段仅供受控验证；"
                "provider_registry 人工审批与真实数据流水线未完成前拒绝启用。"
            )
        if self.email_alerts_enabled or self.webhook_alerts_enabled:
            raise UnsafeConfigurationError(
                "第一阶段只实现站内预警；外部通知鉴权、allowlist 和投递审计"
                "未验收前，拒绝启用邮件或 Webhook。"
            )
        if self.app_host not in {"127.0.0.1", "localhost"}:
            controlled_container = (
                self.app_env == "demo"
                and self.container_localhost_bound
                and Path("/.dockerenv").is_file()
            )
            if not controlled_container:
                raise UnsafeConfigurationError(
                    "第一阶段尚未实现可验证的认证与 RBAC，拒绝远程监听（非 localhost）。"
                )
        return self


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    settings.snapshot_dir.mkdir(parents=True, exist_ok=True)
    settings.export_dir.mkdir(parents=True, exist_ok=True)
    return settings

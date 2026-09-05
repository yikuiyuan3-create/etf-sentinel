from __future__ import annotations

import os
import subprocess
import sys

import pytest
from pydantic import ValidationError

from etf_sentinel.config import Settings, UnsafeConfigurationError


@pytest.mark.parametrize("unsafe_mode", ["live", "LIVE", " live ", "real", "auto"])
def test_trading_mode_other_than_paper_is_rejected(unsafe_mode: str) -> None:
    with pytest.raises((UnsafeConfigurationError, ValidationError), match="只能为 paper"):
        Settings(_env_file=None, trading_mode=unsafe_mode)


@pytest.mark.parametrize(
    "flag",
    [
        "enable_public_service",
        "enable_paid_subscriptions",
        "enable_purchase_redirect",
        "enable_third_party_funds",
        "enable_live_trading",
        "enable_order_entry",
    ],
)
def test_forbidden_phase_one_feature_flag_refuses_configuration(flag: str) -> None:
    with pytest.raises((UnsafeConfigurationError, ValidationError), match="第一阶段禁止启用"):
        Settings(_env_file=None, **{flag: True})


def test_remote_production_listener_requires_authentication() -> None:
    with pytest.raises(
        (UnsafeConfigurationError, ValidationError), match="拒绝.*(?:远程监听|非 localhost)"
    ):
        Settings(
            _env_file=None,
            app_env="production",
            app_host="0.0.0.0",  # noqa: S104 - intentional negative security-boundary test
            auth_enabled=False,
        )


def test_authentication_flag_cannot_bypass_unimplemented_remote_rbac_gate() -> None:
    with pytest.raises(
        (UnsafeConfigurationError, ValidationError), match="拒绝.*(?:远程监听|非 localhost)"
    ):
        Settings(
            _env_file=None,
            app_env="production",
            app_host="0.0.0.0",  # noqa: S104 - intentional negative boundary test
            auth_enabled=True,
        )


def test_non_demo_market_provider_is_rejected_without_silent_fallback() -> None:
    with pytest.raises((UnsafeConfigurationError, ValidationError), match="真实行情.*拒绝启动"):
        Settings(_env_file=None, market_data_provider="twelve_data")


@pytest.mark.parametrize("flag", ["twelve_data_enabled", "gdelt_enabled"])
def test_real_provider_adapters_cannot_be_enabled_in_phase_one(flag: str) -> None:
    with pytest.raises((UnsafeConfigurationError, ValidationError), match="适配器.*拒绝启用"):
        Settings(_env_file=None, **{flag: True})


def test_importing_application_with_live_mode_fails_before_serving() -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = "src"
    environment["TRADING_MODE"] = "live"
    result = subprocess.run(
        [sys.executable, "-c", "import etf_sentinel.main"],
        cwd=os.getcwd(),
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )

    assert result.returncode != 0
    assert "TRADING_MODE" in result.stderr or "只能为 paper" in result.stderr


def test_paper_configuration_keeps_every_external_business_flag_off() -> None:
    settings = Settings(_env_file=None, trading_mode="paper")

    assert settings.trading_mode == "paper"
    assert settings.enable_public_service is False
    assert settings.enable_paid_subscriptions is False
    assert settings.enable_purchase_redirect is False
    assert settings.enable_third_party_funds is False
    assert settings.enable_live_trading is False
    assert settings.enable_order_entry is False


def test_cli_live_mode_exits_two_without_echoing_environment_secrets() -> None:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = "src"
    environment["TRADING_MODE"] = "live"
    environment["TWELVE_DATA_API_KEY"] = "SECRET_CANARY_MUST_NOT_APPEAR"
    result = subprocess.run(
        [sys.executable, "-m", "etf_sentinel.cli", "check-config"],
        cwd=os.getcwd(),
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )

    combined = result.stdout + result.stderr
    assert result.returncode == 2
    assert "SECRET_CANARY_MUST_NOT_APPEAR" not in combined
    assert "TRADING_MODE" in combined or "只能为 paper" in combined

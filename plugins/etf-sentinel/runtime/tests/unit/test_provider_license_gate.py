from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.orm import sessionmaker

from etf_sentinel.enums import DataMode, LicenseStatus
from etf_sentinel.models import DataSnapshot, ProviderRegistry
from etf_sentinel.providers.base import LicenseGateError, authorize_provider
from etf_sentinel.services.signals import decision_policy_hash

NOW = datetime(2025, 1, 30, 8, 0, tzinfo=UTC)


def _registry(**overrides) -> ProviderRegistry:
    values = {
        "provider_code": "licensed_fixture",
        "display_name": "Licensed test fixture",
        "purpose": "test",
        "markets": ["TEST"],
        "regions": ["TEST"],
        "display_right": True,
        "non_display_algorithm_right": True,
        "derivative_right": True,
        "cache_right": True,
        "training_right": False,
        "redistribution_right": False,
        "review_status": LicenseStatus.APPROVED.value,
        "expires_at": NOW + timedelta(days=1),
    }
    values.update(overrides)
    return ProviderRegistry(**values)


def test_unregistered_provider_is_blocked(db_session) -> None:
    with pytest.raises(LicenseGateError, match="未登记"):
        authorize_provider(db_session, "missing", purposes={"algorithm"}, at=NOW)


@pytest.mark.parametrize(
    "status", [LicenseStatus.PENDING, LicenseStatus.BLOCKED, LicenseStatus.EXPIRED]
)
def test_nonapproved_provider_is_blocked(db_session, status: LicenseStatus) -> None:
    db_session.add(_registry(review_status=status.value))
    db_session.commit()

    with pytest.raises(LicenseGateError, match="不是 APPROVED"):
        authorize_provider(db_session, "licensed_fixture", purposes={"algorithm"}, at=NOW)


def test_expired_provider_is_blocked_even_when_review_status_is_approved(db_session) -> None:
    db_session.add(_registry(expires_at=NOW - timedelta(microseconds=1)))
    db_session.commit()

    with pytest.raises(LicenseGateError, match="授权已过期"):
        authorize_provider(db_session, "licensed_fixture", purposes={"algorithm"}, at=NOW)


@pytest.mark.parametrize("purpose", ["training", "redistribution"])
def test_provider_is_blocked_when_requested_use_is_not_licensed(db_session, purpose: str) -> None:
    db_session.add(_registry())
    db_session.commit()

    with pytest.raises(LicenseGateError, match="未获得用途许可"):
        authorize_provider(db_session, "licensed_fixture", purposes={purpose}, at=NOW)


def test_unknown_license_purpose_fails_closed(db_session) -> None:
    db_session.add(_registry())
    db_session.commit()

    with pytest.raises(LicenseGateError, match="未知许可用途"):
        authorize_provider(db_session, "licensed_fixture", purposes={"trade_execution"}, at=NOW)


def test_authorized_provider_returns_registry_record(db_session) -> None:
    row = _registry()
    db_session.add(row)
    db_session.commit()

    authorized = authorize_provider(
        db_session,
        "licensed_fixture",
        purposes={"display", "algorithm", "derivative", "cache"},
        at=NOW,
    )

    assert authorized.id == row.id


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    [
        ("blocked", "不是 APPROVED"),
        ("expired", "授权已过期"),
        ("market_removed", "未授权市场"),
    ],
)
def test_authorization_refreshes_revocation_from_another_session(
    db_session,
    mutation: str,
    expected_error: str,
) -> None:
    row = _registry()
    db_session.add(row)
    db_session.commit()
    authorize_provider(
        db_session,
        row.provider_code,
        purposes={"display"},
        at=NOW,
        market="TEST",
    )
    db_session.commit()

    independent_session = sessionmaker(
        bind=db_session.get_bind(),
        autoflush=False,
        expire_on_commit=False,
    )()
    try:
        independently_loaded = independent_session.get(ProviderRegistry, row.id)
        assert independently_loaded is not None
        if mutation == "blocked":
            independently_loaded.review_status = LicenseStatus.BLOCKED.value
        elif mutation == "expired":
            independently_loaded.expires_at = NOW - timedelta(seconds=1)
        else:
            independently_loaded.markets = []
        independent_session.commit()
    finally:
        independent_session.close()

    with pytest.raises(LicenseGateError, match=expected_error):
        authorize_provider(
            db_session,
            row.provider_code,
            purposes={"display"},
            at=NOW,
            market="TEST",
        )


def test_decision_policy_hash_refreshes_registry_scope_changed_by_another_session(
    db_session, test_settings
) -> None:
    row = _registry()
    snapshot = DataSnapshot(
        provider_code=row.provider_code,
        dataset_type="MARKET_BARS",
        source_uri="internal://policy-refresh",
        as_of=NOW - timedelta(hours=1),
        available_at=NOW - timedelta(minutes=50),
        data_mode=DataMode.DEMO_FIXTURE.value,
        license_scope="INTERNAL_TEST_ONLY",
        parquet_uri="unused-policy-refresh.parquet",
        snapshot_hash="a" * 64,
        config_hash="b" * 64,
        row_count=1,
        quality_flags=[],
        revision="test-v1",
    )
    db_session.add_all([row, snapshot])
    db_session.commit()
    original_hash = decision_policy_hash(
        db_session,
        snapshot=snapshot,
        settings=test_settings,
    )
    db_session.commit()

    independent_session = sessionmaker(
        bind=db_session.get_bind(),
        autoflush=False,
        expire_on_commit=False,
    )()
    try:
        independently_loaded = independent_session.get(ProviderRegistry, row.id)
        assert independently_loaded is not None
        independently_loaded.markets = []
        independent_session.commit()
    finally:
        independent_session.close()

    refreshed_hash = decision_policy_hash(
        db_session,
        snapshot=snapshot,
        settings=test_settings,
    )
    assert refreshed_hash != original_hash
    assert row.markets == []


def test_pending_same_session_revocation_is_flushed_and_not_overwritten(db_session) -> None:
    row = _registry()
    db_session.add(row)
    db_session.commit()
    authorize_provider(db_session, row.provider_code, purposes={"display"}, at=NOW)

    row.review_status = LicenseStatus.BLOCKED.value
    with pytest.raises(LicenseGateError, match="不是 APPROVED"):
        authorize_provider(db_session, row.provider_code, purposes={"display"}, at=NOW)

    assert row.review_status == LicenseStatus.BLOCKED.value


def test_decision_policy_hash_keeps_pending_same_session_registry_scope_change(
    db_session, test_settings
) -> None:
    row = _registry()
    snapshot = DataSnapshot(
        provider_code=row.provider_code,
        dataset_type="MARKET_BARS",
        source_uri="internal://pending-policy-refresh",
        as_of=NOW - timedelta(hours=1),
        available_at=NOW - timedelta(minutes=50),
        data_mode=DataMode.DEMO_FIXTURE.value,
        license_scope="INTERNAL_TEST_ONLY",
        parquet_uri="unused-pending-policy-refresh.parquet",
        snapshot_hash="c" * 64,
        config_hash="d" * 64,
        row_count=1,
        quality_flags=[],
        revision="test-v1",
    )
    db_session.add_all([row, snapshot])
    db_session.commit()
    original_hash = decision_policy_hash(
        db_session,
        snapshot=snapshot,
        settings=test_settings,
    )

    row.markets = []
    changed_hash = decision_policy_hash(
        db_session,
        snapshot=snapshot,
        settings=test_settings,
    )

    assert changed_hash != original_hash
    assert row.markets == []

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

# Importing the model module registers every mapped table on Base.metadata.
from etf_sentinel import models as _models  # noqa: F401
from etf_sentinel.config import Settings
from etf_sentinel.database import Base


@pytest.fixture
def db_session(tmp_path) -> Iterator[Session]:
    database_path = tmp_path / "test.sqlite3"
    engine = create_engine(
        f"sqlite:///{database_path}",
        connect_args={"check_same_thread": False},
    )

    @event.listens_for(engine, "connect")
    def _enable_foreign_keys(dbapi_connection, _connection_record) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(engine)
    local_session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    session = local_session()
    try:
        yield session
    finally:
        session.rollback()
        session.close()
        Base.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture
def test_settings(tmp_path) -> Settings:
    return Settings(
        _env_file=None,
        app_env="test",
        trading_mode="paper",
        database_url=f"sqlite:///{tmp_path / 'settings.sqlite3'}",
        snapshot_dir=tmp_path / "snapshots",
        export_dir=tmp_path / "exports",
        demo_evaluation_time=datetime(2025, 1, 30, 8, 0, tzinfo=UTC),
    )

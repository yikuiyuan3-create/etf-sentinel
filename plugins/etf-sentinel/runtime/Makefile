APP_HOST ?= 127.0.0.1
APP_PORT ?= 8000
UV_CACHE_DIR ?= $(CURDIR)/.uv-cache
UV_PYTHON_INSTALL_DIR ?= $(CURDIR)/.uv-python
UV_ENV = UV_CACHE_DIR=$(UV_CACHE_DIR) UV_PYTHON_INSTALL_DIR=$(UV_PYTHON_INSTALL_DIR)

.PHONY: install init demo serve test lint verify compose-up compose-verify compose-down recovery-drill clean-demo monitor-now monitor-verify test-ui

install:
	$(UV_ENV) uv sync --frozen --all-extras --no-editable --reinstall-package etf-sentinel

init: install
	$(UV_ENV) uv run --no-sync alembic upgrade head

demo: init
	$(UV_ENV) uv run --no-sync etf-sentinel demo
	APP_HOST=$(APP_HOST) APP_PORT=$(APP_PORT) $(UV_ENV) uv run --no-sync etf-sentinel serve

serve: install
	APP_HOST=$(APP_HOST) APP_PORT=$(APP_PORT) $(UV_ENV) uv run --no-sync etf-sentinel serve

test: install
	$(UV_ENV) uv run --no-sync pytest

lint: install
	$(UV_ENV) uv run --no-sync ruff check .
	$(UV_ENV) uv run --no-sync ruff format --check .

verify: install
	$(UV_ENV) uv run --no-sync ruff check .
	$(UV_ENV) uv run --no-sync ruff format --check .
	$(UV_ENV) uv run --no-sync pytest

compose-up:
	APP_PORT=$(APP_PORT) docker compose up --build

compose-verify:
	APP_PORT=$(APP_PORT) docker compose exec -T web python /app/scripts/verify-compose.py

compose-down:
	docker compose down

# Read-only research projection; no new market data or simulated execution.
monitor-now:
	docker compose exec -T worker celery -A etf_sentinel.tasks:celery_app call etf_sentinel.hourly_monitor

monitor-verify:
	docker compose exec -T web python /app/scripts/verify-hourly-compose.py

test-ui:
	node --test tests/ui/test_monitor.cjs

# Fixed synthetic Demo only; creates an isolated restore, never replaces the source.
recovery-drill:
	$(UV_ENV) uv run --no-sync python scripts/recovery-drill.py --execute

clean-demo: install
	$(UV_ENV) uv run --no-sync etf-sentinel reset-demo --confirm-delete-local-demo

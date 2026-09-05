FROM python:3.12.14-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

RUN groupadd --system sentinel && useradd --system --gid sentinel --home /app sentinel

COPY pyproject.toml uv.lock README.md /app/
COPY src /app/src
COPY alembic.ini /app/alembic.ini
COPY alembic /app/alembic
COPY scripts /app/scripts
RUN python -m pip install --upgrade pip==26.2.1 \
    && python -m pip install "uv==0.12.1" \
    && uv sync --frozen --no-dev --no-editable

RUN mkdir -p /app/var/snapshots /app/var/exports && chown -R sentinel:sentinel /app
USER sentinel

EXPOSE 8000
HEALTHCHECK --interval=20s --timeout=5s --start-period=30s --retries=5 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3)"

CMD ["/app/scripts/docker-entrypoint.sh"]

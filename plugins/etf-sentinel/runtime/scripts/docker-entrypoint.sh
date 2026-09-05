#!/bin/sh
set -eu

alembic upgrade head
etf-sentinel demo
exec etf-sentinel serve

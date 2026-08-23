"""Pytest configuration — set environment variables before any test module
imports main.py, which runs init_analytics_db() at import time."""

import os
from pathlib import Path

_TEST_DB = Path(__file__).resolve().parent / "test_analytics.sqlite3"
os.environ.setdefault("ANALYTICS_DB_URL", f"sqlite:///{_TEST_DB.as_posix()}")
os.environ.setdefault("WALKIES_DEV_BYPASS_AUTH", "1")

"""Unit tests for SchemaCache (disk-backed TTL cache)."""
from __future__ import annotations

import pytest

from nl_to_sql.cache.schema_cache import SchemaCache

CONN = "postgresql://user:pass@localhost/db"
SCHEMA = {"users": {"columns": [], "primary_keys": [], "foreign_keys": []}}


@pytest.fixture
def cache(tmp_path):
    return SchemaCache(cache_dir=tmp_path / "schema_cache")


def test_miss_returns_none(cache):
    assert cache.get(CONN) is None


def test_set_and_get(cache):
    cache.set(CONN, SCHEMA)
    assert cache.get(CONN) == SCHEMA


def test_invalidate_removes_entry(cache):
    cache.set(CONN, SCHEMA)
    cache.invalidate(CONN)
    assert cache.get(CONN) is None


def test_invalidate_nonexistent_is_noop(cache):
    cache.invalidate(CONN)  # should not raise


def test_clear_removes_all(cache):
    cache.set(CONN, SCHEMA)
    cache.set("postgresql://other@localhost/db2", SCHEMA)
    cache.clear()
    assert cache.get(CONN) is None
    assert cache.size == 0


def test_size_reflects_entries(cache):
    assert cache.size == 0
    cache.set(CONN, SCHEMA)
    assert cache.size == 1
    cache.set("postgresql://other@localhost/db2", SCHEMA)
    assert cache.size == 2


def test_credentials_not_stored_in_filename(cache, tmp_path):
    cache.set(CONN, SCHEMA)
    files = list((tmp_path / "schema_cache").glob("*.json"))
    assert len(files) == 1
    assert "user" not in files[0].name
    assert "pass" not in files[0].name


def test_corrupted_cache_file_returns_none(cache, tmp_path):
    cache.set(CONN, SCHEMA)
    for f in (tmp_path / "schema_cache").glob("*.json"):
        f.write_text("not valid json", encoding="utf-8")
    assert cache.get(CONN) is None


def test_size_on_empty_cache_dir_returns_zero(tmp_path):
    cache = SchemaCache(cache_dir=tmp_path / "nonexistent")
    assert cache.size == 0

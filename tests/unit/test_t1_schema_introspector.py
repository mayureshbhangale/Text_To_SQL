"""Unit tests for T1: Schema Introspector."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from nl_to_sql.cache.schema_cache import SchemaCache
from nl_to_sql.errors.types import SchemaEmptyError, SchemaIntrospectionError
from nl_to_sql.state import PipelineState
from nl_to_sql.tools import t1_schema_introspector as t1

CONN = "postgresql://user:pass@localhost/testdb"


@pytest.fixture
def state():
    return PipelineState(question="test", db_connection_string=CONN)


@pytest.fixture(autouse=True)
def isolated_cache(tmp_path, monkeypatch):
    """Give each test its own empty cache so tests don't bleed into each other."""
    fresh_cache = SchemaCache(cache_dir=tmp_path / "cache")
    monkeypatch.setattr(t1, "schema_cache", fresh_cache)
    return fresh_cache


def _mock_inspector(tables=("customers",), columns=None, pks=None, fks=None):
    columns = columns or [{"name": "id", "type": "INTEGER", "nullable": False, "default": None}]
    inspector = MagicMock()
    inspector.get_table_names.return_value = list(tables)
    inspector.get_columns.return_value = columns
    inspector.get_pk_constraint.return_value = {"constrained_columns": pks or ["id"]}
    inspector.get_foreign_keys.return_value = fks or []
    return inspector


@patch("nl_to_sql.tools.t1_schema_introspector.inspect")
@patch("nl_to_sql.tools.t1_schema_introspector.create_engine")
def test_introspects_schema(mock_engine, mock_inspect, state):
    mock_inspect.return_value = _mock_inspector()
    result = t1.run(state)
    assert "customers" in result.schema_full
    assert result.trace.node_timings["t1"] >= 0


@patch("nl_to_sql.tools.t1_schema_introspector.inspect")
@patch("nl_to_sql.tools.t1_schema_introspector.create_engine")
def test_result_written_to_cache(mock_engine, mock_inspect, state, isolated_cache):
    mock_inspect.return_value = _mock_inspector()
    t1.run(state)
    assert isolated_cache.get(CONN) is not None


@patch("nl_to_sql.tools.t1_schema_introspector.inspect")
@patch("nl_to_sql.tools.t1_schema_introspector.create_engine")
def test_cache_hit_skips_db(mock_engine, mock_inspect, state, isolated_cache):
    isolated_cache.set(CONN, {"cached_table": {"columns": [], "primary_keys": [], "foreign_keys": []}})
    result = t1.run(state)
    mock_engine.assert_not_called()
    assert "cached_table" in result.schema_full


@patch("nl_to_sql.tools.t1_schema_introspector.inspect")
@patch("nl_to_sql.tools.t1_schema_introspector.create_engine")
def test_force_refresh_bypasses_cache(mock_engine, mock_inspect, state, isolated_cache):
    isolated_cache.set(CONN, {"cached_table": {}})
    state.force_refresh = True
    mock_inspect.return_value = _mock_inspector()
    result = t1.run(state)
    mock_engine.assert_called_once()
    assert "customers" in result.schema_full


@patch("nl_to_sql.tools.t1_schema_introspector.inspect")
@patch("nl_to_sql.tools.t1_schema_introspector.create_engine")
def test_empty_db_raises_schema_empty_error(mock_engine, mock_inspect, state):
    inspector = MagicMock()
    inspector.get_table_names.return_value = []
    mock_inspect.return_value = inspector
    with pytest.raises(SchemaEmptyError):
        t1.run(state)


@patch("nl_to_sql.tools.t1_schema_introspector.inspect")
@patch("nl_to_sql.tools.t1_schema_introspector.create_engine")
def test_engine_error_raises_introspection_error(mock_engine, mock_inspect, state):
    mock_engine.side_effect = Exception("connection refused")
    with pytest.raises(SchemaIntrospectionError):
        t1.run(state)


@patch("nl_to_sql.tools.t1_schema_introspector.inspect")
@patch("nl_to_sql.tools.t1_schema_introspector.create_engine")
def test_timing_recorded_on_error(mock_engine, mock_inspect, state):
    mock_engine.side_effect = Exception("fail")
    with pytest.raises(SchemaIntrospectionError):
        t1.run(state)
    assert "t1" in state.trace.node_timings

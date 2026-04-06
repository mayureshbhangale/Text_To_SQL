"""Unit tests for T2: Schema Normalizer"""
import pytest

from nl_to_sql.errors.types import SchemaNormalizationError
from nl_to_sql.state import PipelineState
from nl_to_sql.tools import t2_schema_normalizer as t2


def test_strips_tbl_prefix(state_with_schema):
    result = t2.run(state_with_schema)
    friendly_names = [t.friendly_name for t in result.schema_norm]
    assert "customers" in friendly_names
    assert "orders" in friendly_names


def test_name_mapping_populated(state_with_schema):
    result = t2.run(state_with_schema)
    assert len(result.name_mapping) > 0


def test_normalize_identifier():
    assert t2._normalize_identifier("tbl_customer_orders") == "customer orders"
    assert t2._normalize_identifier("usr_first_nm") == "first nm"
    assert t2._normalize_identifier("orderID") == "order id"


def test_empty_schema_full_raises():
    state = PipelineState(
        question="q", db_connection_string="postgresql://x@localhost/db"
    )
    with pytest.raises(SchemaNormalizationError):
        t2.run(state)


def test_normalization_error_on_bad_data():
    state = PipelineState(
        question="q", db_connection_string="postgresql://x@localhost/db"
    )
    state.schema_full = {"bad_table": None}  # type: ignore[assignment]
    with pytest.raises(SchemaNormalizationError):
        t2.run(state)

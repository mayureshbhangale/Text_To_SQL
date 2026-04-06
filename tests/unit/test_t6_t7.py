"""Unit tests for T6: Guardrails and T7: SQL Parser + Schema Validator"""
import pytest

from nl_to_sql.errors.types import (
    AmbiguousColumnReferenceError,
    GuardrailViolationError,
    HallucinatedColumnError,
    HallucinatedTableError,
    InvalidJoinError,
    SQLParseError,
)
from nl_to_sql.state import ValidationStatus
from nl_to_sql.tools import t4_join_graph_builder as t4
from nl_to_sql.tools import t6_guardrails as t6
from nl_to_sql.tools import t7_sql_validator as t7

# ── T6: Guardrails ────────────────────────────────────────────────────────────

def test_select_passes(state_with_schema):
    state_with_schema.candidate_sql = "SELECT * FROM customers"
    result = t6.run(state_with_schema)
    assert result.guardrail_result.status == ValidationStatus.PASS


def test_insert_blocked(state_with_schema):
    state_with_schema.candidate_sql = "INSERT INTO customers VALUES (1, 'test', 'a@b.com')"
    with pytest.raises(GuardrailViolationError):
        t6.run(state_with_schema)


def test_drop_blocked(state_with_schema):
    state_with_schema.candidate_sql = "DROP TABLE customers"
    with pytest.raises(GuardrailViolationError):
        t6.run(state_with_schema)


def test_unable_sentinel(state_with_schema):
    state_with_schema.candidate_sql = "UNABLE_TO_ANSWER"
    result = t6.run(state_with_schema)
    assert result.guardrail_result.status == ValidationStatus.FAIL
    assert result.guardrail_result.error_type == "UNABLE_TO_ANSWER"


def test_empty_sql(state_with_schema):
    state_with_schema.candidate_sql = ""
    result = t6.run(state_with_schema)
    assert result.guardrail_result.status == ValidationStatus.FAIL


def test_obfuscated_delete_blocked(state_with_schema):
    # sqlglot token scan layer catches this even if regex misses
    state_with_schema.candidate_sql = "DELETE FROM customers WHERE id = 1"
    with pytest.raises(GuardrailViolationError):
        t6.run(state_with_schema)


# ── T7: SQL Parser + Schema Validator ────────────────────────────────────────

def test_valid_sql_passes(state_with_schema):
    state_with_schema = t4.run(state_with_schema)
    state_with_schema.candidate_sql = (
        "SELECT customers.name, orders.total "
        "FROM customers "
        "JOIN orders ON customers.id = orders.customer_id"
    )
    result = t7.run(state_with_schema)
    assert result.validation_result.status == ValidationStatus.PASS


def test_hallucinated_table_raises(state_with_schema):
    state_with_schema = t4.run(state_with_schema)
    state_with_schema.candidate_sql = "SELECT * FROM ghost_table"
    with pytest.raises(HallucinatedTableError):
        t7.run(state_with_schema)


def test_hallucinated_column_raises(state_with_schema):
    state_with_schema = t4.run(state_with_schema)
    state_with_schema.candidate_sql = "SELECT customers.ghost_col FROM customers"
    with pytest.raises(HallucinatedColumnError):
        t7.run(state_with_schema)


def test_unparseable_sql_raises(state_with_schema):
    state_with_schema = t4.run(state_with_schema)
    state_with_schema.candidate_sql = "THIS IS NOT SQL ;;; !!!"
    with pytest.raises(SQLParseError):
        t7.run(state_with_schema)


def test_ambiguous_column_raises(state_with_schema):
    # 'id' exists in both customers and orders — unqualified reference is ambiguous
    state_with_schema = t4.run(state_with_schema)
    state_with_schema.candidate_sql = (
        "SELECT id FROM customers JOIN orders ON customers.id = orders.customer_id"
    )
    with pytest.raises(AmbiguousColumnReferenceError):
        t7.run(state_with_schema)


def test_invalid_join_raises(state_with_schema):
    # customers and order_items have no direct FK — only via orders
    state_with_schema = t4.run(state_with_schema)
    state_with_schema.candidate_sql = (
        "SELECT customers.name FROM customers "
        "JOIN order_items ON customers.id = order_items.id"
    )
    with pytest.raises(InvalidJoinError):
        t7.run(state_with_schema)

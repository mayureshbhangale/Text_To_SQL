"""Unit tests for T6: Guardrails"""
import pytest

from nl_to_sql.errors.types import GuardrailViolationError
from nl_to_sql.state import ValidationStatus
from nl_to_sql.tools import t6_guardrails as t6


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


"""Unit tests for T7: SQL Parser + Schema Validator"""
from nl_to_sql.errors.types import HallucinatedColumnError, HallucinatedTableError
from nl_to_sql.state import ValidationStatus
from nl_to_sql.tools import t4_join_graph_builder as t4
from nl_to_sql.tools import t7_sql_validator as t7


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

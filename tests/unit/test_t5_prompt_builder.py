"""Unit tests for T5: Prompt Context Builder"""
from nl_to_sql.tools import t2_schema_normalizer as t2
from nl_to_sql.tools import t4_join_graph_builder as t4
from nl_to_sql.tools import t5_prompt_builder as t5


def _build_state(state_with_schema):
    state = t2.run(state_with_schema)
    state = t4.run(state)
    return state


def test_prompt_context_populated(state_with_schema):
    state = _build_state(state_with_schema)
    result = t5.run(state)
    assert len(result.prompt_context) > 0


def test_prompt_contains_table_names(state_with_schema):
    state = _build_state(state_with_schema)
    result = t5.run(state)
    assert "customers" in result.prompt_context
    assert "orders" in result.prompt_context


def test_prompt_contains_fk_hints(state_with_schema):
    state = _build_state(state_with_schema)
    result = t5.run(state)
    assert "customer_id" in result.prompt_context


def test_prompt_contains_select_only_rule(state_with_schema):
    state = _build_state(state_with_schema)
    result = t5.run(state)
    assert "SELECT" in result.prompt_context
    assert "INSERT" in result.prompt_context  # mentioned as blocked


def test_node_timing_recorded(state_with_schema):
    state = _build_state(state_with_schema)
    result = t5.run(state)
    assert "t5" in result.trace.node_timings
    assert result.trace.node_timings["t5"] >= 0

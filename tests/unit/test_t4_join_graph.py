"""Unit tests for T4: Join Graph Builder"""
from nl_to_sql.tools import t4_join_graph_builder as t4


def test_fk_edges_populated(state_with_schema):
    result = t4.run(state_with_schema)
    assert len(result.fk_edges) == 2  # orders→customers, order_items→orders


def test_fk_graph_bidirectional(state_with_schema):
    result = t4.run(state_with_schema)
    # orders references customers, so both directions should exist
    assert "customers" in result.fk_graph.get("orders", [])
    assert "orders" in result.fk_graph.get("customers", [])


def test_join_hint_format(state_with_schema):
    result = t4.run(state_with_schema)
    hints = [e.as_join_hint() for e in result.fk_edges]
    assert any("orders.customer_id -> customers.id" in h for h in hints)


def test_empty_schema_produces_empty_graph(sample_state):
    sample_state.schema_full = {}
    result = t4.run(sample_state)
    assert result.fk_edges == []
    assert result.fk_graph == {}

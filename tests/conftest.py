"""
Shared pytest fixtures.

Every test module can import these via:
    def test_something(sample_state, sample_schema_full): ...
"""

from __future__ import annotations

import pytest

from nl_to_sql.state import PipelineState


SAMPLE_SCHEMA_FULL = {
    "customers": {
        "columns": [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "name", "type": "VARCHAR", "nullable": False},
            {"name": "email", "type": "VARCHAR", "nullable": True},
        ],
        "primary_keys": ["id"],
        "foreign_keys": [],
    },
    "orders": {
        "columns": [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "customer_id", "type": "INTEGER", "nullable": False},
            {"name": "total", "type": "NUMERIC", "nullable": True},
            {"name": "created_at", "type": "TIMESTAMP", "nullable": False},
        ],
        "primary_keys": ["id"],
        "foreign_keys": [
            {
                "from_columns": ["customer_id"],
                "to_table": "customers",
                "to_columns": ["id"],
            }
        ],
    },
    "order_items": {
        "columns": [
            {"name": "id", "type": "INTEGER", "nullable": False},
            {"name": "order_id", "type": "INTEGER", "nullable": False},
            {"name": "product_name", "type": "VARCHAR", "nullable": False},
            {"name": "quantity", "type": "INTEGER", "nullable": False},
            {"name": "price", "type": "NUMERIC", "nullable": False},
        ],
        "primary_keys": ["id"],
        "foreign_keys": [
            {
                "from_columns": ["order_id"],
                "to_table": "orders",
                "to_columns": ["id"],
            }
        ],
    },
}


@pytest.fixture
def sample_schema_full() -> dict:
    return SAMPLE_SCHEMA_FULL


@pytest.fixture
def sample_state() -> PipelineState:
    return PipelineState(
        question="Show me all customers who placed an order",
        db_connection_string="postgresql://test:test@localhost:5432/testdb",
    )


@pytest.fixture
def state_with_schema(sample_state: PipelineState, sample_schema_full: dict) -> PipelineState:
    sample_state.schema_full = sample_schema_full
    return sample_state

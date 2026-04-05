"""
T1: Schema Introspector

Pulls schema metadata from PostgreSQL using SQLAlchemy reflection.
Only metadata is extracted — zero row data ever leaves the database.

Output: schema_full (JSON) written to PipelineState
"""

from __future__ import annotations

import time
from typing import Any

from sqlalchemy import create_engine, inspect, text

from nl_to_sql.cache.schema_cache import schema_cache
from nl_to_sql.errors.types import SchemaEmptyError, SchemaIntrospectionError
from nl_to_sql.state import PipelineState


def run(state: PipelineState) -> PipelineState:
    """
    LangGraph node entrypoint.
    Reads:  state.db_connection_string, state.force_refresh
    Writes: state.schema_full, state.trace.node_timings["t1"]

    Schema is loaded from disk cache if available. Set state.force_refresh=True
    (or call schema_cache.invalidate(conn_str)) to re-introspect after a schema change.
    """
    t_start = time.perf_counter()

    try:
        if not state.force_refresh:
            cached = schema_cache.get(state.db_connection_string)
            if cached is not None:
                state.schema_full = cached
                state.trace.node_timings["t1"] = round(time.perf_counter() - t_start, 4)
                return state

        engine = create_engine(state.db_connection_string)
        inspector = inspect(engine)
        schema_full: dict[str, Any] = {}

        table_names = inspector.get_table_names()
        if not table_names:
            raise SchemaEmptyError(
                "Database connected successfully but no tables were found.",
                context={"connection": _safe_conn_str(state.db_connection_string)},
            )

        for table_name in table_names:
            columns = []
            for col in inspector.get_columns(table_name):
                columns.append({
                    "name": col["name"],
                    "type": str(col["type"]),
                    "nullable": col.get("nullable", True),
                    "default": str(col["default"]) if col.get("default") else None,
                })

            pk_constraint = inspector.get_pk_constraint(table_name)
            primary_keys = pk_constraint.get("constrained_columns", [])

            foreign_keys = []
            for fk in inspector.get_foreign_keys(table_name):
                foreign_keys.append({
                    "from_columns": fk["constrained_columns"],
                    "to_table": fk["referred_table"],
                    "to_columns": fk["referred_columns"],
                })

            schema_full[table_name] = {
                "columns": columns,
                "primary_keys": primary_keys,
                "foreign_keys": foreign_keys,
            }

        state.schema_full = schema_full
        schema_cache.set(state.db_connection_string, schema_full)

    except (SchemaEmptyError, SchemaIntrospectionError):
        raise
    except Exception as exc:
        raise SchemaIntrospectionError(
            f"Failed to introspect schema: {exc}",
            context={"connection": _safe_conn_str(state.db_connection_string)},
        ) from exc
    finally:
        elapsed = time.perf_counter() - t_start
        state.trace.node_timings["t1"] = round(elapsed, 4)

    return state


def _safe_conn_str(conn_str: str) -> str:
    """Strip credentials from connection string for safe logging."""
    try:
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(conn_str)
        safe = parsed._replace(netloc=f"***:***@{parsed.hostname}:{parsed.port}")
        return urlunparse(safe)
    except Exception:
        return "<connection string>"

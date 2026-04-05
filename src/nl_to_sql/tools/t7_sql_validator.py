"""
T7: SQL Parser + Schema Validator

Uses sqlglot to parse the candidate SQL into an AST and validates:
  - All referenced tables exist in the schema
  - All referenced columns exist on the correct tables
  - No ambiguous column references across joined tables
  - All JOINs follow FK relationships (no phantom joins)

This is the authoritative validation gate. T6 is just a fast pre-filter.

Output: validation_result → PipelineState
"""

from __future__ import annotations

import time

import sqlglot
import sqlglot.expressions as exp

from nl_to_sql.errors.types import (
    AmbiguousColumnReferenceError,
    HallucinatedColumnError,
    HallucinatedTableError,
    InvalidJoinError,
    SQLParseError,
)
from nl_to_sql.state import PipelineState, ValidationResult, ValidationStatus


def run(state: PipelineState) -> PipelineState:
    """
    LangGraph node entrypoint.
    Reads:  state.candidate_sql, state.schema_full, state.fk_graph
    Writes: state.validation_result, state.trace.node_timings["t7"]
    Raises: typed validation errors (all RETRYABLE — feed back to A1)
    """
    t_start = time.perf_counter()

    try:
        sql = state.candidate_sql.strip()
        known_tables = set(state.schema_full.keys())

        # Build column lookup: table_name → set of column names
        col_lookup: dict[str, set[str]] = {
            tbl: {col["name"] for col in data["columns"]}
            for tbl, data in state.schema_full.items()
        }

        # Parse
        try:
            statements = sqlglot.parse(sql, dialect="postgres")
            if not statements or statements[0] is None:
                raise SQLParseError(
                    "sqlglot could not parse the candidate SQL.",
                    context={"sql": sql[:300]},
                )
            tree = statements[0]
        except sqlglot.errors.ParseError as exc:
            raise SQLParseError(
                f"SQL parse error: {exc}",
                context={"sql": sql[:300]},
            ) from exc

        # Collect referenced tables from FROM + JOIN clauses
        referenced_tables: set[str] = set()
        alias_map: dict[str, str] = {}   # alias → real table name

        for table_node in tree.find_all(exp.Table):
            real_name = table_node.name
            alias = table_node.alias or real_name
            alias_map[alias] = real_name
            referenced_tables.add(real_name)

        # Validate: all referenced tables exist
        for tbl in referenced_tables:
            if tbl not in known_tables:
                raise HallucinatedTableError(
                    f"Table '{tbl}' does not exist in the schema.",
                    context={"table": tbl, "known_tables": sorted(known_tables)},
                )

        # Validate: column references
        for col_node in tree.find_all(exp.Column):
            col_name = col_node.name
            table_alias = col_node.table

            if table_alias:
                real_table = alias_map.get(table_alias, table_alias)
                if real_table in col_lookup and col_name not in col_lookup[real_table]:
                    raise HallucinatedColumnError(
                        f"Column '{col_name}' does not exist on table '{real_table}'.",
                        context={"column": col_name, "table": real_table},
                    )
            else:
                # Unqualified column — check for ambiguity across joined tables
                matching_tables = [
                    tbl for tbl in referenced_tables
                    if col_name in col_lookup.get(tbl, set())
                ]
                if len(matching_tables) > 1:
                    raise AmbiguousColumnReferenceError(
                        f"Column '{col_name}' is ambiguous — exists in: {matching_tables}",
                        context={"column": col_name, "tables": matching_tables},
                    )

        # Validate: JOIN relationships follow FK graph
        fk_graph = state.fk_graph
        joined_pairs: list[tuple[str, str]] = []
        for join_node in tree.find_all(exp.Join):
            joined_table_node = join_node.find(exp.Table)
            if joined_table_node:
                joined_table = joined_table_node.name
                for other_table in referenced_tables:
                    if other_table != joined_table:
                        joined_pairs.append((other_table, joined_table))

        for a, b in joined_pairs:
            neighbours = fk_graph.get(a, [])
            if b not in neighbours and fk_graph:   # only enforce if FK graph is non-empty
                raise InvalidJoinError(
                    f"No FK relationship between '{a}' and '{b}'.",
                    context={"from": a, "to": b, "fk_graph": fk_graph},
                )

        state.validation_result = ValidationResult(status=ValidationStatus.PASS)

    except (
        SQLParseError,
        HallucinatedTableError,
        HallucinatedColumnError,
        AmbiguousColumnReferenceError,
        InvalidJoinError,
    ):
        raise
    finally:
        state.trace.node_timings["t7"] = round(time.perf_counter() - t_start, 4)

    return state

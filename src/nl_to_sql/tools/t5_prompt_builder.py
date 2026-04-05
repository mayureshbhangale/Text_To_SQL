"""
T5: Prompt Context Builder

Assembles the formatted system prompt sent to A1 (Qwen).

Key design decisions:
- Only schema metadata is included — zero row data
- FK join hints are injected explicitly so the LLM doesn't guess
- Dialect is pinned to PostgreSQL
- Token count is bounded by including only relevant tables when possible

Output: prompt_context string → PipelineState
"""

from __future__ import annotations

import time

from nl_to_sql.state import FKEdge, PipelineState, TableMeta

_SYSTEM_PROMPT_TEMPLATE = """\
You are an expert PostgreSQL query writer. Your only job is to convert
a natural language question into a single, valid, read-only SQL SELECT statement.

Rules you MUST follow:
1. Output ONLY the SQL query — no explanation, no markdown, no code fences.
2. Only use SELECT statements. Never use INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, or any DDL/DML.
3. Only reference tables and columns listed in the schema below.
4. Use the FK join hints provided to construct correct JOINs.
5. Always qualify column names with their table name to avoid ambiguity.
6. If the question cannot be answered with the given schema, output exactly: UNABLE_TO_ANSWER

--- DATABASE SCHEMA (PostgreSQL) ---
{schema_section}

--- FK JOIN HINTS ---
{join_hints_section}

--- ADDITIONAL CONSTRAINTS ---
- Dialect: PostgreSQL
- Read-only: SELECT only
- No LIMIT unless the user asks for a specific number of results
- Use table aliases for readability on multi-table queries
"""


def run(state: PipelineState) -> PipelineState:
    """
    LangGraph node entrypoint.
    Reads:  state.schema_norm, state.fk_edges
    Writes: state.prompt_context, state.trace.node_timings["t5"]
    """
    t_start = time.perf_counter()

    schema_section = _build_schema_section(state.schema_norm)
    join_hints_section = _build_join_hints(state.fk_edges)

    state.prompt_context = _SYSTEM_PROMPT_TEMPLATE.format(
        schema_section=schema_section,
        join_hints_section=join_hints_section or "No foreign key relationships found.",
    )
    state.trace.node_timings["t5"] = round(time.perf_counter() - t_start, 4)

    return state


def _build_schema_section(tables: list[TableMeta]) -> str:
    lines: list[str] = []
    for table in tables:
        lines.append(f"Table: {table.name} (aka '{table.friendly_name}')")
        for col in table.columns:
            pk_marker = " [PK]" if col.is_primary_key else ""
            nullable = "" if col.is_nullable else " NOT NULL"
            lines.append(f"  - {col.name}: {col.data_type}{nullable}{pk_marker}")
        lines.append("")
    return "\n".join(lines)


def _build_join_hints(edges: list[FKEdge]) -> str:
    if not edges:
        return ""
    return "\n".join(f"  - {edge.as_join_hint()}" for edge in edges)

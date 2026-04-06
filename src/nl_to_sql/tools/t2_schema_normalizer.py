"""
T2: Schema Normalizer

Converts raw DB identifiers into human-friendly tokens the LLM can
reason about, while preserving a reverse mapping so generated SQL can
be translated back to real table/column names.

Example: tbl_customer_orders  →  customer order
         usr_first_nm         →  first name

Output: schema_norm + name_mapping written to PipelineState
"""

from __future__ import annotations

import re
import time

from nl_to_sql.errors.types import SchemaNormalizationError
from nl_to_sql.state import ColumnMeta, PipelineState, TableMeta

# Common prefixes that add no semantic value
_STRIP_PREFIXES = re.compile(
    r"^(tbl_|tbl|t_|tb_|vw_|view_|v_|fct_|dim_|stg_|usr_|ref_)",
    re.IGNORECASE,
)


def run(state: PipelineState) -> PipelineState:
    """
    LangGraph node entrypoint.
    Reads:  state.schema_full
    Writes: state.schema_norm, state.name_mapping, state.trace.node_timings["t2"]
    """
    t_start = time.perf_counter()

    try:
        if not state.schema_full:
            raise SchemaNormalizationError(
                "schema_full is empty — T1 must run before T2",
                context={},
            )

        norm_tables: list[TableMeta] = []
        name_mapping: dict[str, str] = {}   # friendly → original

        for table_name, table_data in state.schema_full.items():
            friendly_table = _normalize_identifier(table_name)
            name_mapping[friendly_table] = table_name

            columns: list[ColumnMeta] = []
            for col in table_data["columns"]:
                col_name = col["name"]
                friendly_col = _normalize_identifier(col_name)
                name_mapping[f"{friendly_table}.{friendly_col}"] = f"{table_name}.{col_name}"

                is_pk = col_name in table_data.get("primary_keys", [])
                columns.append(
                    ColumnMeta(
                        name=col_name,
                        data_type=col["type"],
                        is_nullable=col.get("nullable", True),
                        is_primary_key=is_pk,
                    )
                )

            norm_tables.append(
                TableMeta(
                    name=table_name,
                    friendly_name=friendly_table,
                    columns=columns,
                )
            )

        state.schema_norm = norm_tables
        state.name_mapping = name_mapping

    except SchemaNormalizationError:
        raise
    except Exception as exc:
        raise SchemaNormalizationError(
            f"Normalization failed: {exc}",
            context={},
        ) from exc
    finally:
        state.trace.node_timings["t2"] = round(time.perf_counter() - t_start, 4)

    return state


def _normalize_identifier(identifier: str) -> str:
    """
    Turn a DB identifier into a human-readable token string.

    tbl_customer_orders  → customer order
    usr_first_nm         → first nm      (abbreviations left for LLM context)
    orderID              → order id
    """
    # Strip common prefixes
    cleaned = _STRIP_PREFIXES.sub("", identifier)
    # CamelCase → spaced
    cleaned = re.sub(r"([a-z])([A-Z])", r"\1 \2", cleaned)
    # Underscores / hyphens → spaces
    cleaned = re.sub(r"[_\-]+", " ", cleaned)
    # Collapse whitespace
    cleaned = " ".join(cleaned.lower().split())
    return cleaned

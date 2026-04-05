"""
T4: Join Graph Builder

Builds a relationship graph from FK constraints so T5 can provide
accurate JOIN hints to the LLM — no hallucinated joins.

Output: fk_graph (adjacency map) + fk_edges (flat list) → PipelineState
"""

from __future__ import annotations

import time
from collections import defaultdict

from nl_to_sql.state import FKEdge, PipelineState


def run(state: PipelineState) -> PipelineState:
    """
    LangGraph node entrypoint.
    Reads:  state.schema_full
    Writes: state.fk_graph, state.fk_edges, state.trace.node_timings["t4"]
    """
    t_start = time.perf_counter()

    fk_edges: list[FKEdge] = []
    graph: dict[str, list[str]] = defaultdict(list)

    for table_name, table_data in state.schema_full.items():
        for fk in table_data.get("foreign_keys", []):
            for from_col, to_col in zip(
                fk["from_columns"], fk["to_columns"], strict=False
            ):
                edge = FKEdge(
                    from_table=table_name,
                    from_column=from_col,
                    to_table=fk["to_table"],
                    to_column=to_col,
                )
                fk_edges.append(edge)
                # Bidirectional so JOIN hints work in both directions
                graph[table_name].append(fk["to_table"])
                graph[fk["to_table"]].append(table_name)

    state.fk_edges = fk_edges
    state.fk_graph = {k: list(set(v)) for k, v in graph.items()}
    state.trace.node_timings["t4"] = round(time.perf_counter() - t_start, 4)

    return state

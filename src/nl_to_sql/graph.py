"""
LangGraph pipeline graph.

Wires all nodes (T1 → T2 → T4 → T5 → A1 → T6 → T7) into a
directed graph with:
  - Retry loop: T7 failure → back to A1 with error context injected
  - Clarification branch: ambiguous query → surface question to user
  - Fatal exit: guardrail violation or budget exhausted → hard stop

State flows as a single PipelineState object through every node.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Literal

from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph

from nl_to_sql.agents import a1_sql_composer
from nl_to_sql.agents.base import BaseLLMProvider
from nl_to_sql.state import PipelineState, PipelineStatus, ValidationStatus
from nl_to_sql.tools import (
    t1_schema_introspector,
    t2_schema_normalizer,
    t4_join_graph_builder,
    t5_prompt_builder,
    t6_guardrails,
    t7_sql_validator,
)


def build_graph(provider: BaseLLMProvider) -> CompiledStateGraph[PipelineState, None, PipelineState, PipelineState]:
    """
    Build and compile the LangGraph pipeline.

    Args:
        provider: The LLM provider instance (Ollama, Groq, Together, etc.)
                  Injected here so the graph itself has no provider coupling.
    """
    graph = StateGraph(PipelineState)

    # ── Node registration ──────────────────────────────────────────────────

    graph.add_node("t1_introspect", _wrap(t1_schema_introspector.run))
    graph.add_node("t2_normalize", _wrap(t2_schema_normalizer.run))
    graph.add_node("t4_join_graph", _wrap(t4_join_graph_builder.run))
    graph.add_node("t5_prompt", _wrap(t5_prompt_builder.run))
    graph.add_node("a1_compose", _make_llm_node(provider))
    graph.add_node("t6_guardrails", _wrap(t6_guardrails.run))
    graph.add_node("t7_validate", _wrap(t7_sql_validator.run))
    graph.add_node("inject_error_context", _inject_error_context)
    graph.add_node("finalize_success", _finalize_success)
    graph.add_node("finalize_failure", _finalize_failure)

    # ── Edges: happy path ──────────────────────────────────────────────────

    graph.add_edge(START, "t1_introspect")
    graph.add_edge("t1_introspect", "t2_normalize")

    # T2 and T4 can run from T1 output — sequential for now, parallel later
    graph.add_edge("t2_normalize", "t4_join_graph")
    graph.add_edge("t4_join_graph", "t5_prompt")
    graph.add_edge("t5_prompt", "a1_compose")
    graph.add_edge("a1_compose", "t6_guardrails")

    # ── Conditional edges: after T6 ────────────────────────────────────────

    graph.add_conditional_edges(
        "t6_guardrails",
        _route_after_guardrails,
        {
            "pass": "t7_validate",
            "fail": "finalize_failure",
        },
    )

    # ── Conditional edges: after T7 ────────────────────────────────────────

    graph.add_conditional_edges(
        "t7_validate",
        _route_after_validation,
        {
            "pass": "finalize_success",
            "retry": "inject_error_context",
            "fail": "finalize_failure",
        },
    )

    # Retry loop: inject error context → back to A1
    graph.add_edge("inject_error_context", "a1_compose")

    # Terminal nodes
    graph.add_edge("finalize_success", END)
    graph.add_edge("finalize_failure", END)

    return graph.compile()


# ── Routing functions ──────────────────────────────────────────────────────────

def _route_after_guardrails(
    state: PipelineState,
) -> Literal["pass", "fail"]:
    if state.guardrail_result.status == ValidationStatus.PASS:
        return "pass"
    return "fail"


def _route_after_validation(
    state: PipelineState,
) -> Literal["pass", "retry", "fail"]:
    if state.validation_result.status == ValidationStatus.PASS:
        return "pass"

    retry = state.retry_meta
    if retry.budget_exhausted:
        return "fail"

    return "retry"


# ── Node helpers ───────────────────────────────────────────────────────────────

def _inject_error_context(state: PipelineState) -> PipelineState:
    """
    Before retrying A1, inject the validation failure into the question
    so the LLM knows what went wrong and can self-correct.
    """
    error_msg = state.validation_result.error_message or "Unknown validation error"
    error_type = state.validation_result.error_type or "VALIDATION_ERROR"

    state.question = (
        f"{state.question}\n\n"
        f"[RETRY {state.retry_meta.attempt + 1}/{state.retry_meta.max_attempts}]\n"
        f"Your previous SQL failed validation.\n"
        f"Error type: {error_type}\n"
        f"Error detail: {error_msg}\n"
        f"Please generate a corrected SQL query."
    )
    state.retry_meta.attempt += 1
    state.retry_meta.last_error = error_msg
    state.trace.retry_count += 1
    state.status = PipelineStatus.RETRYING
    return state


def _finalize_success(state: PipelineState) -> PipelineState:
    state.final_sql = state.candidate_sql
    state.final_report = {
        "status": "success",
        "sql": state.candidate_sql,
        "retries": state.trace.retry_count,
        "node_timings": state.trace.node_timings,
        "token_usage": state.trace.token_usage,
        "confidence": state.trace.confidence_score,
    }
    state.status = PipelineStatus.SUCCESS
    return state


def _finalize_failure(state: PipelineState) -> PipelineState:
    error_msg = (
        state.guardrail_result.error_message
        or state.validation_result.error_message
        or state.retry_meta.last_error
        or "Pipeline failed"
    )
    state.final_report = {
        "status": "failed",
        "error_type": (
            state.guardrail_result.error_type
            or state.validation_result.error_type
            or "UNKNOWN"
        ),
        "error_message": error_msg,
        "retries": state.trace.retry_count,
        "node_timings": state.trace.node_timings,
    }
    state.status = PipelineStatus.FAILED
    return state


# ── Node wrappers ──────────────────────────────────────────────────────────────

def _wrap(
    fn: Callable[[PipelineState], PipelineState],
) -> Callable[[PipelineState], PipelineState]:
    """Wrap a sync tool function as a LangGraph node."""
    def node(state: PipelineState) -> PipelineState:
        return fn(state)
    node.__name__ = fn.__name__
    return node


def _make_llm_node(
    provider: BaseLLMProvider,
) -> Callable[[PipelineState], PipelineState]:
    """Wrap the async A1 composer as a LangGraph node with provider injected."""
    async def node(state: PipelineState) -> PipelineState:
        return await a1_sql_composer.run(state, provider)

    def sync_node(state: PipelineState) -> PipelineState:
        return asyncio.run(node(state))

    sync_node.__name__ = "a1_compose"
    return sync_node

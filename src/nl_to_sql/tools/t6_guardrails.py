"""
T6: Guardrails

Fast pre-filter before expensive AST parsing in T7.
Uses both regex (speed) and sqlglot token scanning (accuracy) to
detect any DML or DDL in the candidate SQL.

Design note: regex alone can be bypassed (e.g. WITH DELETE AS (...)).
This node is a fast first gate — T7 is the authoritative safety check.

Output: guardrail_result → PipelineState
"""

from __future__ import annotations

import re
import time

import sqlglot

from nl_to_sql.errors.types import GuardrailViolationError
from nl_to_sql.state import PipelineState, ValidationResult, ValidationStatus

# Statements that are never allowed
_BLOCKED_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|MERGE|EXEC|EXECUTE|GRANT|REVOKE|COPY)\b",
    re.IGNORECASE,
)

_UNABLE_SENTINEL = "UNABLE_TO_ANSWER"


def run(state: PipelineState) -> PipelineState:
    """
    LangGraph node entrypoint.
    Reads:  state.candidate_sql
    Writes: state.guardrail_result, state.trace.node_timings["t6"]
    Raises: GuardrailViolationError on DML/DDL detection (always fatal)
    """
    t_start = time.perf_counter()
    sql = state.candidate_sql.strip()

    try:
        # Handle sentinel — LLM explicitly said it can't answer
        if sql == _UNABLE_SENTINEL:
            state.guardrail_result = ValidationResult(
                status=ValidationStatus.FAIL,
                error_type="UNABLE_TO_ANSWER",
                error_message="LLM indicated the question cannot be answered with the available schema.",
            )
            return state

        if not sql:
            state.guardrail_result = ValidationResult(
                status=ValidationStatus.FAIL,
                error_type="EMPTY_SQL",
                error_message="Candidate SQL is empty.",
            )
            return state

        # Layer 1: fast regex scan
        regex_match = _BLOCKED_KEYWORDS.search(sql)
        if regex_match:
            raise GuardrailViolationError(
                f"Blocked keyword detected: {regex_match.group().upper()}",
                context={"matched": regex_match.group(), "sql": sql[:200]},
            )

        # Layer 2: sqlglot token scan (catches obfuscated variants)
        try:
            tokens = sqlglot.tokenize(sql)
            for token in tokens:
                if token.token_type.name in {
                    "INSERT", "UPDATE", "DELETE", "DROP", "CREATE",
                    "ALTER", "TRUNCATE", "MERGE",
                }:
                    raise GuardrailViolationError(
                        f"AST token scan blocked: {token.token_type.name}",
                        context={"token": token.token_type.name, "sql": sql[:200]},
                    )
        except GuardrailViolationError:
            raise
        except Exception:
            pass  # tokenization failure is handled by T7

        state.guardrail_result = ValidationResult(status=ValidationStatus.PASS)

    except GuardrailViolationError:
        raise
    finally:
        state.trace.node_timings["t6"] = round(time.perf_counter() - t_start, 4)

    return state

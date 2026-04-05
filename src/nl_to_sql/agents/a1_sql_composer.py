"""
A1: SQL Composer Agent

The only LLM call in the pipeline. Receives the formatted prompt from T5
and returns a candidate SQL string.

Design principles:
- Depends on BaseLLMProvider — never on a specific provider
- Strips markdown fences from LLM output (models often wrap SQL in ```sql)
- Records token usage and confidence in the trace
- Raises typed errors so the retry loop knows what to do
"""

from __future__ import annotations

import re
import time

from nl_to_sql.agents.base import BaseLLMProvider
from nl_to_sql.errors.types import EmptySQLGeneratedError, LLMProviderError
from nl_to_sql.state import PipelineState

# Strip ```sql ... ``` or ``` ... ``` fences that models often add
_FENCE_RE = re.compile(r"```(?:sql)?\s*(.*?)\s*```", re.DOTALL | re.IGNORECASE)


async def run(state: PipelineState, provider: BaseLLMProvider) -> PipelineState:
    """
    LangGraph node entrypoint (async — LLM call is I/O bound).
    Reads:  state.prompt_context, state.question
    Writes: state.candidate_sql, state.trace (tokens, timing)
    Raises: LLMProviderError, EmptySQLGeneratedError
    """
    t_start = time.perf_counter()

    try:
        response = await provider.complete(
            system_prompt=state.prompt_context,
            user_message=state.question,
            temperature=0.1,      # low temp = deterministic SQL
            max_tokens=1024,
        )

        raw = response.content.strip()

        # Strip markdown code fences if present
        fence_match = _FENCE_RE.search(raw)
        candidate_sql = fence_match.group(1).strip() if fence_match else raw

        if not candidate_sql:
            raise EmptySQLGeneratedError(
                "LLM returned an empty SQL string after fence stripping.",
                context={"raw_response": raw[:200], "model": provider.model_name},
            )

        state.candidate_sql = candidate_sql

        # Record observability data
        state.trace.token_usage["a1"] = response.tokens_used
        if response.confidence is not None:
            state.trace.confidence_score = response.confidence

    except (LLMProviderError, EmptySQLGeneratedError):
        raise
    except Exception as exc:
        raise LLMProviderError(
            f"Unexpected error from LLM provider: {exc}",
            context={"model": provider.model_name},
        ) from exc
    finally:
        state.trace.node_timings["a1"] = round(time.perf_counter() - t_start, 4)

    return state

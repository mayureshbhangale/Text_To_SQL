"""Unit tests for A1: SQL Composer Agent."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from nl_to_sql.agents import a1_sql_composer
from nl_to_sql.agents.base import LLMResponse
from nl_to_sql.errors.types import EmptySQLGeneratedError, LLMProviderError
from nl_to_sql.state import PipelineState


@pytest.fixture
def state():
    return PipelineState(
        question="Show all customers",
        db_connection_string="postgresql://test@localhost/db",
        prompt_context="You are a SQL expert.",
    )


def _make_provider(content="SELECT * FROM customers", tokens=42):
    provider = MagicMock()
    provider.model_name = "mock/model"
    provider.complete = AsyncMock(
        return_value=LLMResponse(content=content, tokens_used=tokens, model="mock/model")
    )
    return provider


@pytest.mark.asyncio
async def test_plain_sql_stored(state):
    provider = _make_provider("SELECT * FROM customers")
    result = await a1_sql_composer.run(state, provider)
    assert result.candidate_sql == "SELECT * FROM customers"


@pytest.mark.asyncio
async def test_strips_sql_code_fence(state):
    provider = _make_provider("```sql\nSELECT * FROM customers\n```")
    result = await a1_sql_composer.run(state, provider)
    assert result.candidate_sql == "SELECT * FROM customers"


@pytest.mark.asyncio
async def test_strips_generic_code_fence(state):
    provider = _make_provider("```\nSELECT 1\n```")
    result = await a1_sql_composer.run(state, provider)
    assert result.candidate_sql == "SELECT 1"


@pytest.mark.asyncio
async def test_token_usage_recorded(state):
    provider = _make_provider(tokens=99)
    await a1_sql_composer.run(state, provider)
    assert state.trace.token_usage["a1"] == 99


@pytest.mark.asyncio
async def test_timing_recorded(state):
    provider = _make_provider()
    await a1_sql_composer.run(state, provider)
    assert "a1" in state.trace.node_timings


@pytest.mark.asyncio
async def test_confidence_recorded_when_present(state):
    provider = MagicMock()
    provider.model_name = "mock/model"
    provider.complete = AsyncMock(
        return_value=LLMResponse(content="SELECT 1", tokens_used=1, model="mock/model", confidence=0.9)
    )
    await a1_sql_composer.run(state, provider)
    assert state.trace.confidence_score == 0.9


@pytest.mark.asyncio
async def test_empty_sql_raises(state):
    provider = _make_provider("   ")
    with pytest.raises(EmptySQLGeneratedError):
        await a1_sql_composer.run(state, provider)


@pytest.mark.asyncio
async def test_provider_error_propagates(state):
    provider = MagicMock()
    provider.model_name = "mock/model"
    provider.complete = AsyncMock(side_effect=LLMProviderError("timeout", context={}))
    with pytest.raises(LLMProviderError):
        await a1_sql_composer.run(state, provider)


@pytest.mark.asyncio
async def test_unexpected_error_wrapped(state):
    provider = MagicMock()
    provider.model_name = "mock/model"
    provider.complete = AsyncMock(side_effect=RuntimeError("boom"))
    with pytest.raises(LLMProviderError):
        await a1_sql_composer.run(state, provider)

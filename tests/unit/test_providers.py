"""Unit tests for LLM providers (Groq, Ollama)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nl_to_sql.agents.providers.groq import GroqProvider
from nl_to_sql.agents.providers.ollama import OllamaProvider
from nl_to_sql.errors.types import LLMProviderError


# ── Groq ──────────────────────────────────────────────────────────────────────

class TestGroqProvider:
    def test_model_name(self):
        p = GroqProvider(api_key="key", model="llama3")
        assert p.model_name == "groq/llama3"

    @pytest.mark.asyncio
    async def test_complete_returns_response(self):
        p = GroqProvider(api_key="key", model="qwen-qwq-32b")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "choices": [{"message": {"content": "SELECT 1"}}],
            "usage": {"total_tokens": 10},
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("nl_to_sql.agents.providers.groq.httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
            mock_client.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value.post = AsyncMock(return_value=mock_resp)
            result = await p.complete("sys", "user")

        assert result.content == "SELECT 1"
        assert result.tokens_used == 10

    @pytest.mark.asyncio
    async def test_complete_raises_on_http_error(self):
        import httpx
        p = GroqProvider(api_key="key")
        with patch("nl_to_sql.agents.providers.groq.httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
            mock_client.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value.post = AsyncMock(
                side_effect=httpx.RequestError("timeout", request=MagicMock())
            )
            with pytest.raises(LLMProviderError):
                await p.complete("sys", "user")

    def test_health_check_true(self):
        p = GroqProvider(api_key="key")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        with patch("nl_to_sql.agents.providers.groq.httpx.get", return_value=mock_resp):
            assert p.health_check() is True

    def test_health_check_false_on_error(self):
        import httpx
        p = GroqProvider(api_key="key")
        with patch(
            "nl_to_sql.agents.providers.groq.httpx.get",
            side_effect=httpx.RequestError("fail", request=MagicMock()),
        ):
            assert p.health_check() is False


# ── Ollama ────────────────────────────────────────────────────────────────────

class TestOllamaProvider:
    def test_model_name(self):
        p = OllamaProvider(model="qwen2.5-coder:7b")
        assert p.model_name == "ollama/qwen2.5-coder:7b"

    @pytest.mark.asyncio
    async def test_complete_returns_response(self):
        p = OllamaProvider()
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "message": {"content": "SELECT 1"},
            "eval_count": 5,
            "prompt_eval_count": 3,
        }
        mock_resp.raise_for_status = MagicMock()

        with patch("nl_to_sql.agents.providers.ollama.httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
            mock_client.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value.post = AsyncMock(return_value=mock_resp)
            result = await p.complete("sys", "user")

        assert result.content == "SELECT 1"
        assert result.tokens_used == 8

    @pytest.mark.asyncio
    async def test_complete_raises_on_http_error(self):
        import httpx
        p = OllamaProvider()
        with patch("nl_to_sql.agents.providers.ollama.httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
            mock_client.return_value.__aexit__ = AsyncMock(return_value=False)
            mock_client.return_value.post = AsyncMock(
                side_effect=httpx.RequestError("timeout", request=MagicMock())
            )
            with pytest.raises(LLMProviderError):
                await p.complete("sys", "user")

    def test_health_check_true(self):
        p = OllamaProvider()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        with patch("nl_to_sql.agents.providers.ollama.httpx.get", return_value=mock_resp):
            assert p.health_check() is True

    def test_health_check_false_on_error(self):
        import httpx
        p = OllamaProvider()
        with patch(
            "nl_to_sql.agents.providers.ollama.httpx.get",
            side_effect=httpx.RequestError("fail", request=MagicMock()),
        ):
            assert p.health_check() is False

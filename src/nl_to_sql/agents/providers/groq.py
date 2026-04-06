"""
Groq provider — fast inference for Qwen, Llama, Mixtral etc.

Usage: set LLM_PROVIDER=groq in .env
       set GROQ_API_KEY=gsk_...
       set GROQ_MODEL=qwen-qwq-32b  (or llama-3.3-70b-versatile etc.)
"""

from __future__ import annotations

import httpx

from nl_to_sql.agents.base import BaseLLMProvider, LLMResponse
from nl_to_sql.errors.types import LLMProviderError

GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"


class GroqProvider(BaseLLMProvider):

    def __init__(self, api_key: str, model: str = "qwen-qwq-32b") -> None:
        self._api_key = api_key
        self._model = model

    @property
    def model_name(self) -> str:
        return f"groq/{self._model}"

    async def complete(
        self,
        system_prompt: str,
        user_message: str,
        temperature: float = 0.1,
        max_tokens: int = 1024,
    ) -> LLMResponse:
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(GROQ_API_URL, headers=headers, json=payload)
                resp.raise_for_status()
                data = resp.json()
                content = data["choices"][0]["message"]["content"]
                tokens = data.get("usage", {}).get("total_tokens", 0)
                return LLMResponse(content=content, tokens_used=tokens, model=self.model_name)
        except httpx.HTTPError as exc:
            raise LLMProviderError(
                f"Groq request failed: {exc}",
                context={"model": self._model},
            ) from exc

    def health_check(self) -> bool:
        try:
            resp = httpx.get(
                "https://api.groq.com/openai/v1/models",
                headers={"Authorization": f"Bearer {self._api_key}"},
                timeout=5.0,
            )
            return bool(resp.status_code == 200)
        except httpx.HTTPError:
            return False

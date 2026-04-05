"""
Ollama provider — runs Qwen (or any model) locally via Ollama.

Usage: set LLM_PROVIDER=ollama in .env
       set OLLAMA_MODEL=qwen2.5-coder:7b
       set OLLAMA_BASE_URL=http://localhost:11434
"""

from __future__ import annotations

import httpx

from nl_to_sql.agents.base import BaseLLMProvider, LLMResponse
from nl_to_sql.errors.types import LLMProviderError


class OllamaProvider(BaseLLMProvider):

    def __init__(self, base_url: str = "http://localhost:11434", model: str = "qwen2.5-coder:7b") -> None:
        self._base_url = base_url.rstrip("/")
        self._model = model

    @property
    def model_name(self) -> str:
        return f"ollama/{self._model}"

    async def complete(
        self,
        system_prompt: str,
        user_message: str,
        temperature: float = 0.1,
        max_tokens: int = 1024,
    ) -> LLMResponse:
        payload = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "options": {"temperature": temperature, "num_predict": max_tokens},
            "stream": False,
        }
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.post(f"{self._base_url}/api/chat", json=payload)
                resp.raise_for_status()
                data = resp.json()
                content = data["message"]["content"]
                tokens = data.get("eval_count", 0) + data.get("prompt_eval_count", 0)
                return LLMResponse(content=content, tokens_used=tokens, model=self.model_name)
        except httpx.HTTPError as exc:
            raise LLMProviderError(
                f"Ollama request failed: {exc}",
                context={"model": self._model, "base_url": self._base_url},
            ) from exc

    def health_check(self) -> bool:
        try:
            resp = httpx.get(f"{self._base_url}/api/tags", timeout=5.0)
            return resp.status_code == 200
        except httpx.HTTPError:
            return False

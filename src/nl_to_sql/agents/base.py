"""
Provider-agnostic LLM interface.

Design principle: A1 (SQL composer) depends on this abstract base,
not on any specific provider. Swap Ollama → Groq → Together by
changing one env var — zero pipeline code changes required.

This is the Open/Closed principle applied to LLM providers.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class LLMResponse:
    content: str
    tokens_used: int = 0
    model: str = ""
    confidence: float | None = None   # provider-specific, if available


class BaseLLMProvider(ABC):
    """
    Every LLM provider must implement this interface.
    The pipeline only ever calls `complete()` — it never knows
    which provider is underneath.
    """

    @abstractmethod
    async def complete(
        self,
        system_prompt: str,
        user_message: str,
        temperature: float = 0.1,
        max_tokens: int = 1024,
    ) -> LLMResponse:
        """Send a prompt and return a structured response."""
        ...

    @abstractmethod
    def health_check(self) -> bool:
        """Return True if the provider is reachable."""
        ...

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Human-readable model identifier for tracing."""
        ...

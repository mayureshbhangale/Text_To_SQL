# ADR 001: LLM provider abstraction layer

**Date:** 2026-04-05
**Status:** Accepted

## Context

The pipeline requires an LLM for SQL generation (A1). Multiple providers
exist (Ollama locally, Groq, Together.ai) with different APIs, latency
profiles, and cost models. Hardcoding any single provider creates tight
coupling and makes benchmarking or swapping impossible without touching
pipeline code.

## Decision

Introduce `BaseLLMProvider` — an abstract interface that every provider
must implement. The pipeline only ever calls `provider.complete()`. The
concrete provider is injected at graph construction time via `build_graph(provider)`.

## Consequences

- **Positive:** Swap providers by changing one env var. No pipeline changes.
- **Positive:** Each provider can be unit-tested in isolation with a mock.
- **Positive:** Open/Closed principle — adding a new provider never modifies existing code.
- **Negative:** Slight indirection — tracing which provider ran requires checking `provider.model_name`.

## Alternatives considered

- LangChain's built-in LLM wrappers: rejected because they abstract too much
  and make token usage / confidence scores harder to access directly.

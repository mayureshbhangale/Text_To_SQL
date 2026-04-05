# NL-to-SQL Pipeline

A production-grade natural language to SQL pipeline built with LangGraph.
Converts plain English questions into validated PostgreSQL queries through
a multi-phase agent pipeline with retry logic, typed error handling, and
full observability.

## What makes this different

Most NL-to-SQL implementations treat the LLM as a black box: dump everything
in, hope SQL comes out. This pipeline treats the LLM as one bounded component
in a validated system.

| Common approach | This pipeline |
|---|---|
| Full DB content sent to LLM | Schema metadata only — zero row data |
| LLM output trusted directly | AST-level validation against real schema |
| Single-shot generation | Retry loop with error context injection |
| Regex keyword blocking | sqlglot AST parsing (bypass-resistant) |
| No observability | Per-node timing, token usage, retry count |
| Hardcoded LLM provider | Provider-agnostic interface (swap via env var) |

## Architecture

```
User question
     │
     ▼
T1: Schema Introspector      ← SQLAlchemy reflection, metadata only
     │
     ▼
T2: Schema Normalizer        ← Friendly tokens, name mapping
     │
     ▼
T4: Join Graph Builder       ← FK relationship edges
     │
     ▼
T5: Prompt Context Builder   ← Structured prompt, dialect pinned
     │
     ▼
A1: SQL Composer (LLM)       ← Qwen / any provider via interface
     │
     ▼
T6: Guardrails               ← Fast DML/DDL block (regex + token scan)
     │
     ▼
T7: SQL Parser + Validator   ← sqlglot AST vs real schema
     │
   ┌─┴─────────┐
   │           │
 PASS        FAIL → inject error context → back to A1 (max 3 retries)
   │
   ▼
Final output: SQL + validation report + trace
```

## Design decisions

See [docs/adr/](docs/adr/) for full Architecture Decision Records.

- **ADR 001** — LLM provider abstraction (swap models without touching pipeline)
- **ADR 002** — Schema-only context (security + cost)
- **ADR 003** — AST validation over regex (bypass-resistant safety)

## Quick start

```bash
# 1. Clone and install
git clone https://github.com/yourname/nl-to-sql
cd nl-to-sql
pip install -e ".[dev]"

# 2. Configure
cp .env.example .env
# Edit .env — set LLM_PROVIDER and DATABASE_URL

# 3. Run tests
make test-cov

# 4. Lint
make lint
```

## LLM providers

Set `LLM_PROVIDER` in `.env`:

| Provider | Value | Notes |
|---|---|---|
| Ollama (local) | `ollama` | Runs Qwen locally, no API key needed |
| Groq | `groq` | Fast cloud inference, requires `GROQ_API_KEY` |
| Together.ai | `together` | Requires `TOGETHER_API_KEY` |

## Error taxonomy

Every failure has a typed error class — no bare `Exception` catches:

| Error | Severity | Behaviour |
|---|---|---|
| `GuardrailViolationError` | Fatal | Hard stop, never retry |
| `HallucinatedTableError` | Retryable | Re-run A1 with error context |
| `HallucinatedColumnError` | Retryable | Re-run A1 with error context |
| `AmbiguousQueryError` | Clarifiable | Ask user for more context |
| `LowConfidenceError` | Clarifiable | Ask user for more context |
| `RetryBudgetExhaustedError` | Fatal | All retries consumed |

## CI pipeline

Every PR is gated on:
- `ruff` — fast linting
- `pylint` — score must be ≥ 8.0 / 10
- `mypy` — strict type checking
- `pytest` — unit + integration tests
- Coverage must be ≥ 80% (reported as percentage in CI output)

## Tech stack

- **LangGraph** — pipeline orchestration and state management
- **SQLAlchemy** — schema introspection (metadata only)
- **sqlglot** — dialect-aware SQL parsing and AST validation
- **Pydantic v2** — typed state object with validation
- **structlog** — structured logging
- **pytest + pytest-cov** — testing and coverage

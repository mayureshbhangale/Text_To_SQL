# ADR 002: Schema-only context — no row data sent to LLM

**Date:** 2026-04-05
**Status:** Accepted

## Context

Most NL-to-SQL implementations dump the full database content (or large
samples) into the LLM prompt. This creates three problems:

1. **Security** — row data may contain PII, credentials, or confidential business data.
2. **Cost** — token usage scales with data volume, not query complexity.
3. **Noise** — actual data values rarely help with query structure generation.

## Decision

T1 (Schema Introspector) extracts **only metadata**: table names, column
names, data types, primary keys, and foreign key constraints. Zero row data
is ever read or forwarded. T5 (Prompt Context Builder) formats this metadata
into the LLM context window.

The database connection is used exclusively by T1 and is never passed to
any LLM-facing component.

## Consequences

- **Positive:** PII never leaves the database boundary.
- **Positive:** Token usage is O(schema size), not O(data size).
- **Positive:** Schema cache (TTL 5 min) makes subsequent requests near-free.
- **Negative:** LLM cannot use example values to resolve ambiguity
  (e.g. "active" vs "inactive" as status values). Mitigated by the
  clarification agent for ambiguous queries.

---

# ADR 003: AST-level validation over regex for SQL safety

**Date:** 2026-04-05
**Status:** Accepted

## Context

A common approach to blocking DML/DDL is a regex check for keywords like
INSERT, DELETE, DROP. This is bypassable:

```sql
WITH DELETE AS (SELECT 1) SELECT * FROM DELETE  -- passes naive regex
```

## Decision

T6 uses regex as a fast first-pass filter. T7 uses sqlglot to parse the
candidate SQL into an Abstract Syntax Tree (AST) and validates structure
against the schema. The AST approach catches:

- Obfuscated DML keywords
- Hallucinated table/column names
- Ambiguous column references
- Joins with no FK relationship

T6 is a speed gate. T7 is the authoritative safety check. A query that
passes T6 but fails T7 is still blocked.

## Consequences

- **Positive:** Cannot be bypassed by keyword obfuscation.
- **Positive:** sqlglot is dialect-aware (PostgreSQL dialect pinned).
- **Positive:** Validation is completely independent of the LLM — swapping
  models does not change safety guarantees.
- **Negative:** sqlglot parse errors on exotic SQL syntax require fallback
  handling (currently raises `SQLParseError` → retry).

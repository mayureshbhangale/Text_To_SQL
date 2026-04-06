"""
Typed error taxonomy for the NL-to-SQL pipeline.

Design principle: every failure mode has a specific type.
No bare `except Exception` anywhere in the codebase.
Each error type carries enough context for the retry logic to
decide whether to retry, ask for clarification, or hard-fail.
"""

from __future__ import annotations

from enum import StrEnum


class ErrorSeverity(StrEnum):
    """Controls retry and escalation behaviour."""
    RETRYABLE    = "retryable"    # send back to LLM with error context
    CLARIFIABLE  = "clarifiable"  # ask user for more info
    FATAL        = "fatal"        # hard stop, no retry


# ── Base ──────────────────────────────────────────────────────────────────────

class NLToSQLError(Exception):
    """Base class for all pipeline errors."""

    severity: ErrorSeverity = ErrorSeverity.FATAL
    error_code: str = "UNKNOWN_ERROR"

    def __init__(self, message: str, context: dict[str, object] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.context = context or {}

    def to_dict(self) -> dict[str, object]:
        return {
            "error_code": self.error_code,
            "severity": self.severity,
            "message": self.message,
            "context": self.context,
        }


# ── Schema errors (Phase 1) ───────────────────────────────────────────────────

class SchemaIntrospectionError(NLToSQLError):
    """T1 failed to connect to or introspect the database."""
    severity = ErrorSeverity.FATAL
    error_code = "SCHEMA_INTROSPECTION_FAILED"


class SchemaEmptyError(NLToSQLError):
    """T1 connected successfully but found no tables."""
    severity = ErrorSeverity.FATAL
    error_code = "SCHEMA_EMPTY"


class SchemaNormalizationError(NLToSQLError):
    """T2 failed to produce a valid normalized schema."""
    severity = ErrorSeverity.FATAL
    error_code = "SCHEMA_NORMALIZATION_FAILED"


# ── Generation errors (Phase 2) ───────────────────────────────────────────────

class LLMProviderError(NLToSQLError):
    """A1 provider returned an error (timeout, rate limit, bad response)."""
    severity = ErrorSeverity.RETRYABLE
    error_code = "LLM_PROVIDER_ERROR"


class EmptySQLGeneratedError(NLToSQLError):
    """A1 returned an empty or whitespace-only SQL string."""
    severity = ErrorSeverity.RETRYABLE
    error_code = "EMPTY_SQL_GENERATED"


class AmbiguousQueryError(NLToSQLError):
    """
    The user's question is too vague to generate reliable SQL.
    Triggers the clarification path instead of retry.
    """
    severity = ErrorSeverity.CLARIFIABLE
    error_code = "AMBIGUOUS_QUERY"


class LowConfidenceError(NLToSQLError):
    """
    A1 generated SQL but confidence score is below threshold.
    Triggers clarification or retry depending on retry budget.
    """
    severity = ErrorSeverity.CLARIFIABLE
    error_code = "LOW_CONFIDENCE_GENERATION"


# ── Guardrail errors (Phase 3 — T6) ──────────────────────────────────────────

class GuardrailViolationError(NLToSQLError):
    """
    T6 detected a DML/DDL statement (INSERT, UPDATE, DELETE, DROP, etc.).
    This is always fatal — never retry a write attempt.
    """
    severity = ErrorSeverity.FATAL
    error_code = "GUARDRAIL_VIOLATION"


# ── Validation errors (Phase 3 — T7) ─────────────────────────────────────────

class HallucinatedTableError(NLToSQLError):
    """T7 found a table name in the SQL that does not exist in the schema."""
    severity = ErrorSeverity.RETRYABLE
    error_code = "HALLUCINATED_TABLE"


class HallucinatedColumnError(NLToSQLError):
    """T7 found a column name that does not exist on the referenced table."""
    severity = ErrorSeverity.RETRYABLE
    error_code = "HALLUCINATED_COLUMN"


class AmbiguousColumnReferenceError(NLToSQLError):
    """T7 found a column referenced without a table qualifier that is ambiguous."""
    severity = ErrorSeverity.RETRYABLE
    error_code = "AMBIGUOUS_COLUMN_REFERENCE"


class SQLParseError(NLToSQLError):
    """T7 sqlglot could not parse the candidate SQL at all."""
    severity = ErrorSeverity.RETRYABLE
    error_code = "SQL_PARSE_ERROR"


class InvalidJoinError(NLToSQLError):
    """T7 detected a JOIN between tables with no FK relationship."""
    severity = ErrorSeverity.RETRYABLE
    error_code = "INVALID_JOIN"


# ── Retry budget exhausted ────────────────────────────────────────────────────

class RetryBudgetExhaustedError(NLToSQLError):
    """
    All retry attempts consumed without a valid SQL being produced.
    Surfaces to the user with the last known error.
    """
    severity = ErrorSeverity.FATAL
    error_code = "RETRY_BUDGET_EXHAUSTED"


# ── Convenience lookup ────────────────────────────────────────────────────────

RETRYABLE_ERRORS = (
    LLMProviderError,
    EmptySQLGeneratedError,
    HallucinatedTableError,
    HallucinatedColumnError,
    AmbiguousColumnReferenceError,
    SQLParseError,
    InvalidJoinError,
)

CLARIFIABLE_ERRORS = (
    AmbiguousQueryError,
    LowConfidenceError,
)

FATAL_ERRORS = (
    SchemaIntrospectionError,
    SchemaEmptyError,
    SchemaNormalizationError,
    GuardrailViolationError,
    RetryBudgetExhaustedError,
)

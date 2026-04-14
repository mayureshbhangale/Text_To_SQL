"""
NL-to-SQL Evaluator — Flask web app.

Run:
    python app.py
Then open: http://localhost:8080
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import sys
from pathlib import Path

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

load_dotenv()
sys.path.insert(0, str(Path(__file__).parent / "src"))

SPIDER_DIR = Path("spider_data")
DEV_DATA: list[dict] = json.loads((SPIDER_DIR / "dev.json").read_text())

app = Flask(__name__)

# ── Provider with raw-response capture ───────────────────────────────────────

def _build_base_provider():
    name = os.getenv("LLM_PROVIDER", "groq").lower()
    if name == "groq":
        from nl_to_sql.agents.providers.groq import GroqProvider
        return GroqProvider(
            api_key=os.getenv("GROQ_API_KEY", ""),
            model=os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
        )
    if name == "ollama":
        from nl_to_sql.agents.providers.ollama import OllamaProvider
        return OllamaProvider(
            base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
            model=os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b"),
        )
    raise ValueError(f"Unknown LLM_PROVIDER={name}")


class CapturingProvider:
    """Wraps any provider and captures the prompt + raw response for display."""

    def __init__(self, inner):
        self._inner = inner
        self.last_prompt: str = ""
        self.last_user_msg: str = ""
        self.last_raw_response: str = ""
        self.last_tokens: int = 0

    @property
    def model_name(self) -> str:
        return self._inner.model_name

    def health_check(self) -> bool:
        return self._inner.health_check()

    async def complete(self, system_prompt, user_message, **kwargs):
        self.last_prompt = system_prompt
        self.last_user_msg = user_message
        response = await self._inner.complete(system_prompt, user_message, **kwargs)
        self.last_raw_response = response.content
        self.last_tokens = response.tokens_used
        return response


_base_provider = _build_base_provider()
capturing_provider = CapturingProvider(_base_provider)

from nl_to_sql.state import PipelineState
from nl_to_sql.tools import t1_schema_introspector as t1
from nl_to_sql.tools import t2_schema_normalizer as t2
from nl_to_sql.tools import t4_join_graph_builder as t4
from nl_to_sql.tools import t5_prompt_builder as t5
from nl_to_sql.tools import t6_guardrails as t6
from nl_to_sql.tools import t7_sql_validator as t7
from nl_to_sql.agents import a1_sql_composer
from nl_to_sql.errors.types import GuardrailViolationError


# ── DB helpers ────────────────────────────────────────────────────────────────

def _db_path(db_id: str) -> Path:
    p = SPIDER_DIR / "database" / db_id / f"{db_id}.sqlite"
    if not p.exists():
        p = SPIDER_DIR / "database" / db_id / f"{db_id}.db"
    return p


def _execute_sql(db_path: Path, sql: str) -> tuple[list[str], list[list], str | None]:
    try:
        con = sqlite3.connect(str(db_path))
        cur = con.execute(sql)
        columns = [d[0] for d in cur.description] if cur.description else []
        rows = [list(r) for r in cur.fetchall()]
        con.close()
        return columns, rows, None
    except Exception as exc:
        return [], [], str(exc)


# ── Step-by-step pipeline runner ──────────────────────────────────────────────

def _run_pipeline_with_steps(question: str, db_url: str) -> dict:
    """Run each node manually and collect per-step output for the UI."""

    steps = []
    state = PipelineState(question=question, db_connection_string=db_url, dialect="sqlite")

    def record(node_id: str, label: str, status: str, timing_ms: float, detail: dict):
        steps.append({
            "id":     node_id,
            "label":  label,
            "status": status,   # "pass" | "fail" | "skip"
            "ms":     round(timing_ms, 1),
            "detail": detail,
        })

    # T1
    try:
        state = t1.run(state)
        ms = state.trace.node_timings.get("t1", 0) * 1000
        tables_summary = {
            name: {
                "columns": [c["name"] for c in data["columns"]],
                "primary_keys": data["primary_keys"],
                "foreign_keys": [
                    f"{fk['from_columns']} → {fk['to_table']}.{fk['to_columns']}"
                    for fk in data["foreign_keys"]
                ],
            }
            for name, data in state.schema_full.items()
        }
        record("t1", "T1 · Schema Introspector", "pass", ms, {
            "summary": f"Found {len(state.schema_full)} tables",
            "input": {
                "db_connection_string": state.db_connection_string,
                "force_refresh": state.force_refresh,
                "dialect": state.dialect,
            },
            "output": {
                "tables": tables_summary,
                "cache_hit": ms < 2,
                "table_count": len(state.schema_full),
            },
        })
    except Exception as exc:
        record("t1", "T1 · Schema Introspector", "fail", 0, {"error": str(exc)})
        return {"steps": steps, "final_sql": "", "error": str(exc),
                "timings": {}, "retries": 0, "token_usage": {}}

    # T2
    state = t2.run(state)
    ms = state.trace.node_timings.get("t2", 0) * 1000
    record("t2", "T2 · Schema Normalizer", "pass", ms, {
        "summary": f"Normalized {len(state.schema_norm)} tables",
        "input": {
            "tables": list(state.schema_full.keys()),
        },
        "output": {
            "friendly_names": {t.name: t.friendly_name for t in state.schema_norm},
            "name_mapping": state.name_mapping,
        },
    })

    # T4
    state = t4.run(state)
    ms = state.trace.node_timings.get("t4", 0) * 1000
    fk_input = {
        tbl: [fk for fk in data.get("foreign_keys", [])]
        for tbl, data in state.schema_full.items()
        if data.get("foreign_keys")
    }
    record("t4", "T4 · Join Graph Builder", "pass", ms, {
        "summary": f"Found {len(state.fk_edges)} FK edges",
        "input": {"foreign_keys_from_schema": fk_input},
        "output": {
            "fk_edges": [e.as_join_hint() for e in state.fk_edges],
            "fk_graph": state.fk_graph,
        },
    })

    # T5
    state = t5.run(state)
    ms = state.trace.node_timings.get("t5", 0) * 1000
    record("t5", "T5 · Prompt Builder", "pass", ms, {
        "summary": "System prompt assembled",
        "input": {
            "schema_norm": [{"table": t.name, "friendly": t.friendly_name, "columns": [c.name for c in t.columns]} for t in state.schema_norm],
            "fk_edges": [e.as_join_hint() for e in state.fk_edges],
            "dialect": state.dialect,
        },
        "output": {"prompt_context": state.prompt_context},
    })

    # A1 (with retry loop)
    max_retries = state.retry_meta.max_attempts
    final_sql = ""
    for attempt in range(max_retries + 1):
        try:
            state = asyncio.run(a1_sql_composer.run(state, capturing_provider))
            ms = state.trace.node_timings.get("a1", 0) * 1000
            record("a1", f"A1 · SQL Composer (LLM){' — retry ' + str(attempt) if attempt else ''}", "pass", ms, {
                "summary": f"Generated SQL  ·  {capturing_provider.last_tokens} tokens",
                "input": {
                    "system_prompt": capturing_provider.last_prompt,
                    "user_message": capturing_provider.last_user_msg,
                    "temperature": 0.1,
                    "max_tokens": 1024,
                    "model": capturing_provider.model_name,
                },
                "output": {
                    "raw_response": capturing_provider.last_raw_response,
                    "candidate_sql": state.candidate_sql,
                    "tokens_used": capturing_provider.last_tokens,
                },
            })
        except Exception as exc:
            record("a1", "A1 · SQL Composer (LLM)", "fail", 0, {"error": str(exc)})
            return {"steps": steps, "final_sql": "", "error": str(exc),
                    "timings": state.trace.node_timings, "retries": attempt,
                    "token_usage": state.trace.token_usage}

        # T6
        try:
            state = t6.run(state)
            ms = state.trace.node_timings.get("t6", 0) * 1000
            record("t6", "T6 · Guardrails", "pass", ms, {
                "summary": "SELECT-only check passed",
                "input": {"candidate_sql": state.candidate_sql},
                "output": {"status": "PASS", "checks": ["regex keyword scan", "sqlglot token scan"]},
            })
        except GuardrailViolationError as exc:
            ms = state.trace.node_timings.get("t6", 0) * 1000
            record("t6", "T6 · Guardrails", "fail", ms, {
                "summary": "DML/DDL blocked",
                "input": {"candidate_sql": state.candidate_sql},
                "output": {"status": "FAIL", "error": str(exc)},
            })
            return {"steps": steps, "final_sql": "", "error": str(exc),
                    "timings": state.trace.node_timings, "retries": attempt,
                    "token_usage": state.trace.token_usage}

        # T7
        try:
            state = t7.run(state)
            ms = state.trace.node_timings.get("t7", 0) * 1000
            record("t7", "T7 · SQL Validator", "pass", ms, {
                "summary": "AST validation passed — tables & columns verified",
                "input": {
                    "candidate_sql": state.candidate_sql,
                    "known_tables": list(state.schema_full.keys()),
                    "fk_graph": state.fk_graph,
                },
                "output": {
                    "status": "PASS",
                    "checks_performed": ["table existence", "column existence", "ambiguous column refs", "FK join validity"],
                },
            })
            final_sql = state.candidate_sql
            break  # success

        except Exception as exc:
            ms = state.trace.node_timings.get("t7", 0) * 1000
            record("t7", f"T7 · SQL Validator (attempt {attempt + 1})", "fail", ms, {
                "summary": str(exc),
                "input": {
                    "candidate_sql": state.candidate_sql,
                    "known_tables": list(state.schema_full.keys()),
                },
                "output": {"status": "FAIL", "error": str(exc), "error_type": type(exc).__name__},
            })
            if attempt >= max_retries - 1:
                return {"steps": steps, "final_sql": "", "error": str(exc),
                        "timings": state.trace.node_timings, "retries": attempt + 1,
                        "token_usage": state.trace.token_usage}
            # inject error context and retry
            state.validation_result.error_message = str(exc)
            state.validation_result.error_type = type(exc).__name__
            state.question = (
                f"{question}\n\n[RETRY {attempt + 1}/{max_retries}]\n"
                f"Your previous SQL failed: {exc}\nPlease fix it."
            )
            state.retry_meta.attempt += 1
            state.trace.retry_count += 1
            state.candidate_sql = ""

    return {
        "steps": steps,
        "final_sql": final_sql,
        "error": None,
        "timings": state.trace.node_timings,
        "retries": state.trace.retry_count,
        "token_usage": state.trace.token_usage,
    }


# ── API routes ────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/databases")
def api_databases():
    return jsonify(sorted(set(d["db_id"] for d in DEV_DATA)))


@app.route("/api/questions")
def api_questions():
    db_id = request.args.get("db", "")
    return jsonify([
        {"index": i, "question": d["question"], "gold_sql": d["query"]}
        for i, d in enumerate(DEV_DATA) if d["db_id"] == db_id
    ])


@app.route("/api/run", methods=["POST"])
def api_run():
    body     = request.get_json()
    db_id    = body["db_id"]
    question = body["question"]
    gold_sql = body["gold_sql"]

    db_path = _db_path(db_id)
    db_url  = f"sqlite:///{db_path.resolve()}"

    result = _run_pipeline_with_steps(question, db_url)

    gen_sql = result["final_sql"]
    gen_cols, gen_rows, gen_err   = _execute_sql(db_path, gen_sql) if gen_sql else ([], [], "No SQL generated")
    gold_cols, gold_rows, gold_err = _execute_sql(db_path, gold_sql)

    exec_match = (
        gen_err is None and gold_err is None
        and sorted([tuple(r) for r in gen_rows]) == sorted([tuple(r) for r in gold_rows])
    )
    exact_match = " ".join(gen_sql.lower().split()) == " ".join(gold_sql.lower().split())

    return jsonify({
        "steps":         result["steps"],
        "generated_sql": gen_sql,
        "gold_sql":      gold_sql,
        "exact_match":   exact_match,
        "exec_match":    exec_match,
        "pipeline_error": result.get("error"),
        "gen_columns":   gen_cols,
        "gen_rows":      gen_rows[:200],
        "gen_error":     gen_err,
        "gold_columns":  gold_cols,
        "gold_rows":     gold_rows[:200],
        "gold_error":    gold_err,
        "timings":       result["timings"],
        "retries":       result["retries"],
        "token_usage":   result["token_usage"],
        "model":         capturing_provider.model_name,
    })


if __name__ == "__main__":
    print(f"\nProvider : {capturing_provider.model_name}")
    print(f"Spider   : {SPIDER_DIR}/database  ({len(set(d['db_id'] for d in DEV_DATA))} databases, {len(DEV_DATA)} questions)")
    print("\nOpen: http://localhost:8080\n")
    app.run(debug=False, port=8080, host="127.0.0.1")

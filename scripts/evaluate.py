"""
Evaluate the NL-to-SQL pipeline against the Spider dev set.

Compares generated SQL against gold SQL using two metrics:
  - Exact match:  generated SQL == gold SQL (case/whitespace normalised)
  - Execution match: both SQLs return the same result rows when run on the DB

Usage:
    # Run on all 1034 dev questions
    python scripts/evaluate.py

    # Limit to first N questions (quick smoke test)
    python scripts/evaluate.py --limit 50

    # One specific database only
    python scripts/evaluate.py --db concert_singer --limit 20

    # Show every result (not just failures)
    python scripts/evaluate.py --limit 20 --verbose
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv

load_dotenv()

SPIDER_DIR = Path("spider_data")
DEV_FILE   = SPIDER_DIR / "dev.json"
DB_DIR     = SPIDER_DIR / "database"


# ── SQL helpers ───────────────────────────────────────────────────────────────

def _normalise(sql: str) -> str:
    """Lowercase + collapse whitespace for exact-match comparison."""
    return " ".join(sql.lower().split())


def _execute(db_path: Path, sql: str) -> list | None:
    """Run SQL and return sorted result rows, or None on error."""
    try:
        con = sqlite3.connect(str(db_path))
        con.row_factory = sqlite3.Row
        rows = con.execute(sql).fetchall()
        con.close()
        return sorted([tuple(r) for r in rows])
    except Exception:
        return None


def _db_path(db_id: str) -> Path:
    p = DB_DIR / db_id / f"{db_id}.sqlite"
    if not p.exists():
        # fallback: some entries only have .db
        p = DB_DIR / db_id / f"{db_id}.db"
    return p


# ── Provider ─────────────────────────────────────────────────────────────────

def _build_provider():
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
    print(f"ERROR: Unknown LLM_PROVIDER={name}")
    sys.exit(1)


# ── Rate-limit-aware invoker ──────────────────────────────────────────────────

def _invoke_with_backoff(graph, state, max_retries: int = 4):
    """Invoke graph, retrying on 429 with exponential backoff."""
    import httpx
    delay = 10
    for attempt in range(max_retries):
        try:
            return graph.invoke(state)
        except Exception as exc:
            if "429" in str(exc) and attempt < max_retries - 1:
                print(f"       [rate limit — waiting {delay}s]")
                time.sleep(delay)
                delay *= 2
            else:
                raise


# ── Evaluation loop ───────────────────────────────────────────────────────────

def run_eval(limit: int | None, db_filter: str | None, verbose: bool) -> None:
    from nl_to_sql.graph import build_graph
    from nl_to_sql.state import PipelineState

    examples: list[dict] = json.loads(DEV_FILE.read_text())
    if db_filter:
        examples = [e for e in examples if e["db_id"] == db_filter]
    if limit:
        examples = examples[:limit]

    provider = _build_provider()
    graph    = build_graph(provider)

    total = len(examples)
    exact_hits = 0
    exec_hits  = 0
    errors     = 0
    results    = []

    print(f"\nEvaluating {total} questions  (provider: {provider.model_name})\n")
    print(f"{'#':>4}  {'DB':<30}  {'EM':>3}  {'EX':>3}  Question")
    print("-" * 90)

    for i, ex in enumerate(examples, 1):
        db_id    = ex["db_id"]
        question = ex["question"]
        gold_sql = ex["query"]
        db_path  = _db_path(db_id)

        if not db_path.exists():
            errors += 1
            results.append({"db": db_id, "question": question, "status": "db_missing"})
            continue

        db_url = f"sqlite:///{db_path.resolve()}"
        state  = PipelineState(
            question=question,
            db_connection_string=db_url,
            dialect="sqlite",
        )

        try:
            raw = _invoke_with_backoff(graph, state)
            final = PipelineState(**raw) if isinstance(raw, dict) else raw
            gen_sql = final.final_sql or ""
        except Exception as exc:
            errors += 1
            results.append({
                "db": db_id, "question": question,
                "gold": gold_sql, "generated": "",
                "exact_match": False, "exec_match": False,
                "error": str(exc),
            })
            print(f"{i:>4}  {db_id:<30}  {'✗':>3}  {'✗':>3}  {question[:50]}  [ERROR]")
            continue

        em = _normalise(gen_sql) == _normalise(gold_sql)
        gold_rows = _execute(db_path, gold_sql)
        gen_rows  = _execute(db_path, gen_sql) if gen_sql else None
        ex_match  = (gold_rows is not None and gen_rows == gold_rows)

        if em:
            exact_hits += 1
        if ex_match:
            exec_hits += 1

        em_sym = "✓" if em else "✗"
        ex_sym = "✓" if ex_match else "✗"
        print(f"{i:>4}  {db_id:<30}  {em_sym:>3}  {ex_sym:>3}  {question[:50]}")

        entry = {
            "db": db_id, "question": question,
            "gold": gold_sql, "generated": gen_sql,
            "exact_match": em, "exec_match": ex_match,
        }
        if verbose and (not em or not ex_match):
            print(f"       Gold : {gold_sql}")
            print(f"       Gen  : {gen_sql}")
        results.append(entry)

        # Pause between requests to stay within Groq rate limits
        time.sleep(1.5)

    # ── Summary ───────────────────────────────────────────────────────────────
    evaluated = total - errors
    print("\n" + "=" * 90)
    print(f"Results on {evaluated} questions ({errors} skipped/errors)\n")
    print(f"  Exact match      : {exact_hits}/{evaluated}  ({100*exact_hits/max(evaluated,1):.1f}%)")
    print(f"  Execution match  : {exec_hits}/{evaluated}  ({100*exec_hits/max(evaluated,1):.1f}%)")

    # Per-database breakdown
    if not db_filter and evaluated > 0:
        from collections import defaultdict
        by_db: dict[str, dict] = defaultdict(lambda: {"em": 0, "ex": 0, "n": 0})
        for r in results:
            if "exact_match" not in r:
                continue
            d = by_db[r["db"]]
            d["n"]  += 1
            d["em"] += int(r["exact_match"])
            d["ex"] += int(r["exec_match"])

        print("\n  Per-database execution match:")
        for db, s in sorted(by_db.items(), key=lambda x: -x[1]["ex"]/max(x[1]["n"],1)):
            pct = 100 * s["ex"] / max(s["n"], 1)
            print(f"    {db:<35} {s['ex']:>3}/{s['n']:<3}  ({pct:.0f}%)")

    # Save full results
    out = Path("eval_results.json")
    out.write_text(json.dumps(results, indent=2))
    print(f"\nFull results saved to {out}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit",   type=int, default=None, help="Max questions to evaluate")
    parser.add_argument("--db",      default=None,           help="Filter to one database by name")
    parser.add_argument("--verbose", action="store_true",    help="Print gold vs generated on mismatches")
    args = parser.parse_args()
    run_eval(args.limit, args.db, args.verbose)


if __name__ == "__main__":
    main()

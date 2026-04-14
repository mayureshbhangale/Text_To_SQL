"""
Cross-database benchmark — 10 questions per database, all 20 databases.
Gives a realistic spread across the full benchmark instead of grinding
through one database and hitting rate limits.

Sleeps 3s between questions, extra 10s between databases.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv
load_dotenv()

SPIDER_DIR = Path("spider_data")
DEV_FILE   = SPIDER_DIR / "dev.json"
DB_DIR     = SPIDER_DIR / "database"
QUESTIONS_PER_DB = 10


def _normalise(sql: str) -> str:
    return " ".join(sql.lower().split())


def _execute(db_path: Path, sql: str):
    try:
        con = sqlite3.connect(str(db_path), timeout=5)
        rows = con.execute(sql).fetchall()
        con.close()
        return sorted([tuple(r) for r in rows])
    except Exception:
        return None


def _db_path(db_id: str) -> Path:
    p = DB_DIR / db_id / f"{db_id}.sqlite"
    if not p.exists():
        p = DB_DIR / db_id / f"{db_id}.db"
    return p


def _build_provider():
    name = os.getenv("LLM_PROVIDER", "groq").lower()
    if name == "groq":
        from nl_to_sql.agents.providers.groq import GroqProvider
        return GroqProvider(
            api_key=os.getenv("GROQ_API_KEY", ""),
            model=os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
        )
    from nl_to_sql.agents.providers.ollama import OllamaProvider
    return OllamaProvider(
        base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
        model=os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b"),
    )


def _invoke_with_backoff(graph, state, max_retries=5):
    delay = 15
    for attempt in range(max_retries):
        try:
            return graph.invoke(state)
        except Exception as exc:
            if "429" in str(exc) and attempt < max_retries - 1:
                print(f"         [rate limit — sleeping {delay}s]", flush=True)
                time.sleep(delay)
                delay = min(delay * 2, 120)
            else:
                raise


def main():
    from nl_to_sql.graph import build_graph
    from nl_to_sql.state import PipelineState

    all_examples: list[dict] = json.loads(DEV_FILE.read_text())

    # Group by database, take first N per db
    by_db: dict[str, list[dict]] = defaultdict(list)
    for ex in all_examples:
        by_db[ex["db_id"]].append(ex)

    sample: list[dict] = []
    for db_id, exs in sorted(by_db.items()):
        sample.extend(exs[:QUESTIONS_PER_DB])

    provider = _build_provider()
    graph    = build_graph(provider)

    total_em = 0
    total_ex = 0
    total_err = 0
    total_n = 0
    results = []
    per_db: dict[str, dict] = defaultdict(lambda: {"em": 0, "ex": 0, "n": 0, "err": 0})

    print(f"\nCross-DB benchmark: {QUESTIONS_PER_DB} questions × {len(by_db)} databases = {len(sample)} total", flush=True)
    print(f"Provider: {provider.model_name}\n", flush=True)
    print(f"{'#':>4}  {'DB':<35}  {'EM':>3}  {'EX':>3}  Question", flush=True)
    print("-" * 95, flush=True)

    current_db = None
    q_num = 0

    for ex in sample:
        db_id    = ex["db_id"]
        question = ex["question"]
        gold_sql = ex["query"]
        db_path  = _db_path(db_id)
        q_num   += 1

        # Extra pause between databases to let rate limits reset
        if db_id != current_db:
            if current_db is not None:
                print(f"       [switching DB → {db_id}, pausing 12s]", flush=True)
                time.sleep(12)
            current_db = db_id

        if not db_path.exists():
            total_err += 1
            per_db[db_id]["err"] += 1
            print(f"{q_num:>4}  {db_id:<35}  {'✗':>3}  {'✗':>3}  DB MISSING", flush=True)
            continue

        db_url = f"sqlite:///{db_path.resolve()}"
        state  = PipelineState(
            question=question,
            db_connection_string=db_url,
            dialect="sqlite",
        )

        try:
            raw   = _invoke_with_backoff(graph, state)
            final = PipelineState(**raw) if isinstance(raw, dict) else raw
            gen_sql = final.final_sql or ""
        except Exception as exc:
            total_err += 1
            per_db[db_id]["err"] += 1
            results.append({"db": db_id, "question": question, "gold": gold_sql,
                            "generated": "", "exact_match": False,
                            "exec_match": False, "error": str(exc)})
            print(f"{q_num:>4}  {db_id:<35}  {'✗':>3}  {'✗':>3}  {question[:45]}  [ERROR]", flush=True)
            time.sleep(3)
            continue

        em        = _normalise(gen_sql) == _normalise(gold_sql)
        gold_rows = _execute(db_path, gold_sql)
        gen_rows  = _execute(db_path, gen_sql) if gen_sql else None
        ex_match  = gold_rows is not None and gen_rows == gold_rows

        total_n  += 1
        if em: total_em += 1
        if ex_match: total_ex += 1

        d = per_db[db_id]
        d["n"]  += 1
        if em: d["em"] += 1
        if ex_match: d["ex"] += 1

        em_sym = "✓" if em else "✗"
        ex_sym = "✓" if ex_match else "✗"
        print(f"{q_num:>4}  {db_id:<35}  {em_sym:>3}  {ex_sym:>3}  {question[:45]}", flush=True)

        if not ex_match:
            print(f"       Gold: {gold_sql[:80]}", flush=True)
            print(f"       Gen : {gen_sql[:80]}", flush=True)

        results.append({"db": db_id, "question": question, "gold": gold_sql,
                        "generated": gen_sql, "exact_match": em, "exec_match": ex_match})

        time.sleep(3)  # 3s between questions — generous for rate limits

    # ── Summary ────────────────────────────────────────────────────────────────
    evaluated = total_n
    print("\n" + "=" * 95, flush=True)
    print(f"RESULTS: {evaluated} evaluated, {total_err} errors\n", flush=True)
    print(f"  Exact match     : {total_em}/{evaluated}  ({100*total_em/max(evaluated,1):.1f}%)", flush=True)
    print(f"  Execution match : {total_ex}/{evaluated}  ({100*total_ex/max(evaluated,1):.1f}%)", flush=True)

    print("\n  Per-database breakdown:", flush=True)
    print(f"  {'Database':<35}  {'Exec Match':>12}  {'Exact Match':>12}  {'Errors':>6}", flush=True)
    print("  " + "-" * 75, flush=True)
    for db_id, s in sorted(per_db.items(), key=lambda x: -x[1]["ex"] / max(x[1]["n"], 1)):
        n = s["n"]
        ex_pct = 100 * s["ex"] / max(n, 1)
        em_pct = 100 * s["em"] / max(n, 1)
        print(f"  {db_id:<35}  {s['ex']:>4}/{n:<4} ({ex_pct:>4.0f}%)  {s['em']:>4}/{n:<4} ({em_pct:>4.0f}%)  {s['err']:>6}", flush=True)

    out = Path("eval_results_crossdb.json")
    out.write_text(json.dumps(results, indent=2))
    print(f"\nResults saved to {out}", flush=True)


if __name__ == "__main__":
    main()

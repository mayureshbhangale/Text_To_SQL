"""
Interactive runner for the NL-to-SQL pipeline.

Usage:
    # Ask a single question
    python scripts/run_pipeline.py "How many singers are from the USA?"

    # Override database
    python scripts/run_pipeline.py "List all students who own a dog" \
        --db data/cosql/databases/pets_1/pets_1.db

    # Use Groq instead of Ollama
    LLM_PROVIDER=groq python scripts/run_pipeline.py "..."

Requirements:
    - Copy .env.example to .env and fill in your provider credentials
    - For Ollama: have Ollama running locally (ollama serve)
    - For Groq: set GROQ_API_KEY in .env
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Make sure the src package is importable when running from repo root
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dotenv import load_dotenv

load_dotenv()


def _build_provider():
    provider_name = os.getenv("LLM_PROVIDER", "ollama").lower()

    if provider_name == "ollama":
        from nl_to_sql.agents.providers.ollama import OllamaProvider
        return OllamaProvider(
            base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
            model=os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b"),
        )

    if provider_name == "groq":
        api_key = os.getenv("GROQ_API_KEY", "")
        if not api_key:
            print("ERROR: GROQ_API_KEY not set in .env")
            sys.exit(1)
        from nl_to_sql.agents.providers.groq import GroqProvider
        return GroqProvider(
            api_key=api_key,
            model=os.getenv("GROQ_MODEL", "qwen-qwq-32b"),
        )

    print(f"ERROR: Unknown LLM_PROVIDER={provider_name!r}. Choose: ollama | groq")
    sys.exit(1)


def _detect_dialect(db_url: str) -> str:
    if db_url.startswith("sqlite"):
        return "sqlite"
    if db_url.startswith("postgresql") or db_url.startswith("postgres"):
        return "postgres"
    return "sqlite"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the NL-to-SQL pipeline")
    parser.add_argument("question", help="Natural language question to convert to SQL")
    parser.add_argument(
        "--db",
        default=None,
        help="Path to SQLite .db file (overrides DATABASE_URL in .env)",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Force schema re-introspection (bypass cache)",
    )
    args = parser.parse_args()

    # Resolve database URL
    if args.db:
        db_path = Path(args.db).resolve()
        # Spider stores data in .sqlite files; .db files are empty stubs
        if db_path.suffix == ".db" and not db_path.stat().st_size:
            sqlite_alt = db_path.with_suffix(".sqlite")
            if sqlite_alt.exists():
                db_path = sqlite_alt
        db_url = f"sqlite:///{db_path}"
    else:
        db_url = os.getenv("DATABASE_URL", "")
        if not db_url:
            print("ERROR: Set DATABASE_URL in .env or pass --db path/to/file.db")
            sys.exit(1)

    dialect = _detect_dialect(db_url)
    provider = _build_provider()

    print(f"\nProvider : {provider.model_name}")
    print(f"Database : {db_url}")
    print(f"Dialect  : {dialect}")
    print(f"Question : {args.question}\n")
    print("Running pipeline...\n")

    from nl_to_sql.graph import build_graph
    from nl_to_sql.state import PipelineState

    graph = build_graph(provider)
    initial_state = PipelineState(
        question=args.question,
        db_connection_string=db_url,
        dialect=dialect,
        force_refresh=args.refresh,
    )

    result = graph.invoke(initial_state)

    # LangGraph returns a dict; reconstruct state for clean access
    if isinstance(result, dict):
        final_state = PipelineState(**result)
    else:
        final_state = result

    print("=" * 60)
    if final_state.final_sql:
        print("RESULT: SUCCESS")
        print(f"\nSQL:\n{final_state.final_sql}")
    else:
        print("RESULT: FAILED")

    print(f"\nReport:\n{json.dumps(final_state.final_report, indent=2)}")
    print(f"\nRetries : {final_state.trace.retry_count}")
    print(f"Timings : {final_state.trace.node_timings}")


if __name__ == "__main__":
    main()

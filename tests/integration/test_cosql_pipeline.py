"""
Integration tests using CoSQL/Spider SQLite databases.

Tests the full pipeline from T1 (schema introspection) through T5 (prompt building)
against real SQLite databases — no LLM call (A1 is mocked).

Databases are in data/cosql/databases/ (created by scripts/create_cosql_fixtures.py).
Run fixture creation first if databases are missing:
    python scripts/create_cosql_fixtures.py
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from nl_to_sql.agents.base import LLMResponse
from nl_to_sql.state import PipelineState, ValidationStatus
from nl_to_sql.tools import (
    t1_schema_introspector as t1,
    t2_schema_normalizer as t2,
    t4_join_graph_builder as t4,
    t5_prompt_builder as t5,
    t6_guardrails as t6,
    t7_sql_validator as t7,
)

DB_DIR = Path(__file__).parent.parent.parent / "data" / "cosql" / "databases"

# Skip the entire module if databases haven't been created yet
pytestmark = pytest.mark.skipif(
    not DB_DIR.exists(),
    reason="CoSQL databases not found — run: python scripts/create_cosql_fixtures.py",
)


def _db_uri(name: str) -> str:
    return f"sqlite:///{DB_DIR / name / (name + '.db')}"


def _state(db_name: str, question: str = "test") -> PipelineState:
    return PipelineState(
        question=question,
        db_connection_string=_db_uri(db_name),
        dialect="sqlite",
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def run_phase1(state: PipelineState) -> PipelineState:
    """Run T1 → T2 → T4 (schema understanding phase)."""
    state = t1.run(state)
    state = t2.run(state)
    state = t4.run(state)
    return state


# ── T1: Schema introspection against real SQLite DBs ─────────────────────────

class TestT1SchemaIntrospection:

    def test_concert_singer_tables_found(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = _state("concert_singer")
        result = t1.run(state)
        assert set(result.schema_full.keys()) == {"stadium", "singer", "concert", "singer_in_concert"}

    def test_pets_1_tables_found(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = _state("pets_1")
        result = t1.run(state)
        assert set(result.schema_full.keys()) == {"Student", "Pets", "Has_Pet"}

    def test_car_1_tables_found(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = _state("car_1")
        result = t1.run(state)
        assert "car_makers" in result.schema_full
        assert "cars_data" in result.schema_full

    def test_foreign_keys_captured(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = _state("concert_singer")
        result = t1.run(state)
        concert_fks = result.schema_full["concert"]["foreign_keys"]
        assert any(fk["to_table"] == "stadium" for fk in concert_fks)

    def test_primary_keys_captured(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = _state("pets_1")
        result = t1.run(state)
        assert "StuID" in result.schema_full["Student"]["primary_keys"]

    def test_timing_recorded(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = _state("concert_singer")
        result = t1.run(state)
        assert result.trace.node_timings["t1"] >= 0


# ── T2: Schema normalization ──────────────────────────────────────────────────

class TestT2SchemaNormalization:

    def test_friendly_names_generated(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = run_phase1(_state("concert_singer"))
        friendly_names = {t.friendly_name for t in state.schema_norm}
        # singer_in_concert → "singer in concert"
        assert "singer in concert" in friendly_names

    def test_name_mapping_populated(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = run_phase1(_state("pets_1"))
        # Every friendly name maps back to an original table name
        assert all(v in {"Student", "Pets", "Has_Pet"} for v in state.name_mapping.values()
                   if "." not in v)  # table-level entries only


# ── T4: FK join graph ─────────────────────────────────────────────────────────

class TestT4JoinGraph:

    def test_concert_singer_fk_edges(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = run_phase1(_state("concert_singer"))
        # concert → stadium, singer_in_concert → concert, singer_in_concert → singer
        edge_pairs = {(e.from_table, e.to_table) for e in state.fk_edges}
        assert ("concert", "stadium") in edge_pairs

    def test_car_1_has_multi_hop_fk(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = run_phase1(_state("car_1"))
        edge_pairs = {(e.from_table, e.to_table) for e in state.fk_edges}
        assert ("countries", "continents") in edge_pairs
        assert ("car_makers", "countries") in edge_pairs

    def test_fk_graph_is_bidirectional(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = run_phase1(_state("concert_singer"))
        # concert → stadium means stadium should also list concert as neighbour
        assert "concert" in state.fk_graph.get("stadium", [])


# ── T5: Prompt context ────────────────────────────────────────────────────────

class TestT5PromptBuilder:

    def test_prompt_contains_table_names(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = run_phase1(_state("concert_singer"))
        state = t5.run(state)
        assert "stadium" in state.prompt_context
        assert "singer" in state.prompt_context

    def test_prompt_contains_sqlite_dialect(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = run_phase1(_state("pets_1"))
        state = t5.run(state)
        assert "SQLITE" in state.prompt_context

    def test_prompt_contains_fk_hints(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = run_phase1(_state("concert_singer"))
        state = t5.run(state)
        assert "->" in state.prompt_context  # FK hint format: from_table.col -> to_table.col


# ── T6 + T7: Validation against CoSQL schemas ────────────────────────────────

class TestValidationWithCoSQL:

    def test_valid_select_passes_concert_singer(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = run_phase1(_state("concert_singer", "How many singers are there?"))
        state.candidate_sql = "SELECT COUNT(*) FROM singer"
        state = t6.run(state)
        assert state.guardrail_result.status == ValidationStatus.PASS
        state = t7.run(state)
        assert state.validation_result.status == ValidationStatus.PASS

    def test_valid_join_passes_pets_1(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = run_phase1(_state("pets_1", "What are the names of students who own a dog?"))
        state.candidate_sql = (
            "SELECT Student.Fname, Student.LName "
            "FROM Student "
            "JOIN Has_Pet ON Student.StuID = Has_Pet.StuID "
            "JOIN Pets ON Has_Pet.PetID = Pets.PetID "
            "WHERE Pets.PetType = 'dog'"
        )
        state = t6.run(state)
        assert state.guardrail_result.status == ValidationStatus.PASS
        state = t7.run(state)
        assert state.validation_result.status == ValidationStatus.PASS

    def test_valid_multi_table_car_1(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = run_phase1(_state("car_1", "What are all the car makers from the USA?"))
        state.candidate_sql = (
            "SELECT car_makers.FullName "
            "FROM car_makers "
            "JOIN countries ON car_makers.Country = countries.CountryId "
            "WHERE countries.CountryName = 'usa'"
        )
        state = t6.run(state)
        state = t7.run(state)
        assert state.validation_result.status == ValidationStatus.PASS

    def test_hallucinated_table_caught(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        from nl_to_sql.errors.types import HallucinatedTableError
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = run_phase1(_state("concert_singer"))
        state.candidate_sql = "SELECT * FROM ghost_table"
        t6.run(state)
        with pytest.raises(HallucinatedTableError):
            t7.run(state)

    def test_hallucinated_column_caught(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        from nl_to_sql.errors.types import HallucinatedColumnError
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = run_phase1(_state("pets_1"))
        state.candidate_sql = "SELECT Student.nonexistent_col FROM Student"
        t6.run(state)
        with pytest.raises(HallucinatedColumnError):
            t7.run(state)

    def test_dml_blocked_on_cosql_db(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        from nl_to_sql.errors.types import GuardrailViolationError
        monkeypatch.setattr(t1, "schema_cache", SchemaCache(tmp_path))
        state = run_phase1(_state("concert_singer"))
        state.candidate_sql = "DELETE FROM singer WHERE Singer_ID = 1"
        with pytest.raises(GuardrailViolationError):
            t6.run(state)


# ── End-to-end: schema cache works across pipeline phases ────────────────────

class TestSchemaCache:

    def test_second_run_uses_cache(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        cache = SchemaCache(tmp_path)
        monkeypatch.setattr(t1, "schema_cache", cache)

        state1 = _state("concert_singer")
        t1.run(state1)
        assert cache.size == 1

        # Second run — cache should be hit, size stays at 1
        state2 = _state("concert_singer")
        t1.run(state2)
        assert cache.size == 1
        assert state2.schema_full == state1.schema_full

    def test_force_refresh_repopulates_cache(self, tmp_path, monkeypatch):
        from nl_to_sql.cache.schema_cache import SchemaCache
        cache = SchemaCache(tmp_path)
        monkeypatch.setattr(t1, "schema_cache", cache)

        state = _state("concert_singer")
        t1.run(state)

        state2 = _state("concert_singer")
        state2.force_refresh = True
        t1.run(state2)
        # Cache should still be 1 entry (overwritten, not added)
        assert cache.size == 1

from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.elements import ColumnElement

from apps.core.models import ActiveThreat
from apps.modules.postgre.active_threats import ActiveThreatDAO


def test_resolve_sort_column_release_date_uses_coalesce():
    column = ActiveThreatDAO._resolve_sort_column("release_date")
    assert isinstance(column, ColumnElement)
    assert column.name == "coalesce"


def test_resolve_sort_column_name_uses_model_column():
    column = ActiveThreatDAO._resolve_sort_column("name")
    assert column is ActiveThreat.name


def test_resolve_sort_column_unknown_returns_none():
    assert ActiveThreatDAO._resolve_sort_column("not_a_field") is None


def test_enabled_sort_key_maps_onto_is_blocked_inverted():
    """D8 replaced the ``enabled`` column, but the frontend's column keeps its meaning.

    ``enabled`` now resolves to ``is_blocked`` with the direction flipped, so "sort ascending by
    enabled" still puts the monitored threats where they were before the flag inverted.
    """
    assert ActiveThreatDAO._resolve_sort_column("enabled") is ActiveThreat.is_blocked
    assert ActiveThreatDAO._sort_is_inverted("enabled") is True


def test_blocked_sort_key_is_the_forward_facing_one():
    assert ActiveThreatDAO._resolve_sort_column("blocked") is ActiveThreat.is_blocked
    assert ActiveThreatDAO._sort_is_inverted("blocked") is False


def test_other_sort_keys_are_never_inverted():
    assert ActiveThreatDAO._sort_is_inverted("name") is False
    assert ActiveThreatDAO._sort_is_inverted(None) is False


def test_search_clause_matches_technique_ids_via_array_to_string():
    clause = ActiveThreatDAO._search_clause("T1053", ["name", "severity"])
    sql = str(clause.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert "array_to_string" in sql
    assert "techniques" in sql
    assert "%T1053%" in sql
    assert "name" in sql
    assert "severity" in sql

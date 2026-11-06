from unittest.mock import MagicMock

from sqlalchemy import select
from sqlalchemy.dialects import postgresql

from apps.core.enums import AttackChainSortField
from apps.core.models import ActiveThreat, AttackChain
from apps.modules.postgre.attack_chains import _CHAIN_LIST_SORTABLE, AttackChainsDAO


def test_attack_chain_sort_field_matches_dao_sortable_columns():
    enum_values = {member.value for member in AttackChainSortField}
    dao_values = _CHAIN_LIST_SORTABLE | {"severity"}
    assert enum_values == dao_values


def test_resolve_chain_sort_column_last_seen_uses_model_column():
    column = AttackChainsDAO._resolve_chain_sort_column("last_seen")
    assert column is AttackChain.last_seen


def test_resolve_chain_sort_column_created_at_uses_model_column():
    column = AttackChainsDAO._resolve_chain_sort_column("created_at")
    assert column is AttackChain.created_at


def test_resolve_chain_sort_column_severity_uses_active_threat_column():
    column = AttackChainsDAO._resolve_chain_sort_column("severity")
    assert column is ActiveThreat.severity


def test_resolve_chain_sort_column_none_defaults_to_last_seen():
    column = AttackChainsDAO._resolve_chain_sort_column(None)
    assert column is AttackChain.last_seen


def test_chain_list_search_matches_event_technique_ids():
    dao = AttackChainsDAO(MagicMock())
    stmt = dao._apply_chain_list_search(select(AttackChain.id), "T1053")
    sql = str(stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert "EXISTS" in sql.upper()
    assert "technique_ids" in sql
    assert "array_to_string" in sql
    assert "%T1053%" in sql

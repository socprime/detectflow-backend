"""Unit tests for :class:`SigmaValidationService` transaction boundaries."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

import apps.managers.sigma_validation_service as svs_module
from apps.core.models import Rule
from apps.managers.sigma_validation_service import SigmaValidationService


def test_rule_timestamps_survive_flush():
    """``created``/``updated`` must be Python-side defaults, not ``func.now()``.

    Validation flushes the rule, then Kafka reads ``rule.updated``. A SQL-side
    ``onupdate`` expires that column, and the producer thread raises MissingGreenlet.
    """
    for name in ("created", "updated"):
        column = Rule.__table__.c[name]
        assert column.default is not None, f"{name} needs a Python-side default"
        assert not column.default.is_clause_element, f"{name} default must not be a SQL expression"

    onupdate = Rule.__table__.c.updated.onupdate
    assert onupdate is not None
    assert not onupdate.is_clause_element, "updated onupdate must not be a SQL expression"


@pytest.mark.asyncio
async def test_validate_rule_flushes_without_commit(monkeypatch):
    db = AsyncMock()
    db.flush = AsyncMock()
    db.commit = AsyncMock()
    service = SigmaValidationService(db)
    service.get_current_module_version = AsyncMock(return_value="1.0.0")
    # Single-rule validation also runs through the module-level threadpool helper.
    monkeypatch.setattr(
        svs_module,
        "_validate_bodies_sync",
        MagicMock(return_value=[svs_module.ValidationResult(is_supported=True, reason=None)]),
    )

    rule = MagicMock()
    rule.id = uuid4()
    rule.body = "title: Test\nstatus: test"
    rule.validated_with_version = None

    supported = await service.validate_rule(rule, force=True)

    assert supported is True
    db.flush.assert_awaited_once()
    db.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_validate_rules_batch_flushes_without_commit(monkeypatch):
    db = AsyncMock()
    db.flush = AsyncMock()
    db.commit = AsyncMock()
    service = SigmaValidationService(db)
    service.get_current_module_version = AsyncMock(return_value="1.0.0")
    # Batch validation runs through the module-level threadpool helper.
    monkeypatch.setattr(
        svs_module,
        "_validate_bodies_sync",
        MagicMock(return_value=[svs_module.ValidationResult(is_supported=False, reason="unsupported")]),
    )

    rule = MagicMock()
    rule.id = uuid4()
    rule.body = "title: Test\nstatus: test"
    rule.validated_with_version = None

    results = await service.validate_rules_batch([rule], force=True)

    assert results[str(rule.id)] is False
    db.flush.assert_awaited_once()
    db.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_validate_rules_batch_skips_flush_when_nothing_to_validate():
    db = AsyncMock()
    db.flush = AsyncMock()
    db.commit = AsyncMock()
    service = SigmaValidationService(db)
    service.get_current_module_version = AsyncMock(return_value="1.0.0")

    rule = MagicMock()
    rule.id = uuid4()
    rule.validated_with_version = "1.0.0"
    rule.is_supported = True

    results = await service.validate_rules_batch([rule], force=False)

    assert results[str(rule.id)] is True
    db.flush.assert_not_awaited()
    db.commit.assert_not_awaited()

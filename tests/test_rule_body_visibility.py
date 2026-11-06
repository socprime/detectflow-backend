"""Tests for HIDE_RESTRICTED_RULE_BODIES API body redaction."""

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from apps.core.converters import RuleConverter


def _rule(
    *, repository_type: str, body: str = "title: Secret Rule\ndetection:\n  condition: selection"
) -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        name="Secret Rule",
        version=1,
        repository_id=uuid4(),
        repository=SimpleNamespace(type=repository_type, name=f"{repository_type}-repo"),
        product=None,
        service=None,
        category=None,
        folder_path=None,
        sigma_uuid=None,
        level=None,
        external_id=None,
        created=datetime.now(UTC),
        updated=datetime.now(UTC),
        is_supported=True,
        unsupported_reason=None,
        is_user_edited_system_rule=False,
        body=body,
        content_hash="abc123",
    )


def _content(*, body: str = "title: Old\ndetection:\n  condition: selection") -> SimpleNamespace:
    return SimpleNamespace(
        rule_id=uuid4(),
        version=1,
        body=body,
        content_hash="def456",
        changed_at=datetime.now(UTC),
        change_source="manual",
        changed_by="tester",
    )


@pytest.mark.parametrize("repo_type", ["local", "external"])
def test_full_detail_keeps_body_for_allowed_repos_when_hiding(monkeypatch, repo_type: str) -> None:
    monkeypatch.setattr("apps.core.converters.settings.hide_restricted_rule_bodies", True)
    body = "title: Visible\n"
    result = RuleConverter.to_full_detail(_rule(repository_type=repo_type, body=body))
    assert result.body == body


@pytest.mark.parametrize("repo_type", ["api", "gitlab"])
def test_full_detail_hides_body_for_restricted_repos(monkeypatch, repo_type: str) -> None:
    monkeypatch.setattr("apps.core.converters.settings.hide_restricted_rule_bodies", True)
    result = RuleConverter.to_full_detail(_rule(repository_type=repo_type))
    assert result.body == ""
    assert result.repository_type == repo_type
    assert result.content_hash == "abc123"


def test_full_detail_shows_body_when_flag_off(monkeypatch) -> None:
    monkeypatch.setattr("apps.core.converters.settings.hide_restricted_rule_bodies", False)
    body = "title: Always Visible\n"
    result = RuleConverter.to_full_detail(_rule(repository_type="api", body=body))
    assert result.body == body


def test_version_detail_hides_body_for_restricted_repo(monkeypatch) -> None:
    monkeypatch.setattr("apps.core.converters.settings.hide_restricted_rule_bodies", True)
    content = _content(body="title: Hidden Version\n")
    result = RuleConverter.to_version_detail(content, repository_type="api")
    assert result.body == ""


def test_version_detail_keeps_body_for_local_repo(monkeypatch) -> None:
    monkeypatch.setattr("apps.core.converters.settings.hide_restricted_rule_bodies", True)
    body = "title: Local Version\n"
    content = _content(body=body)
    result = RuleConverter.to_version_detail(content, repository_type="local")
    assert result.body == body

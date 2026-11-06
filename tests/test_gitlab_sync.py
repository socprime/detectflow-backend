from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from apps.clients.gitlab_api import GitLabApiClient
from apps.core.exceptions import BadRequestError
from apps.core.schemas import IntegrationUpdateRequest
from apps.integrations.providers.gitlab import GitLabConfig, GitLabProvider
from apps.integrations.registry import IntegrationRegistry
from apps.managers.integrations import IntegrationsManager
from apps.modules.postgre.gitlab import gitlab_integration_lock_object_id
from apps.modules.postgre.rule import RuleDAO
from apps.modules.utils import compute_logsource_folder_path, gitlab_folder_path_from_file_path

USER = type("User", (), {"id": uuid4(), "email": "tester@example.com"})()


def test_compute_logsource_folder_path_product_category():
    assert compute_logsource_folder_path("Windows", None, "process_creation") == (
        "_product/windows/_category/process_creation"
    )


def test_gitlab_folder_path_from_file_path():
    assert gitlab_folder_path_from_file_path("windows/process_creation/rule.yml") == "windows/process_creation"
    assert gitlab_folder_path_from_file_path("rule.yml") is None


def test_parse_additional_fields_preserves_explicit_folder_path():
    rule = {
        "body": "title: Example\nlogsource:\n  product: windows\n",
        "folder_path": "custom/gitlab/path",
    }
    RuleDAO._parse_additional_fields_from_body(rule, rule["body"])
    assert rule["folder_path"] == "custom/gitlab/path"


def test_parse_additional_fields_computes_logsource_folder_path_when_missing():
    rule = {"body": "title: Example\nlogsource:\n  product: windows\n  category: process_creation\n"}
    RuleDAO._parse_additional_fields_from_body(rule, rule["body"])
    assert rule["folder_path"] == "_product/windows/_category/process_creation"


def test_parse_additional_fields_keeps_explicit_none_folder_path():
    # GitLab rules in the project root pass folder_path=None and must stay at the repository
    # root rather than being pushed into a logsource-derived virtual folder.
    rule = {
        "body": "title: Example\nlogsource:\n  product: windows\n  category: process_creation\n",
        "folder_path": None,
    }
    RuleDAO._parse_additional_fields_from_body(rule, rule["body"])
    assert rule["folder_path"] is None


def test_gitlab_client_from_integration_config():
    client = GitLabApiClient.from_integration_config(
        {
            "base_url": "https://gitlab.example.com",
            "token": "secret",
            "project_path": "group/repo",
            "branch": "dev",
            "subpath": "rules",
            "verify_ssl": False,
        },
        author_name="Alice",
        author_email="alice@example.com",
    )
    assert client.base_url == "https://gitlab.example.com"
    assert client.project_path == "group/repo"
    assert client.branch == "dev"
    assert client.subpath == "rules"
    assert client.verify_ssl is False
    assert client.author_name == "Alice"


def test_gitlab_provider_registered_with_pull_capability():
    provider_cls = IntegrationRegistry.get("gitlab")
    assert provider_cls is GitLabProvider
    assert provider_cls.get_config_schema() is GitLabConfig
    from apps.integrations.base import PullableIntegration, get_provider_capabilities

    assert issubclass(provider_cls, PullableIntegration)
    assert "pull_rules" in get_provider_capabilities(provider_cls)
    assert provider_cls.immutable_config_fields() == ("base_url", "project_path", "branch", "subpath")


def test_gitlab_config_rejects_empty_project_path():
    with pytest.raises(ValidationError, match="project_path must not be empty"):
        GitLabConfig(token="secret", project_path="   ")


def test_gitlab_config_rejects_base_url_without_scheme():
    with pytest.raises(ValidationError, match="base_url must start with"):
        GitLabConfig(token="secret", project_path="group/repo", base_url="gitlab.com")


def test_gitlab_config_normalizes_base_url_and_project_path():
    config = GitLabConfig(token="secret", project_path="/group/repo/", base_url="https://gitlab.example.com/")
    assert config.base_url == "https://gitlab.example.com"
    assert config.project_path == "group/repo"


def test_gitlab_config_rejects_non_pem_ca_cert():
    with pytest.raises(ValidationError, match="ca_cert must be a PEM-encoded certificate"):
        GitLabConfig(token="secret", project_path="group/repo", ca_cert="not-a-cert")


def test_gitlab_ssl_verify_disabled_when_verify_ssl_false():
    client = GitLabApiClient(base_url="https://gl", token="t", project_path="g/r", verify_ssl=False)
    assert client._ssl_verify() is False


def test_gitlab_ssl_verify_uses_system_context_without_ca_cert():
    import ssl

    client = GitLabApiClient(base_url="https://gl", token="t", project_path="g/r")
    assert isinstance(client._ssl_verify(), ssl.SSLContext)


def test_gitlab_ssl_verify_builds_context_from_ca_cert():
    import ssl
    from datetime import datetime

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "gitlab.internal")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime(2020, 1, 1))
        .not_valid_after(datetime(2040, 1, 1))
        .sign(key, hashes.SHA256())
    )
    pem = cert.public_bytes(serialization.Encoding.PEM).decode()

    client = GitLabApiClient(base_url="https://gl", token="t", project_path="g/r", ca_cert=pem)
    assert isinstance(client._ssl_verify(), ssl.SSLContext)


def test_gitlab_sync_identity_values_normalize():
    values = GitLabProvider.sync_identity_values(
        {
            "base_url": "https://gitlab.example.com/",
            "token": "secret",
            "project_path": "/group/repo/",
            "branch": "main",
            "subpath": "rules/",
        }
    )
    assert values == {
        "base_url": "https://gitlab.example.com",
        "project_path": "group/repo",
        "branch": "main",
        "subpath": "rules",
    }


def test_gitlab_changed_immutable_config_fields_detects_project_path():
    existing = {
        "base_url": "https://gitlab.com",
        "token": "secret",
        "project_path": "group/old",
        "branch": "main",
        "subpath": "",
    }
    merged = dict(existing)
    merged["project_path"] = "group/new"
    assert GitLabProvider.changed_immutable_config_fields(existing, merged) == ["project_path"]


def test_gitlab_changed_immutable_config_fields_ignores_equivalent_normalization():
    existing = {
        "base_url": "https://gitlab.com",
        "token": "secret",
        "project_path": "group/repo",
        "branch": "main",
        "subpath": "",
    }
    merged = dict(existing)
    merged["project_path"] = "/group/repo/"
    assert GitLabProvider.changed_immutable_config_fields(existing, merged) == []


@pytest.mark.asyncio
async def test_gitlab_update_rejects_immutable_config_change(activity_log):
    mgr = IntegrationsManager(AsyncMock())
    mgr.dao = AsyncMock()
    integration_id = uuid4()
    existing = type(
        "Integration",
        (),
        {
            "id": integration_id,
            "name": "GitLab Prod",
            "integration_type": "gitlab",
            "config": {
                "base_url": "https://gitlab.com",
                "token": "secret",
                "project_path": "group/repo",
                "branch": "main",
                "subpath": "rules",
                "auto_pull": True,
            },
        },
    )()
    mgr.get_by_id = AsyncMock(return_value=existing)
    req = IntegrationUpdateRequest(
        config={
            "profile_name": "GitLab Prod",
            "base_url": "https://gitlab.com",
            "token": "secret",
            "project_path": "group/other-repo",
            "branch": "main",
            "subpath": "rules",
            "auto_pull": True,
        }
    )
    with pytest.raises(BadRequestError, match="project_path"):
        await mgr.update(integration_id, req, USER)


@pytest.mark.asyncio
async def test_gitlab_update_allows_token_and_auto_pull_change(activity_log):
    from unittest.mock import patch

    mgr = IntegrationsManager(AsyncMock())
    mgr.dao = AsyncMock()
    integration_id = uuid4()
    existing = type(
        "Integration",
        (),
        {
            "id": integration_id,
            "name": "GitLab Prod",
            "integration_type": "gitlab",
            "config": {
                "base_url": "https://gitlab.com",
                "token": "old-secret",
                "project_path": "group/repo",
                "branch": "main",
                "subpath": "rules",
                "auto_pull": True,
            },
        },
    )()
    updated = type(
        "Integration",
        (),
        {
            "id": integration_id,
            "name": "GitLab Prod",
            "integration_type": "gitlab",
            "config": {"token": "new-secret", "auto_pull": False},
        },
    )()
    mgr.get_by_id = AsyncMock(side_effect=[existing, updated])
    mgr.dao.update.return_value = updated
    mgr.check_connection = AsyncMock()
    req = IntegrationUpdateRequest(
        config={
            "profile_name": "GitLab Prod",
            "base_url": "https://gitlab.com",
            "token": "new-secret",
            "project_path": "group/repo",
            "branch": "main",
            "subpath": "rules",
            "auto_pull": False,
            "verify_ssl": True,
        }
    )
    with patch(
        "apps.managers.integrations.GitlabSyncManager.reconcile_owned_repository",
        new=AsyncMock(),
    ) as reconcile_mock:
        await mgr.update(integration_id, req, USER)
    assert mgr.dao.update.await_args.kwargs["config"]["token"] == "new-secret"
    assert mgr.dao.update.await_args.kwargs["config"]["auto_pull"] is False
    reconcile_mock.assert_awaited_once_with(mgr.db, updated)


@pytest.mark.asyncio
async def test_gitlab_update_reconciles_owned_repository(activity_log):
    from unittest.mock import patch

    mgr = IntegrationsManager(AsyncMock())
    mgr.dao = AsyncMock()
    integration_id = uuid4()
    existing = type(
        "Integration",
        (),
        {
            "id": integration_id,
            "name": "GitLab Prod",
            "integration_type": "gitlab",
            "config": {"token": "secret", "project_path": "group/repo"},
        },
    )()
    updated = type(
        "Integration",
        (),
        {
            "id": integration_id,
            "name": "GitLab Renamed",
            "integration_type": "gitlab",
            "config": {"token": "secret", "project_path": "group/repo", "auto_pull": True},
        },
    )()
    mgr.get_by_id = AsyncMock(return_value=existing)
    mgr.dao.update.return_value = updated

    req = IntegrationUpdateRequest(name="GitLab Renamed")
    with patch(
        "apps.managers.integrations.GitlabSyncManager.reconcile_owned_repository",
        new=AsyncMock(),
    ) as reconcile_mock:
        await mgr.update(integration_id, req, USER)

    reconcile_mock.assert_awaited_once_with(mgr.db, updated)


@pytest.mark.asyncio
async def test_reconcile_owned_repository_syncs_name_and_sync_enabled():
    from unittest.mock import patch

    from apps.managers.gitlab_sync import GitlabSyncManager

    integration_id = uuid4()
    repository_id = uuid4()
    integration = type(
        "Integration",
        (),
        {"id": integration_id, "name": "GitLab Renamed", "config": {"auto_pull": False}},
    )()

    link = type("GitlabRepository", (), {"repository_id": repository_id, "integration_id": integration_id})()
    repo = type("Repository", (), {"id": repository_id, "name": "GitLab Prod", "sync_enabled": True})()

    gitlab_dao = AsyncMock()
    gitlab_dao.get_by_integration.return_value = link
    repo_dao = AsyncMock()
    repo_dao.get_by_id.return_value = repo

    with (
        patch("apps.managers.gitlab_sync.GitlabRepositoryDAO", return_value=gitlab_dao),
        patch("apps.managers.gitlab_sync.RepositoryDAO", return_value=repo_dao),
    ):
        await GitlabSyncManager.reconcile_owned_repository(AsyncMock(), integration)

    repo_dao.update.assert_awaited_once()
    kwargs = repo_dao.update.await_args.kwargs
    assert kwargs["name"] == "GitLab Renamed"
    assert kwargs["sync_enabled"] is False


@pytest.mark.asyncio
async def test_reconcile_owned_repository_noop_when_already_aligned():
    from unittest.mock import patch

    from apps.managers.gitlab_sync import GitlabSyncManager

    integration_id = uuid4()
    repository_id = uuid4()
    integration = type(
        "Integration",
        (),
        {"id": integration_id, "name": "GitLab Prod", "config": {"auto_pull": True}},
    )()

    link = type("GitlabRepository", (), {"repository_id": repository_id, "integration_id": integration_id})()
    repo = type("Repository", (), {"id": repository_id, "name": "GitLab Prod", "sync_enabled": True})()

    gitlab_dao = AsyncMock()
    gitlab_dao.get_by_integration.return_value = link
    repo_dao = AsyncMock()
    repo_dao.get_by_id.return_value = repo

    with (
        patch("apps.managers.gitlab_sync.GitlabRepositoryDAO", return_value=gitlab_dao),
        patch("apps.managers.gitlab_sync.RepositoryDAO", return_value=repo_dao),
    ):
        await GitlabSyncManager.reconcile_owned_repository(AsyncMock(), integration)

    repo_dao.update.assert_not_awaited()


def test_gitlab_integration_lock_object_id_is_stable():
    integration_id = UUID("11111111-2222-3333-4444-555555555555")
    assert gitlab_integration_lock_object_id(integration_id) == gitlab_integration_lock_object_id(integration_id)
    assert 0 <= gitlab_integration_lock_object_id(integration_id) < 2**31 - 1


@pytest.mark.asyncio
async def test_try_acquire_gitlab_integration_lock_with_retry_succeeds_on_second_attempt():
    from unittest.mock import AsyncMock, MagicMock, patch

    from apps.modules.postgre.gitlab import try_acquire_gitlab_integration_lock_with_retry

    session = AsyncMock()
    integration_id = UUID(int=1)
    results = [MagicMock(scalar=MagicMock(return_value=False)), MagicMock(scalar=MagicMock(return_value=True))]
    session.execute = AsyncMock(side_effect=results)

    with patch("apps.modules.postgre.gitlab.asyncio.sleep", new=AsyncMock()) as sleep_mock:
        acquired = await try_acquire_gitlab_integration_lock_with_retry(session, integration_id, retry_delay_seconds=0)

    assert acquired is True
    assert session.execute.await_count == 2
    sleep_mock.assert_awaited_once_with(0)


@pytest.mark.asyncio
async def test_try_acquire_gitlab_integration_lock_with_retry_returns_false_after_two_failures():
    from unittest.mock import AsyncMock, MagicMock, patch

    from apps.modules.postgre.gitlab import try_acquire_gitlab_integration_lock_with_retry

    session = AsyncMock()
    result = MagicMock()
    result.scalar.return_value = False
    session.execute = AsyncMock(return_value=result)

    with patch("apps.modules.postgre.gitlab.asyncio.sleep", new=AsyncMock()) as sleep_mock:
        acquired = await try_acquire_gitlab_integration_lock_with_retry(session, UUID(int=1), retry_delay_seconds=0)

    assert acquired is False
    assert session.execute.await_count == 2
    sleep_mock.assert_awaited_once_with(0)


@pytest.mark.asyncio
async def test_try_acquire_gitlab_integration_lock_returns_scalar():
    from unittest.mock import AsyncMock, MagicMock

    from apps.modules.postgre.gitlab import try_acquire_gitlab_integration_lock

    session = AsyncMock()
    result = MagicMock()
    result.scalar.return_value = True
    session.execute.return_value = result

    acquired = await try_acquire_gitlab_integration_lock(session, UUID(int=1))

    assert acquired is True
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_delete_gitlab_rule_local_only_skips_remote(activity_log):
    from unittest.mock import AsyncMock, MagicMock

    from apps.core.models import GITLAB_REPOSITORY_TYPE
    from apps.managers.rule import RulesOrchestrator

    rule_id = uuid4()
    rule = MagicMock()
    rule.id = rule_id
    rule.name = "GitLab Rule"
    rule.repository.type = GITLAB_REPOSITORY_TYPE
    rule.repository_id = uuid4()

    mgr = RulesOrchestrator(AsyncMock())
    mgr.rule_repo = AsyncMock()
    mgr.rule_repo.get_by_id.return_value = rule
    mgr._delete_gitlab_rule = AsyncMock()

    user = MagicMock()
    await mgr.delete_rule(rule_id, user, gitlab_push=False, gitlab_force=False)

    mgr._delete_gitlab_rule.assert_awaited_once_with(
        rule,
        user,
        gitlab_push=False,
        gitlab_force=False,
    )


@pytest.mark.asyncio
async def test_delete_gitlab_rule_remote_before_local_when_gitlab_push_true(activity_log):
    from unittest.mock import AsyncMock, MagicMock, patch

    from apps.core.models import GITLAB_REPOSITORY_TYPE
    from apps.managers.rule import RulesOrchestrator

    rule_id = uuid4()
    integration_id = uuid4()
    rule = MagicMock()
    rule.id = rule_id
    rule.name = "GitLab Rule"
    rule.repository.type = GITLAB_REPOSITORY_TYPE
    rule.repository_id = uuid4()

    mgr = RulesOrchestrator(AsyncMock())
    mgr.pipeline_rule_repo = AsyncMock()
    mgr.pipeline_rule_repo.get_pipelines_by_rule_id.return_value = []
    mgr.kafka = AsyncMock()
    mgr.rule_repo = AsyncMock()
    mgr._delete_gitlab_remote = AsyncMock()
    mgr._gitlab_integration_id = AsyncMock(return_value=integration_id)

    user = MagicMock()
    with patch(
        "apps.managers.rule.try_acquire_gitlab_integration_lock_with_retry",
        new=AsyncMock(return_value=True),
    ):
        await mgr._delete_gitlab_rule(rule, user, gitlab_push=True, gitlab_force=False)

    mgr._delete_gitlab_remote.assert_awaited_once_with(rule, user, gitlab_force=False)
    mgr.rule_repo.delete.assert_awaited_once_with(rule.id)

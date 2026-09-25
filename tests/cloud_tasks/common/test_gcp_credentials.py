"""Tests for cloud_tasks.common.gcp_credentials: the credentials the runner uses."""

from unittest.mock import MagicMock, patch

import pytest
from google.auth import impersonated_credentials
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as UserCredentials

from cloud_tasks.common.config import GCPConfig
from cloud_tasks.common.gcp_credentials import CLOUD_PLATFORM_SCOPE, load_runner_credentials


@pytest.fixture
def config() -> GCPConfig:
    """A GCP configuration with no credentials of its own."""
    return GCPConfig(project_id="test-project", region="us-central1", job_id="test-job")


def test_default_credentials_are_used_when_nothing_is_configured(config: GCPConfig) -> None:
    """With no credentials file and no service account, the local login is used as-is."""
    user_credentials = MagicMock(spec=UserCredentials)
    with patch(
        "cloud_tasks.common.gcp_credentials.get_default_credentials",
        return_value=(user_credentials, "project-from-adc"),
    ):
        result = load_runner_credentials(config)

    assert result.credentials is user_credentials
    assert result.project_id == "project-from-adc"
    assert result.source_is_personal is True
    assert result.impersonated_service_account is None


def test_credentials_file_is_not_personal(tmp_path, config: GCPConfig) -> None:
    """A credentials file is a service account key, which doesn't belong to a person."""
    key_file = tmp_path / "key.json"
    key_file.write_text("{}")
    config.credentials_file = str(key_file)
    key_credentials = MagicMock(spec=service_account.Credentials)

    with patch.object(
        service_account.Credentials, "from_service_account_file", return_value=key_credentials
    ):
        result = load_runner_credentials(config)

    assert result.credentials is key_credentials
    assert result.source_is_personal is False


def test_runner_service_account_is_impersonated(config: GCPConfig) -> None:
    """Naming a runner service account makes every call be made as that account."""
    config.runner_service_account = "runner@test-project.iam.gserviceaccount.com"
    source = MagicMock(spec=UserCredentials)
    impersonated = MagicMock(spec=impersonated_credentials.Credentials)

    with (
        patch(
            "cloud_tasks.common.gcp_credentials.get_default_credentials",
            return_value=(source, "test-project"),
        ),
        patch(
            "cloud_tasks.common.gcp_credentials.impersonated_credentials.Credentials",
            return_value=impersonated,
        ) as mock_impersonate,
    ):
        result = load_runner_credentials(config)

    assert result.credentials is impersonated
    assert result.impersonated_service_account == config.runner_service_account
    mock_impersonate.assert_called_once_with(
        source_credentials=source,
        target_principal=config.runner_service_account,
        target_scopes=[CLOUD_PLATFORM_SCOPE],
    )


def test_impersonation_does_not_hide_that_the_login_will_expire(config: GCPConfig) -> None:
    """The impersonated token is refreshed with the credentials underneath it.

    A personal login that expires takes the impersonation with it, so impersonating a
    service account is not a way to make a run survive its author's login.
    """
    config.runner_service_account = "runner@test-project.iam.gserviceaccount.com"
    with (
        patch(
            "cloud_tasks.common.gcp_credentials.get_default_credentials",
            return_value=(MagicMock(spec=UserCredentials), "test-project"),
        ),
        patch("cloud_tasks.common.gcp_credentials.impersonated_credentials.Credentials"),
    ):
        result = load_runner_credentials(config)

    assert result.source_is_personal is True


def test_impersonation_failure_says_what_permission_is_missing(config: GCPConfig) -> None:
    """Not being allowed to impersonate is the usual failure, so it names the role."""
    config.runner_service_account = "runner@test-project.iam.gserviceaccount.com"
    with (
        patch(
            "cloud_tasks.common.gcp_credentials.get_default_credentials",
            return_value=(MagicMock(spec=UserCredentials), "test-project"),
        ),
        patch(
            "cloud_tasks.common.gcp_credentials.impersonated_credentials.Credentials",
            side_effect=Exception("permission denied"),
        ),
    ):
        with pytest.raises(RuntimeError, match="serviceAccountTokenCreator"):
            load_runner_credentials(config)

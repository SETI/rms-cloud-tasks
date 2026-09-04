"""Tests for cloud_tasks.instance_manager.gcp: local credential suitability."""

from unittest.mock import MagicMock, patch

import pytest
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials as UserCredentials

from cloud_tasks.common.config import GCPConfig
from cloud_tasks.instance_manager.gcp import GCPComputeInstanceManager


def _make_manager(gcp_config: GCPConfig, credentials) -> GCPComputeInstanceManager:
    """Build a GCP instance manager whose default credentials are the ones given.

    Parameters:
        gcp_config: Configuration to build the manager from
        credentials: What google.auth.default() should hand back

    Returns:
        GCPComputeInstanceManager: A manager with every client mocked out.
    """
    with (
        patch(
            "cloud_tasks.instance_manager.gcp.get_default_credentials",
            return_value=(credentials, "test-project"),
        ),
        patch("google.cloud.compute_v1.ZonesClient", return_value=MagicMock()),
        patch("google.cloud.compute_v1.RegionsClient", return_value=MagicMock()),
        patch("google.cloud.compute_v1.MachineTypesClient", return_value=MagicMock()),
        patch("google.cloud.compute_v1.DisksClient", return_value=MagicMock()),
        patch("google.cloud.compute_v1.ImagesClient", return_value=MagicMock()),
        patch("google.cloud.billing.CloudCatalogClient", return_value=MagicMock()),
        patch.object(GCPComputeInstanceManager, "_load_pricing_cache_from_file", lambda self: None),
        patch.object(GCPComputeInstanceManager, "_save_pricing_cache_to_file", lambda self: None),
    ):
        return GCPComputeInstanceManager(gcp_config)


@pytest.fixture
def config() -> GCPConfig:
    """A minimal GCP configuration using default application credentials."""
    return GCPConfig(project_id="test-project", region="us-central1", job_id="test-job")


def test_personal_credentials_are_reported(config: GCPConfig) -> None:
    """A login from "gcloud auth application-default login" won't outlast a long job."""
    manager = _make_manager(config, MagicMock(spec=UserCredentials))

    warning = manager.local_credential_warning()

    assert warning is not None
    assert "service account" in warning
    assert "TERMINATE" in warning
    # The warning says what to grant the service account it recommends
    assert "roles/compute.instanceAdmin.v1" in warning
    assert "roles/pubsub.editor" in warning
    # Reading prices from the Cloud Billing Catalog API needs no role at all, so there is
    # none to name; a billing role would also be granted on a billing account rather than
    # on the project the rest of these are granted on
    assert "roles/billing" not in warning


def test_service_account_credentials_are_not_reported(config: GCPConfig) -> None:
    """A service account keeps working unattended, so there is nothing to say."""
    manager = _make_manager(config, MagicMock(spec=service_account.Credentials))

    assert manager.local_credential_warning() is None


def test_credentials_file_is_not_reported(tmp_path, config: GCPConfig) -> None:
    """Naming a credentials file in the configuration is naming a service account."""
    key_file = tmp_path / "key.json"
    key_file.write_text("{}")
    config.credentials_file = str(key_file)

    with patch.object(
        service_account.Credentials,
        "from_service_account_file",
        return_value=MagicMock(spec=service_account.Credentials),
    ):
        manager = _make_manager(config, MagicMock(spec=UserCredentials))

    assert manager.local_credential_warning() is None

"""
Credentials used by the cloud_tasks runner itself when talking to GCP.
"""

import logging
from typing import Any, NamedTuple

from google.auth import default as get_default_credentials
from google.auth import impersonated_credentials
from google.oauth2 import credentials as oauth2_credentials
from google.oauth2 import service_account

from .config import GCPConfig

LOGGER = logging.getLogger(__name__)

#: Everything the runner does - Compute, Pub/Sub, Monitoring, the pricing catalog - is
#: covered by this one scope, and an impersonated token has to be asked for by scope.
CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


class RunnerCredentials(NamedTuple):
    """The credentials the runner makes its own API calls with.

    Attributes:
        credentials: What to hand to each Google client library.
        project_id: The project the credentials came with, if they came with one.
        source_is_personal: Whether the credentials these are built on belong to a person
            rather than to a service, and so expire while a long job is still running.
        impersonated_service_account: The service account being impersonated, or None.
    """

    credentials: Any
    project_id: str | None
    source_is_personal: bool
    impersonated_service_account: str | None


def load_runner_credentials(gcp_config: GCPConfig) -> RunnerCredentials:
    """Build the credentials the runner should use, following the GCP configuration.

    A credentials_file is used directly. Otherwise the Application Default Credentials are
    used, which are a person's own login when they come from "gcloud auth
    application-default login". Either way, if runner_service_account names a service
    account, the result impersonates it, so that every call the runner makes is made as
    that account and is allowed exactly what that account is allowed.

    Impersonation does not make personal credentials last longer: the impersonated token is
    refreshed with the credentials underneath it, so a personal login that expires takes
    the impersonation with it. source_is_personal reports on those underlying credentials.

    Parameters:
        gcp_config: GCP configuration; credentials_file and runner_service_account are used

    Returns:
        RunnerCredentials: The credentials and what is known about them.

    Raises:
        RuntimeError: If the credentials file cannot be loaded, if there are no default
            credentials, or if the named service account cannot be impersonated.
    """
    project_id: str | None = None
    source_is_personal = False

    if gcp_config.credentials_file:
        try:
            credentials: Any = service_account.Credentials.from_service_account_file(
                gcp_config.credentials_file, scopes=[CLOUD_PLATFORM_SCOPE]
            )
            LOGGER.debug(f"Using credentials from file: {gcp_config.credentials_file}")
        except Exception as e:
            raise RuntimeError(
                f"Error loading credentials file: {gcp_config.credentials_file}: {e}"
            )
    else:
        try:
            credentials, project_id = get_default_credentials(scopes=[CLOUD_PLATFORM_SCOPE])
            LOGGER.debug("Using default application credentials")
        except Exception as e:
            raise RuntimeError(
                f"Error getting default credentials: {e}. "
                "Please ensure you're authenticated with 'gcloud auth application-default "
                "login' or provide a credentials_file entry in the GCP configuration."
            )
        # Application Default Credentials are a person's own login when they come from
        # "gcloud auth application-default login"; every other kind (a service account key,
        # the metadata server on a GCE instance, workload identity federation) belongs to a
        # service and keeps working unattended.
        source_is_personal = isinstance(credentials, oauth2_credentials.Credentials)

    runner_service_account = gcp_config.runner_service_account
    if runner_service_account:
        LOGGER.info(f'Running as service account "{runner_service_account}"')
        try:
            credentials = impersonated_credentials.Credentials(
                source_credentials=credentials,
                target_principal=runner_service_account,
                target_scopes=[CLOUD_PLATFORM_SCOPE],
            )
        except Exception as e:
            raise RuntimeError(
                f'Error impersonating service account "{runner_service_account}": {e}. '
                "The credentials this is running on need the role "
                '"roles/iam.serviceAccountTokenCreator" on that service account.'
            )

    return RunnerCredentials(
        credentials=credentials,
        project_id=project_id,
        source_is_personal=source_is_personal,
        impersonated_service_account=runner_service_account,
    )

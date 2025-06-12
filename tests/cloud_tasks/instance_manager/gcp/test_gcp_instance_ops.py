"""Unit tests for the GCP Compute Engine instance manager."""

import asyncio
import copy
from unittest.mock import MagicMock, patch, AsyncMock

import pytest
from google.api_core.exceptions import NotFound  # type: ignore
import uuid as _uuid  # Import uuid module with alias to avoid conflicts

from cloud_tasks.instance_manager.gcp import GCPComputeInstanceManager

from .conftest import deepcopy_gcp_instance_manager


@pytest.mark.asyncio
async def test_start_instance_basic(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test starting a basic instance with minimal parameters."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Arrange
    instance_type = "n1-standard-2"
    boot_disk_type = "pd-balanced"
    startup_script = "#!/bin/bash\necho 'Hello World'"
    job_id = "test-job-123"
    use_spot = False
    image = "ubuntu-2404-lts"

    # Mock the UUID generation to have a predictable instance ID
    mock_uuid = _uuid.UUID("12345678-1234-5678-1234-567812345678")
    with patch("uuid.uuid4", return_value=mock_uuid):
        # Mock get_image_from_family to return a predictable image path
        with patch.object(
            gcp_instance_manager_n1_n2, "get_image_from_family", new=AsyncMock()
        ) as mock_get_image:
            mock_get_image.return_value = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-lts"

            # Mock the insert operation and its result
            mock_operation = MagicMock()
            mock_operation.name = "mock-operation-name"
            mock_operation.error_code = None
            mock_operation.warnings = None
            mock_result = MagicMock()
            mock_operation.result.return_value = mock_result

            mock_compute_client = MagicMock()
            mock_compute_client.insert = MagicMock(return_value=mock_operation)

            with patch("google.cloud.compute_v1.InstancesClient", return_value=mock_compute_client):
                # Mock _wait_for_operation to return successfully
                with patch.object(
                    gcp_instance_manager_n1_n2, "_wait_for_operation", new=AsyncMock()
                ) as mock_wait:
                    mock_wait.return_value = mock_result

                    # Act
                    instance_id, zone = await gcp_instance_manager_n1_n2.start_instance(
                        instance_type=instance_type,
                        boot_disk_size=20,
                        boot_disk_type=boot_disk_type,
                        startup_script=startup_script,
                        job_id=job_id,
                        use_spot=use_spot,
                        image_uri=image,
                        zone=gcp_instance_manager_n1_n2._zone,
                    )

                    # Assert
                    assert instance_id.startswith(
                        f"{gcp_instance_manager_n1_n2._JOB_ID_TAG_PREFIX}{job_id}-"
                    )
                    assert zone == gcp_instance_manager_n1_n2._zone

                    # Check that the compute client was called with the correct parameters
                    mock_compute_client.insert.assert_called_once()
                    call_args = mock_compute_client.insert.call_args
                    assert call_args[1]["project"] == gcp_instance_manager_n1_n2._project_id
                    assert call_args[1]["zone"] == gcp_instance_manager_n1_n2._zone

                    # Check instance resource configuration
                    instance_config = call_args[1]["instance_resource"]
                    assert instance_config.name == instance_id
                    assert (
                        instance_config.machine_type
                        == f"zones/{gcp_instance_manager_n1_n2._zone}/machineTypes/{instance_type}"
                    )

                    # Check metadata (startup script)
                    assert instance_config.metadata.items[0].key == "startup-script"
                    assert instance_config.metadata.items[0].value == startup_script

                    # Check scheduling (not preemptible)
                    assert not instance_config.scheduling.preemptible

                    # Verify tags for job identification
                    assert instance_config.tags.items == [
                        gcp_instance_manager_n1_n2._job_id_to_tag(job_id)
                    ]

                    # Verify wait_for_operation was called
                    mock_wait.assert_called_once()


@pytest.mark.asyncio
async def test_start_instance_spot(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test starting a spot instance."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Arrange
    instance_type = "n1-standard-2"
    boot_disk_type = "pd-balanced"
    startup_script = "#!/bin/bash\necho 'Hello World'"
    job_id = "test-job-123"
    use_spot = True
    image = "ubuntu-2404-lts"

    # Mock the UUID generation to have a predictable instance ID
    mock_uuid = _uuid.UUID("12345678-1234-5678-1234-567812345678")
    with patch("uuid.uuid4", return_value=mock_uuid):
        # Mock get_image_from_family to return a predictable image path
        with patch.object(
            gcp_instance_manager_n1_n2, "get_image_from_family", new=AsyncMock()
        ) as mock_get_image:
            mock_get_image.return_value = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-lts"

            # Mock the insert operation and its result
            mock_operation = MagicMock()
            mock_operation.name = "mock-operation-name"
            mock_operation.error_code = None
            mock_operation.warnings = None
            mock_result = MagicMock()
            mock_operation.result.return_value = mock_result

            mock_compute_client = MagicMock()
            mock_compute_client.insert = MagicMock(return_value=mock_operation)
            gcp_instance_manager_n1_n2._get_compute_client = MagicMock(
                return_value=mock_compute_client
            )

            # Mock _wait_for_operation to return successfully
            with patch.object(
                gcp_instance_manager_n1_n2, "_wait_for_operation", new=AsyncMock()
            ) as mock_wait:
                mock_wait.return_value = mock_result

                # Act
                instance_id, zone = await gcp_instance_manager_n1_n2.start_instance(
                    instance_type=instance_type,
                    boot_disk_size=20,
                    boot_disk_type=boot_disk_type,
                    startup_script=startup_script,
                    job_id=job_id,
                    use_spot=use_spot,
                    image_uri=image,
                    zone=gcp_instance_manager_n1_n2._zone,
                )

                # Assert
                assert instance_id.startswith(
                    f"{gcp_instance_manager_n1_n2._JOB_ID_TAG_PREFIX}{job_id}-"
                )
                assert zone == gcp_instance_manager_n1_n2._zone

                # Check that the compute client was called with the correct parameters
                mock_compute_client.insert.assert_called_once()
                call_args = mock_compute_client.insert.call_args

                # Check that spot scheduling was used
                instance_config = call_args[1]["instance_resource"]
                assert instance_config.scheduling.preemptible is True
                assert instance_config.scheduling.automatic_restart is False
                assert instance_config.scheduling.on_host_maintenance == "TERMINATE"


@pytest.mark.asyncio
async def test_start_instance_with_service_account(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test starting an instance with a service account."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    instance_type = "n1-standard-2"
    boot_disk_type = "pd-balanced"
    startup_script = "#!/bin/bash\necho 'Hello World'"
    job_id = "test-job-123"
    use_spot = False
    image = "ubuntu-2404-lts"

    # Set a service account for the instance
    service_account = "test-service-account@test-project.iam.gserviceaccount.com"
    gcp_instance_manager_n1_n2._service_account = service_account

    # Mock the UUID generation to have a predictable instance ID
    mock_uuid = _uuid.UUID("12345678-1234-5678-1234-567812345678")
    with patch("uuid.uuid4", return_value=mock_uuid):
        # Mock get_image_from_family to return a predictable image path
        with patch.object(
            gcp_instance_manager_n1_n2, "get_image_from_family", new=AsyncMock()
        ) as mock_get_image:
            mock_get_image.return_value = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-lts"

            # Mock the insert operation and its result
            mock_operation = MagicMock()
            mock_operation.name = "mock-operation-name"
            mock_operation.error_code = None
            mock_operation.warnings = None
            mock_result = MagicMock()
            mock_operation.result.return_value = mock_result

            mock_compute_client = MagicMock()
            mock_compute_client.insert = MagicMock(return_value=mock_operation)
            gcp_instance_manager_n1_n2._get_compute_client = MagicMock(
                return_value=mock_compute_client
            )

            # Mock _wait_for_operation to return successfully
            with patch.object(
                gcp_instance_manager_n1_n2, "_wait_for_operation", new=AsyncMock()
            ) as mock_wait:
                mock_wait.return_value = mock_result

                # Act
                instance_id, zone = await gcp_instance_manager_n1_n2.start_instance(
                    instance_type=instance_type,
                    boot_disk_size=20,
                    boot_disk_type=boot_disk_type,
                    startup_script=startup_script,
                    job_id=job_id,
                    use_spot=use_spot,
                    image_uri=image,
                    zone=gcp_instance_manager_n1_n2._zone,
                )

                # Assert
                assert instance_id.startswith(
                    f"{gcp_instance_manager_n1_n2._JOB_ID_TAG_PREFIX}{job_id}-"
                )
                assert zone == gcp_instance_manager_n1_n2._zone

                # Check that the service account was included in the instance configuration
                call_args = mock_compute_client.insert.call_args
                instance_config = call_args[1]["instance_resource"]

                assert len(instance_config.service_accounts) == 1
                assert instance_config.service_accounts[0].email == service_account
                assert instance_config.service_accounts[0].scopes == [
                    "https://www.googleapis.com/auth/cloud-platform"
                ]


@pytest.mark.asyncio
async def test_start_instance_with_custom_image_uri(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test starting an instance with a custom image URI."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    instance_type = "n1-standard-2"
    boot_disk_type = "pd-balanced"
    startup_script = "#!/bin/bash\necho 'Hello World'"
    job_id = "test-job-123"
    use_spot = False
    custom_image = "https://compute.googleapis.com/compute/v1/projects/my-project/global/images/my-custom-image"

    # Mock the UUID generation to have a predictable instance ID
    mock_uuid = _uuid.UUID("12345678-1234-5678-1234-567812345678")
    with patch("uuid.uuid4", return_value=mock_uuid):
        # Mock the insert operation and its result
        mock_operation = MagicMock()
        mock_operation.name = "mock-operation-name"
        mock_operation.error_code = None
        mock_operation.warnings = None
        mock_result = MagicMock()
        mock_operation.result.return_value = mock_result

        mock_compute_client = MagicMock()
        mock_compute_client.insert = MagicMock(return_value=mock_operation)
        gcp_instance_manager_n1_n2._get_compute_client = MagicMock(return_value=mock_compute_client)

        # Mock _wait_for_operation to return successfully
        with patch.object(
            gcp_instance_manager_n1_n2, "_wait_for_operation", new=AsyncMock()
        ) as mock_wait:
            mock_wait.return_value = mock_result

            # Act
            instance_id, zone = await gcp_instance_manager_n1_n2.start_instance(
                instance_type=instance_type,
                boot_disk_size=20,
                boot_disk_type=boot_disk_type,
                startup_script=startup_script,
                job_id=job_id,
                use_spot=use_spot,
                image_uri=custom_image,
                zone=gcp_instance_manager_n1_n2._zone,
            )

            # Assert
            assert instance_id.startswith(
                f"{gcp_instance_manager_n1_n2._JOB_ID_TAG_PREFIX}{job_id}-"
            )
            assert zone == gcp_instance_manager_n1_n2._zone

            # Check that the image was set correctly in the instance configuration
            call_args = mock_compute_client.insert.call_args
            instance_config = call_args[1]["instance_resource"]

            assert instance_config.disks[0].initialize_params.source_image == custom_image
            # get_image_from_family should not have been called since we provided a full image URI


@pytest.mark.asyncio
async def test_start_instance_with_random_zone(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test starting an instance with a wildcard zone that needs to be randomly selected."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    instance_type = "n1-standard-2"
    boot_disk_type = "pd-balanced"
    startup_script = "#!/bin/bash\necho 'Hello World'"
    job_id = "test-job-123"
    use_spot = False
    image = "ubuntu-2404-lts"
    wildcard_zone = "us-central1-*"  # Wildcard zone

    # Mock _get_random_zone to return a specific zone
    with patch.object(
        gcp_instance_manager_n1_n2, "_get_random_zone", new=AsyncMock()
    ) as mock_get_random_zone:
        random_zone = "us-central1-c"
        mock_get_random_zone.return_value = random_zone

        # Mock the UUID generation to have a predictable instance ID
        mock_uuid = _uuid.UUID("12345678-1234-5678-1234-567812345678")
        with patch("uuid.uuid4", return_value=mock_uuid):
            # Mock get_image_from_family to return a predictable image path
            with patch.object(
                gcp_instance_manager_n1_n2, "get_image_from_family", new=AsyncMock()
            ) as mock_get_image:
                mock_get_image.return_value = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-lts"

                # Mock the insert operation and its result
                mock_operation = MagicMock()
                mock_operation.name = "mock-operation-name"
                mock_operation.error_code = None
                mock_operation.warnings = None
                mock_result = MagicMock()
                mock_operation.result.return_value = mock_result

                mock_compute_client = MagicMock()
                mock_compute_client.insert = MagicMock(return_value=mock_operation)
                gcp_instance_manager_n1_n2._get_compute_client = MagicMock(
                    return_value=mock_compute_client
                )

                # Mock _wait_for_operation to return successfully
                with patch.object(
                    gcp_instance_manager_n1_n2, "_wait_for_operation", new=AsyncMock()
                ) as mock_wait:
                    mock_wait.return_value = mock_result

                    # Act
                    instance_id, zone = await gcp_instance_manager_n1_n2.start_instance(
                        instance_type=instance_type,
                        boot_disk_size=20,
                        boot_disk_type=boot_disk_type,
                        startup_script=startup_script,
                        job_id=job_id,
                        use_spot=use_spot,
                        image_uri=image,
                        zone=wildcard_zone,
                    )

                    # Assert
                    assert instance_id.startswith(
                        f"{gcp_instance_manager_n1_n2._JOB_ID_TAG_PREFIX}{job_id}-"
                    )
                    assert zone == random_zone

                    # Verify that _get_random_zone was called to resolve the wildcard
                    mock_get_random_zone.assert_called_once()

                    # Check that the resolved random zone was used
                    call_args = mock_compute_client.insert.call_args
                    assert call_args[1]["zone"] == random_zone


@pytest.mark.asyncio
async def test_start_instance_error_handling(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test error handling when starting an instance fails."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    instance_type = "n1-standard-2"
    boot_disk_type = "pd-balanced"
    startup_script = "#!/bin/bash\necho 'Hello World'"
    job_id = "test-job-123"
    use_spot = False
    image = "ubuntu-2404-lts"

    # Mock the UUID generation to have a predictable instance ID
    mock_uuid = _uuid.UUID("12345678-1234-5678-1234-567812345678")
    with patch("uuid.uuid4", return_value=mock_uuid):
        # Mock get_image_from_family to return a predictable image path
        with patch.object(
            gcp_instance_manager_n1_n2, "get_image_from_family", new=AsyncMock()
        ) as mock_get_image:
            mock_get_image.return_value = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-lts"

            # Mock the insert operation to raise an exception
            error_message = "Failed to create instance"
            mock_compute_client = MagicMock()
            mock_compute_client.insert = MagicMock(side_effect=RuntimeError(error_message))
            gcp_instance_manager_n1_n2._get_compute_client = MagicMock(
                return_value=mock_compute_client
            )

            # Act & Assert
            with pytest.raises(RuntimeError, match=error_message):
                await gcp_instance_manager_n1_n2.start_instance(
                    instance_type=instance_type,
                    boot_disk_size=20,
                    boot_disk_type=boot_disk_type,
                    startup_script=startup_script,
                    job_id=job_id,
                    use_spot=use_spot,
                    image_uri=image,
                    zone=gcp_instance_manager_n1_n2._zone,
                )


@pytest.mark.asyncio
async def test_terminate_instance_basic(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test terminating an instance with successful operation."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    instance_id = "test-instance-123"

    # Mock the delete operation and its result
    mock_operation = MagicMock()
    mock_operation.name = "mock-operation-name"
    mock_operation.error_code = None
    mock_operation.warnings = None
    mock_result = MagicMock()
    mock_operation.result.return_value = mock_result

    mock_compute_client = MagicMock()
    mock_compute_client.delete = MagicMock(return_value=mock_operation)
    gcp_instance_manager_n1_n2._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Mock _wait_for_operation to return successfully
    with patch.object(
        gcp_instance_manager_n1_n2, "_wait_for_operation", new=AsyncMock()
    ) as mock_wait:
        mock_wait.return_value = mock_result

        # Act
        await gcp_instance_manager_n1_n2.terminate_instance(instance_id)

        # Assert
        # Check that the compute client was called with the correct parameters
        mock_compute_client.delete.assert_called_once_with(
            project=gcp_instance_manager_n1_n2._project_id,
            zone=gcp_instance_manager_n1_n2._zone,
            instance=instance_id,
        )

        # Verify _wait_for_operation was called with the correct arguments
        mock_wait.assert_called_once_with(
            mock_operation,  # Pass the operation object, not just its name
            f"Termination of instance {instance_id}",
        )


@pytest.mark.asyncio
async def test_terminate_instance_not_found(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test terminating an instance that doesn't exist."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    instance_id = "nonexistent-instance"

    # Mock the delete method to raise NotFound exception
    mock_compute_client = MagicMock()
    mock_compute_client.delete = MagicMock(side_effect=NotFound("Instance not found"))
    gcp_instance_manager_n1_n2._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act
    try:
        await gcp_instance_manager_n1_n2.terminate_instance(instance_id)
    except NotFound:
        pass
    except Exception as e:
        raise e

    # Assert
    # Check that the compute client was called with the correct parameters
    mock_compute_client.delete.assert_called_once_with(
        project=gcp_instance_manager_n1_n2._project_id,
        zone=gcp_instance_manager_n1_n2._zone,
        instance=instance_id,
    )
    # Note: No exception should be raised as the method handles NotFound gracefully


@pytest.mark.asyncio
async def test_terminate_instance_error_handling(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test error handling when terminating an instance fails."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    instance_id = "test-instance-123"

    # Mock the delete method to raise an exception
    error_message = "Failed to terminate instance"
    mock_compute_client = MagicMock()
    mock_compute_client.delete = MagicMock(side_effect=RuntimeError(error_message))
    gcp_instance_manager_n1_n2._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act & Assert
    with pytest.raises(RuntimeError, match=error_message):
        await gcp_instance_manager_n1_n2.terminate_instance(instance_id)

    # Verify the method was called with correct parameters
    mock_compute_client.delete.assert_called_once_with(
        project=gcp_instance_manager_n1_n2._project_id,
        zone=gcp_instance_manager_n1_n2._zone,
        instance=instance_id,
    )


@pytest.mark.asyncio
async def test_list_running_instances_basic(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test listing running instances with no filters."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_instance1 = MagicMock()
    mock_instance1.name = "instance-1"
    mock_instance1.machine_type = "zones/us-central1-a/machineTypes/n1-standard-2"
    mock_instance1.status = "RUNNING"
    mock_instance1.creation_timestamp = "2024-03-20T10:00:00.000-07:00"
    mock_instance1.zone = "zones/us-central1-a"
    mock_instance1.tags = MagicMock()
    mock_instance1.tags.items = ["rmscr-job-123"]
    mock_instance1.network_interfaces = [
        MagicMock(network_i_p="10.0.0.2", access_configs=[MagicMock(nat_i_p="34.123.123.123")])
    ]

    mock_instance2 = MagicMock()
    mock_instance2.name = "instance-2"
    mock_instance2.machine_type = "zones/us-central1-a/machineTypes/n2-standard-4"
    mock_instance2.status = "RUNNING"
    mock_instance2.creation_timestamp = "2024-03-20T11:00:00.000-07:00"
    mock_instance2.zone = "zones/us-central1-a"
    mock_instance2.tags = MagicMock()
    mock_instance2.tags.items = ["rmscr-job-456"]
    mock_instance2.network_interfaces = [
        MagicMock(network_i_p="10.0.0.3", access_configs=[MagicMock(nat_i_p="34.123.123.124")])
    ]

    # Mock the compute client's list method
    mock_compute_client = MagicMock()
    mock_compute_client.list.return_value = [mock_instance1, mock_instance2]
    gcp_instance_manager_n1_n2._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act
    instances = await gcp_instance_manager_n1_n2.list_running_instances()

    # Assert
    assert len(instances) == 2

    # Check first instance
    assert instances[0]["id"] == "instance-1"
    assert instances[0]["type"] == "n1-standard-2"
    assert instances[0]["state"] == "running"
    assert instances[0]["zone"] == "us-central1-a"
    assert instances[0]["job_id"] == "job-123"
    assert instances[0]["private_ip"] == "10.0.0.2"
    assert instances[0]["public_ip"] == "34.123.123.123"

    # Check second instance
    assert instances[1]["id"] == "instance-2"
    assert instances[1]["type"] == "n2-standard-4"
    assert instances[1]["state"] == "running"
    assert instances[1]["zone"] == "us-central1-a"
    assert instances[1]["job_id"] == "job-456"
    assert instances[1]["private_ip"] == "10.0.0.3"
    assert instances[1]["public_ip"] == "34.123.123.124"


@pytest.mark.asyncio
async def test_list_running_instances_with_job_id(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test listing instances filtered by job ID."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_instance1 = MagicMock()
    mock_instance1.name = "instance-1"
    mock_instance1.machine_type = "zones/us-central1-a/machineTypes/n1-standard-2"
    mock_instance1.status = "RUNNING"
    mock_instance1.creation_timestamp = "2024-03-20T10:00:00.000-07:00"
    mock_instance1.zone = "zones/us-central1-a"
    mock_instance1.tags = MagicMock()
    mock_instance1.tags.items = ["rmscr-job-123"]
    mock_instance1.network_interfaces = [
        MagicMock(network_i_p="10.0.0.2", access_configs=[MagicMock(nat_i_p="34.123.123.123")])
    ]

    mock_instance2 = MagicMock()
    mock_instance2.name = "instance-2"
    mock_instance2.machine_type = "zones/us-central1-a/machineTypes/n2-standard-4"
    mock_instance2.status = "RUNNING"
    mock_instance2.creation_timestamp = "2024-03-20T11:00:00.000-07:00"
    mock_instance2.zone = "zones/us-central1-a"
    mock_instance2.tags = MagicMock()
    mock_instance2.tags.items = ["rmscr-job-456"]
    mock_instance2.network_interfaces = [
        MagicMock(network_i_p="10.0.0.3", access_configs=[MagicMock(nat_i_p="34.123.123.124")])
    ]

    # Mock the compute client's list method
    mock_compute_client = MagicMock()
    mock_compute_client.list.return_value = [mock_instance1, mock_instance2]
    gcp_instance_manager_n1_n2._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act
    instances = await gcp_instance_manager_n1_n2.list_running_instances(job_id="job-123")

    # Assert
    assert len(instances) == 1
    assert instances[0]["id"] == "instance-1"
    assert instances[0]["job_id"] == "job-123"


@pytest.mark.asyncio
async def test_list_running_instances_include_non_job(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test listing instances including non-job instances."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_instance1 = MagicMock()
    mock_instance1.name = "instance-1"
    mock_instance1.machine_type = "zones/us-central1-a/machineTypes/n1-standard-2"
    mock_instance1.status = "RUNNING"
    mock_instance1.creation_timestamp = "2024-03-20T10:00:00.000-07:00"
    mock_instance1.zone = "zones/us-central1-a"
    mock_instance1.tags = MagicMock()
    mock_instance1.tags.items = ["rmscr-job-123"]
    mock_instance1.network_interfaces = [
        MagicMock(network_i_p="10.0.0.2", access_configs=[MagicMock(nat_i_p="34.123.123.123")])
    ]

    mock_instance2 = MagicMock()
    mock_instance2.name = "instance-2"
    mock_instance2.machine_type = "zones/us-central1-a/machineTypes/n2-standard-4"
    mock_instance2.status = "RUNNING"
    mock_instance2.creation_timestamp = "2024-03-20T11:00:00.000-07:00"
    mock_instance2.zone = "zones/us-central1-a"
    mock_instance2.tags = MagicMock()
    mock_instance2.tags.items = []  # No job tag
    mock_instance2.network_interfaces = [
        MagicMock(network_i_p="10.0.0.3", access_configs=[MagicMock(nat_i_p="34.123.123.124")])
    ]

    # Mock the compute client's list method
    mock_compute_client = MagicMock()
    mock_compute_client.list.return_value = [mock_instance1, mock_instance2]
    gcp_instance_manager_n1_n2._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act
    instances = await gcp_instance_manager_n1_n2.list_running_instances(include_non_job=True)

    # Assert
    assert len(instances) == 2
    assert instances[0]["id"] == "instance-1"
    assert instances[0]["job_id"] == "job-123"
    assert "job_id" not in instances[1]
    assert instances[1]["id"] == "instance-2"


@pytest.mark.asyncio
async def test_list_running_instances_region_based(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test listing instances across all zones in a region when no specific zone is set."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Clear the zone to force region-based listing
    gcp_instance_manager_n1_n2._zone = None

    # Mock zones in the region
    mock_zone1 = MagicMock()
    mock_zone1.name = "us-central1-a"
    mock_zone2 = MagicMock()
    mock_zone2.name = "us-central1-b"

    # Mock the zones client to return our mock zones
    gcp_instance_manager_n1_n2._zones_client.list.return_value = [mock_zone1, mock_zone2]

    # Create mock instances for each zone
    mock_instance1 = MagicMock()
    mock_instance1.name = "instance-1"
    mock_instance1.machine_type = "zones/us-central1-a/machineTypes/n1-standard-2"
    mock_instance1.status = "RUNNING"
    mock_instance1.creation_timestamp = "2024-03-20T10:00:00.000-07:00"
    mock_instance1.zone = "zones/us-central1-a"
    mock_instance1.tags = MagicMock()
    mock_instance1.tags.items = ["rmscr-job-123"]
    mock_instance1.network_interfaces = [
        MagicMock(network_i_p="10.0.0.2", access_configs=[MagicMock(nat_i_p="34.123.123.123")])
    ]

    mock_instance2 = MagicMock()
    mock_instance2.name = "instance-2"
    mock_instance2.machine_type = "zones/us-central1-b/machineTypes/n2-standard-4"
    mock_instance2.status = "RUNNING"
    mock_instance2.creation_timestamp = "2024-03-20T11:00:00.000-07:00"
    mock_instance2.zone = "zones/us-central1-b"
    mock_instance2.tags = MagicMock()
    mock_instance2.tags.items = ["rmscr-job-456"]
    mock_instance2.network_interfaces = [
        MagicMock(network_i_p="10.0.0.3", access_configs=[MagicMock(nat_i_p="34.123.123.124")])
    ]

    # Mock the compute client to return different instances for each zone
    mock_compute_client = MagicMock()
    mock_compute_client.list = MagicMock(side_effect=[[mock_instance1], [mock_instance2]])
    gcp_instance_manager_n1_n2._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act
    instances = await gcp_instance_manager_n1_n2.list_running_instances()

    # Assert
    assert len(instances) == 2
    assert instances[0]["id"] == "instance-1"
    assert instances[0]["zone"] == "us-central1-a"
    assert instances[1]["id"] == "instance-2"
    assert instances[1]["zone"] == "us-central1-b"

    # Verify that zones were listed
    gcp_instance_manager_n1_n2._zones_client.list.assert_called_once()
    # Verify that instances were listed in each zone
    assert mock_compute_client.list.call_count == 2


@pytest.mark.asyncio
async def test_list_running_instances_zone_listing_error(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test error handling when listing zones fails."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    gcp_instance_manager_n1_n2._zone = None  # Force region-based listing
    error_msg = "Permission denied"
    gcp_instance_manager_n1_n2._zones_client.list = MagicMock(side_effect=RuntimeError(error_msg))

    # Act & Assert
    with pytest.raises(ValueError, match=f"Error listing zones.*{error_msg}"):
        await gcp_instance_manager_n1_n2.list_running_instances()


@pytest.mark.asyncio
async def test_list_running_instances_instance_listing_error(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test handling when listing instances in a zone fails."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    gcp_instance_manager_n1_n2._zone = None  # Force region-based listing

    # Mock zones in the region
    mock_zone1 = MagicMock()
    mock_zone1.name = "us-central1-a"
    mock_zone2 = MagicMock()
    mock_zone2.name = "us-central1-b"
    gcp_instance_manager_n1_n2._zones_client.list.return_value = [mock_zone1, mock_zone2]

    # Mock instance in first zone
    mock_instance1 = MagicMock()
    mock_instance1.name = "instance-1"
    mock_instance1.machine_type = "zones/us-central1-a/machineTypes/n1-standard-2"
    mock_instance1.status = "RUNNING"
    mock_instance1.creation_timestamp = "2024-03-20T10:00:00.000-07:00"
    mock_instance1.zone = "zones/us-central1-a"
    mock_instance1.tags = MagicMock()
    mock_instance1.tags.items = ["rmscr-job-123"]
    mock_instance1.network_interfaces = [
        MagicMock(network_i_p="10.0.0.2", access_configs=[MagicMock(nat_i_p="34.123.123.123")])
    ]

    # Mock the compute client to succeed for first zone but fail for second
    mock_compute_client = MagicMock()
    mock_compute_client.list = MagicMock(
        side_effect=[[mock_instance1], RuntimeError("Failed to list instances")]
    )
    gcp_instance_manager_n1_n2._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act
    instances = await gcp_instance_manager_n1_n2.list_running_instances()

    # Assert
    # Should still get instances from the successful zone
    assert len(instances) == 1
    assert instances[0]["id"] == "instance-1"
    assert instances[0]["zone"] == "us-central1-a"


@pytest.mark.asyncio
async def test_list_running_instances_unknown_status(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test handling instances with unknown status."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_instance = MagicMock()
    mock_instance.name = "instance-1"
    mock_instance.machine_type = "zones/us-central1-a/machineTypes/n1-standard-2"
    mock_instance.status = "UNKNOWN_STATUS"  # Status not in _STATUS_MAP
    mock_instance.creation_timestamp = "2024-03-20T10:00:00.000-07:00"
    mock_instance.zone = "zones/us-central1-a"
    mock_instance.tags = MagicMock()
    mock_instance.tags.items = ["rmscr-job-123"]
    mock_instance.network_interfaces = [
        MagicMock(network_i_p="10.0.0.2", access_configs=[MagicMock(nat_i_p="34.123.123.123")])
    ]

    # Mock the compute client
    mock_compute_client = MagicMock()
    mock_compute_client.list.return_value = [mock_instance]
    gcp_instance_manager_n1_n2._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act
    instances = await gcp_instance_manager_n1_n2.list_running_instances()

    # Assert
    assert len(instances) == 1
    assert instances[0]["id"] == "instance-1"
    assert instances[0]["state"] == "unknown"


@pytest.mark.asyncio
async def test_list_running_instances_no_network_interfaces(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test handling instances with no network interfaces."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_instance = MagicMock()
    mock_instance.name = "instance-1"
    mock_instance.machine_type = "zones/us-central1-a/machineTypes/n1-standard-2"
    mock_instance.status = "RUNNING"
    mock_instance.creation_timestamp = "2024-03-20T10:00:00.000-07:00"
    mock_instance.zone = "zones/us-central1-a"
    mock_instance.tags = MagicMock()
    mock_instance.tags.items = ["rmscr-job-123"]
    mock_instance.network_interfaces = []  # No network interfaces

    # Mock the compute client
    mock_compute_client = MagicMock()
    mock_compute_client.list.return_value = [mock_instance]
    gcp_instance_manager_n1_n2._get_compute_client = MagicMock(return_value=mock_compute_client)

    # Act
    instances = await gcp_instance_manager_n1_n2.list_running_instances()

    # Assert
    assert len(instances) == 1
    assert instances[0]["id"] == "instance-1"
    assert "private_ip" not in instances[0]
    assert "public_ip" not in instances[0]


@pytest.mark.asyncio
async def test_wait_for_operation_success(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test successful operation completion."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_operation = MagicMock()
    mock_operation.name = "mock-operation-name"
    mock_operation.error_code = None
    mock_operation.warnings = None
    mock_result = MagicMock()
    mock_operation.result.return_value = mock_result

    # Act
    result = await gcp_instance_manager_n1_n2._wait_for_operation(mock_operation, "Test operation")

    # Assert
    assert result == mock_result
    mock_operation.result.assert_called_once_with(
        timeout=gcp_instance_manager_n1_n2._DEFAULT_OPERATION_TIMEOUT
    )


@pytest.mark.asyncio
async def test_wait_for_operation_with_error(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test operation that fails with an error."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_operation = MagicMock()
    mock_operation.name = "mock-operation-name"
    mock_operation.error_code = "RESOURCE_NOT_FOUND"
    mock_operation.error_message = "The resource was not found"
    mock_operation.exception = MagicMock(return_value=RuntimeError("Resource not found"))

    # Act & Assert
    with pytest.raises(RuntimeError, match="Resource not found"):
        await gcp_instance_manager_n1_n2._wait_for_operation(mock_operation, "Test operation")

    mock_operation.result.assert_called_once_with(
        timeout=gcp_instance_manager_n1_n2._DEFAULT_OPERATION_TIMEOUT
    )


@pytest.mark.asyncio
async def test_wait_for_operation_with_warnings(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test operation that completes with warnings."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_operation = MagicMock()
    mock_operation.name = "mock-operation-name"
    mock_operation.error_code = None

    # Create mock warnings
    mock_warning1 = MagicMock()
    mock_warning1.code = "QUOTA_WARNING"
    mock_warning1.message = "Approaching quota limit"

    mock_warning2 = MagicMock()
    mock_warning2.code = "PERFORMANCE_WARNING"
    mock_warning2.message = "Instance may experience degraded performance"

    mock_operation.warnings = [mock_warning1, mock_warning2]

    mock_result = MagicMock()
    mock_operation.result.return_value = mock_result

    # Act
    result = await gcp_instance_manager_n1_n2._wait_for_operation(mock_operation, "Test operation")

    # Assert
    assert result == mock_result
    mock_operation.result.assert_called_once_with(
        timeout=gcp_instance_manager_n1_n2._DEFAULT_OPERATION_TIMEOUT
    )


@pytest.mark.asyncio
async def test_wait_for_operation_timeout(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test operation that times out."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    mock_operation = MagicMock()
    mock_operation.name = "mock-operation-name"
    mock_operation.result = MagicMock(side_effect=TimeoutError("Operation timed out"))

    # Act & Assert
    with pytest.raises(TimeoutError, match="Operation timed out"):
        await gcp_instance_manager_n1_n2._wait_for_operation(mock_operation, "Test operation")

    mock_operation.result.assert_called_once_with(
        timeout=gcp_instance_manager_n1_n2._DEFAULT_OPERATION_TIMEOUT
    )


@pytest.mark.asyncio
async def test_wait_for_operation_cancellation(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test operation that gets cancelled."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Arrange
    mock_operation = MagicMock()
    mock_operation.name = "mock-operation-name"
    mock_operation.result = MagicMock(side_effect=asyncio.CancelledError())

    # Act & Assert
    with pytest.raises(asyncio.CancelledError):
        await gcp_instance_manager_n1_n2._wait_for_operation(mock_operation, "Test operation")

    mock_operation.result.assert_called_once_with(
        timeout=gcp_instance_manager_n1_n2._DEFAULT_OPERATION_TIMEOUT
    )

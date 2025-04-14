from google.api_core.exceptions import NotFound  # type: ignore


@pytest.mark.asyncio
async def test_terminate_instance_basic(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test terminating an instance with successful operation."""
    # Arrange
    instance_id = "test-instance-123"

    # Mock the delete operation and its result
    mock_operation = MagicMock()
    mock_operation.name = "mock-operation-name"
    mock_operation.error_code = None
    mock_operation.warnings = None
    mock_result = MagicMock()
    mock_operation.result.return_value = mock_result

    gcp_instance_manager._compute_client.delete = MagicMock(return_value=mock_operation)

    # Mock _wait_for_operation to return successfully
    with patch.object(gcp_instance_manager, "_wait_for_operation", new=AsyncMock()) as mock_wait:
        mock_wait.return_value = mock_result

        # Act
        await gcp_instance_manager.terminate_instance(instance_id)

        # Assert
        # Check that the compute client was called with the correct parameters
        gcp_instance_manager._compute_client.delete.assert_called_once_with(
            project=gcp_instance_manager._project_id,
            zone=gcp_instance_manager._zone,
            instance=instance_id,
        )

        # Verify _wait_for_operation was called with operation.name
        mock_wait.assert_called_once_with(mock_operation.name)


@pytest.mark.asyncio
async def test_terminate_instance_not_found(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test terminating an instance that doesn't exist."""
    # Arrange
    instance_id = "nonexistent-instance"

    # Mock the delete method to raise NotFound exception
    gcp_instance_manager._compute_client.delete = MagicMock(
        side_effect=NotFound("Instance not found")
    )

    # Act
    await gcp_instance_manager.terminate_instance(instance_id)

    # Assert
    # Check that the compute client was called with the correct parameters
    gcp_instance_manager._compute_client.delete.assert_called_once_with(
        project=gcp_instance_manager._project_id,
        zone=gcp_instance_manager._zone,
        instance=instance_id,
    )
    # Note: No exception should be raised as the method handles NotFound gracefully


@pytest.mark.asyncio
async def test_terminate_instance_error_handling(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test error handling when terminating an instance fails."""
    # Arrange
    instance_id = "test-instance-123"

    # Mock the delete method to raise an exception
    error_message = "Failed to terminate instance"
    gcp_instance_manager._compute_client.delete = MagicMock(side_effect=RuntimeError(error_message))

    # Act & Assert
    with pytest.raises(RuntimeError, match=error_message):
        await gcp_instance_manager.terminate_instance(instance_id)

    # Verify the method was called with correct parameters
    gcp_instance_manager._compute_client.delete.assert_called_once_with(
        project=gcp_instance_manager._project_id,
        zone=gcp_instance_manager._zone,
        instance=instance_id,
    )


@pytest.mark.asyncio
async def test_start_instance_basic(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test starting a basic instance with minimal parameters."""
    # Arrange
    instance_type = "n1-standard-2"
    startup_script = "#!/bin/bash\necho 'Hello World'"
    job_id = "test-job-123"
    use_spot = False
    image = "ubuntu-2404-lts"

    # Mock the UUID generation to have a predictable instance ID
    with patch("uuid.uuid4", return_value="mock-uuid"):
        # Mock _get_image_from_family to return a predictable image path
        with patch.object(
            gcp_instance_manager, "_get_image_from_family", new=AsyncMock()
        ) as mock_get_image:
            mock_get_image.return_value = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-lts"

            # Mock the insert operation and its result
            mock_operation = MagicMock()
            mock_operation.name = "mock-operation-name"
            mock_operation.error_code = None
            mock_operation.warnings = None
            mock_result = MagicMock()
            mock_operation.result.return_value = mock_result

            gcp_instance_manager._compute_client.insert = MagicMock(return_value=mock_operation)

            # Mock _wait_for_operation to return successfully
            with patch.object(
                gcp_instance_manager, "_wait_for_operation", new=AsyncMock()
            ) as mock_wait:
                mock_wait.return_value = mock_result

                # Act
                result = await gcp_instance_manager.start_instance(
                    instance_type=instance_type,
                    boot_disk_size=20,
                    startup_script=startup_script,
                    job_id=job_id,
                    use_spot=use_spot,
                    image=image,
                    zone=gcp_instance_manager._zone,
                )

                # Extract instance_id from result (which might be a tuple or just a string)
                instance_id = result[0] if isinstance(result, tuple) else result

                # Assert
                assert instance_id == f"{gcp_instance_manager._JOB_ID_TAG_PREFIX}{job_id}-mock-uuid"

                # Check that the compute client was called with the correct parameters
                gcp_instance_manager._compute_client.insert.assert_called_once()
                call_args = gcp_instance_manager._compute_client.insert.call_args
                assert call_args[1]["project"] == gcp_instance_manager._project_id
                assert call_args[1]["zone"] == gcp_instance_manager._zone

                # Check the instance resource configuration
                instance_config = call_args[1]["instance_resource"]
                assert (
                    instance_config["name"]
                    == f"{gcp_instance_manager._JOB_ID_TAG_PREFIX}{job_id}-mock-uuid"
                )
                assert (
                    instance_config["machine_type"]
                    == f"zones/{gcp_instance_manager._zone}/machineTypes/{instance_type}"
                )

                # Check metadata (startup script)
                assert instance_config["metadata"]["items"][0]["key"] == "startup-script"
                assert instance_config["metadata"]["items"][0]["value"] == startup_script

                # Check that on-demand scheduling was used (not spot)
                assert instance_config["scheduling"] == {}

                # Verify tags for job identification
                assert instance_config["tags"]["items"] == [
                    gcp_instance_manager._job_id_to_tag(job_id)
                ]

                # Verify wait_for_operation was called
                mock_wait.assert_called_once()


@pytest.mark.asyncio
async def test_start_instance_spot(
    gcp_instance_manager: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test starting a spot instance."""
    # Arrange
    instance_type = "n1-standard-2"
    startup_script = "#!/bin/bash\necho 'Hello World'"
    job_id = "test-job-123"
    use_spot = True
    image = "ubuntu-2404-lts"

    # Mock the UUID generation to have a predictable instance ID
    with patch("uuid.uuid4", return_value="mock-uuid"):
        # Mock _get_image_from_family to return a predictable image path
        with patch.object(
            gcp_instance_manager, "_get_image_from_family", new=AsyncMock()
        ) as mock_get_image:
            mock_get_image.return_value = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-lts"

            # Mock the insert operation and its result
            mock_operation = MagicMock()
            mock_operation.name = "mock-operation-name"
            mock_operation.error_code = None
            mock_operation.warnings = None
            mock_result = MagicMock()
            mock_operation.result.return_value = mock_result

            gcp_instance_manager._compute_client.insert = MagicMock(return_value=mock_operation)

            # Mock _wait_for_operation to return successfully
            with patch.object(
                gcp_instance_manager, "_wait_for_operation", new=AsyncMock()
            ) as mock_wait:
                mock_wait.return_value = mock_result

                # Act
                result = await gcp_instance_manager.start_instance(
                    instance_type=instance_type,
                    boot_disk_size=20,
                    startup_script=startup_script,
                    job_id=job_id,
                    use_spot=use_spot,
                    image=image,
                    zone=gcp_instance_manager._zone,
                )

                # Extract instance_id from result (which might be a tuple or just a string)
                instance_id = result[0] if isinstance(result, tuple) else result

                # Assert
                assert instance_id == f"{gcp_instance_manager._JOB_ID_TAG_PREFIX}{job_id}-mock-uuid"

                # Check that the compute client was called with the correct parameters
                gcp_instance_manager._compute_client.insert.assert_called_once()
                call_args = gcp_instance_manager._compute_client.insert.call_args

                # Check the instance resource configuration for spot instance
                instance_config = call_args[1]["instance_resource"]

                # Check that spot scheduling was used
                assert instance_config["scheduling"] == {
                    "preemptible": True,
                    "automatic_restart": False,
                    "on_host_maintenance": "TERMINATE",
                }

"""Unit tests for the GCP Compute Engine instance manager."""

import copy
from unittest.mock import MagicMock, AsyncMock

import pytest

from cloud_tasks.instance_manager.gcp import GCPComputeInstanceManager

from .conftest import deepcopy_gcp_instance_manager


@pytest.mark.asyncio
async def test_get_image_from_family(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test getting image from a family."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    gcp_instance_manager_n1_n2._credentials = mock_credentials
    family_name = "ubuntu-2404-lts"

    # Create mock image data
    mock_image_data = {
        "name": "ubuntu-2404-20240401",
        "family": family_name,
        "self_link": "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-20240401",
    }

    # Mock list_available_images to return our mock image
    gcp_instance_manager_n1_n2.list_available_images = AsyncMock(return_value=[mock_image_data])

    # Act
    image_uri = await gcp_instance_manager_n1_n2.get_image_from_family(family_name)

    # Assert
    assert image_uri == mock_image_data["self_link"]
    gcp_instance_manager_n1_n2.list_available_images.assert_called_once()


@pytest.mark.asyncio
async def test_get_image_from_family_error(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test error handling when getting image from a family fails."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    family_name = "nonexistent-family"

    # Mock list_available_images to return empty list
    gcp_instance_manager_n1_n2.list_available_images = AsyncMock(return_value=[])

    ret = await gcp_instance_manager_n1_n2.get_image_from_family(family_name)
    assert ret is None


@pytest.mark.asyncio
async def test_get_default_image(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test getting the default Ubuntu 24.04 LTS image."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)

    # Create mock image
    mock_image = MagicMock()
    mock_image.name = "ubuntu-2404-20240401"
    mock_image.creation_timestamp = "2024-04-01T12:00:00.000-07:00"
    mock_image.self_link = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-20240401"

    # Mock the images client
    gcp_instance_manager_n1_n2._images_client.get_from_family = MagicMock(return_value=mock_image)

    # Act
    image_uri = await gcp_instance_manager_n1_n2.get_default_image()

    # Assert
    assert image_uri == mock_image.self_link
    gcp_instance_manager_n1_n2._images_client.get_from_family.assert_called_once_with(
        project="ubuntu-os-cloud", family="ubuntu-2404-lts-amd64"
    )


@pytest.mark.asyncio
async def test_get_default_image_no_images(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test error handling when no default images are found."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Mock the images client to return None
    gcp_instance_manager_n1_n2._images_client.get_from_family = MagicMock(return_value=None)

    # Act & Assert
    ret = await gcp_instance_manager_n1_n2.get_default_image()
    assert ret is None


@pytest.mark.asyncio
async def test_list_available_images(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test listing available images."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Create mock images for public projects
    mock_ubuntu_image = MagicMock()
    mock_ubuntu_image.id = "12345"
    mock_ubuntu_image.name = "ubuntu-2404-20240501"
    mock_ubuntu_image.description = "Ubuntu 24.04 LTS"
    mock_ubuntu_image.family = "ubuntu-2404-lts"
    mock_ubuntu_image.creation_timestamp = "2024-05-01T12:00:00.000-07:00"
    mock_ubuntu_image.self_link = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-20240501"
    mock_ubuntu_image.status = "READY"
    mock_ubuntu_image.deprecated = None

    # Create a mock deprecated image
    mock_deprecated_image = MagicMock()
    mock_deprecated_image.id = "23456"
    mock_deprecated_image.name = "ubuntu-2204-deprecated"
    mock_deprecated_image.family = "ubuntu-2204-lts"
    mock_deprecated_image.creation_timestamp = "2022-04-01T12:00:00.000-07:00"
    mock_deprecated_image.deprecated = MagicMock()
    mock_deprecated_image.deprecated.state = "DEPRECATED"

    # Create mock images for user project
    mock_custom_image = MagicMock()
    mock_custom_image.id = "34567"
    mock_custom_image.name = "custom-image"
    mock_custom_image.description = "Custom user image"
    mock_custom_image.family = "custom-family"
    mock_custom_image.creation_timestamp = "2024-05-15T12:00:00.000-07:00"
    mock_custom_image.self_link = (
        "https://compute.googleapis.com/compute/v1/projects/test-project/global/images/custom-image"
    )
    mock_custom_image.status = "READY"

    # Mock the images client to return different responses for different projects
    def mock_list_images(**kwargs):
        request = kwargs.get("request")
        if request.project == "ubuntu-os-cloud":
            return [mock_ubuntu_image, mock_deprecated_image]
        elif request.project == "test-project":
            return [mock_custom_image]
        else:
            return []

    gcp_instance_manager_n1_n2._images_client.list = MagicMock(side_effect=mock_list_images)

    # Act
    images = await gcp_instance_manager_n1_n2.list_available_images()

    # Assert
    # We should get at least the Ubuntu image and the custom image
    # The deprecated image should be filtered out
    assert len(images) >= 2

    # Find the Ubuntu image in the results
    ubuntu_result = next((img for img in images if img["name"] == "ubuntu-2404-20240501"), None)
    assert ubuntu_result is not None
    assert ubuntu_result["id"] == "12345"
    assert ubuntu_result["description"] == "Ubuntu 24.04 LTS"
    assert ubuntu_result["family"] == "ubuntu-2404-lts"
    assert ubuntu_result["source"] == "GCP"
    assert ubuntu_result["project"] == "ubuntu-os-cloud"
    assert ubuntu_result["status"] == "READY"

    # Find the custom image in the results
    custom_result = next((img for img in images if img["name"] == "custom-image"), None)
    assert custom_result is not None
    assert custom_result["id"] == "34567"
    assert custom_result["description"] == "Custom user image"
    assert custom_result["family"] == "custom-family"
    assert custom_result["source"] == "User"
    assert custom_result["project"] == "test-project"
    assert custom_result["status"] == "READY"


@pytest.mark.asyncio
async def test_list_available_images_error_handling(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test error handling when listing images."""
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)

    # Mock the images client to raise an exception for one project but succeed for another
    def mock_list_images(**kwargs):
        request = kwargs.get("request")
        if request.project == "ubuntu-os-cloud":
            # Create a mock image for ubuntu-os-cloud
            mock_image = MagicMock()
            mock_image.id = "12345"
            mock_image.name = "ubuntu-2404-20240501"
            mock_image.description = "Ubuntu 24.04 LTS"
            mock_image.family = "ubuntu-2404-lts"
            mock_image.creation_timestamp = "2024-05-01T12:00:00.000-07:00"
            mock_image.self_link = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-20240501"
            mock_image.status = "READY"
            mock_image.deprecated = None
            return [mock_image]
        else:
            # Raise an exception for all other projects
            raise RuntimeError(f"Error accessing project {request.project}")

    gcp_instance_manager_n1_n2._images_client.list = MagicMock(side_effect=mock_list_images)

    # Act
    images = await gcp_instance_manager_n1_n2.list_available_images()

    # Assert
    # We should still get the Ubuntu image, even though other projects failed
    assert len(images) == 1
    assert images[0]["name"] == "ubuntu-2404-20240501"
    assert images[0]["project"] == "ubuntu-os-cloud"

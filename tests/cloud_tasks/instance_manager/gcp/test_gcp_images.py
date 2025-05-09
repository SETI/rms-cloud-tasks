"""Unit tests for the GCP Compute Engine instance manager."""

import asyncio
import copy
import random
from typing import Any, Dict, Tuple, List
from unittest.mock import MagicMock, patch, AsyncMock

import pytest
from google.api_core.exceptions import NotFound  # type: ignore
from google.cloud import billing
import uuid as _uuid  # Import uuid module with alias to avoid conflicts

from cloud_tasks.instance_manager.gcp import GCPComputeInstanceManager

from .conftest import (
    deepcopy_gcp_instance_manager,
    N1_2_CPU_PRICE,
    N1_2_RAM_PRICE,
    N1_4_CPU_PRICE,
    N1_4_RAM_PRICE,
    N2_CPU_PRICE,
    N2_RAM_PRICE,
    N1_PREEMPTIBLE_CPU_PRICE,
    N1_PREEMPTIBLE_RAM_PRICE,
    N2_PREEMPTIBLE_CPU_PRICE,
    N2_PREEMPTIBLE_RAM_PRICE,
    PD_STANDARD_PRICE,
    PD_BALANCED_PRICE,
    PD_SSD_PRICE,
    PD_EXTREME_PRICE,
    HD_BALANCED_PRICE,
    PD_EXTREME_IOPS_PRICE,
    HD_BALANCED_IOPS_PRICE,
    HD_BALANCED_THROUGHPUT_PRICE,
    LSSD_PRICE,
    LSSD_PREEMPTIBLE_PRICE,
)


@pytest.mark.asyncio
async def test_get_image_from_family(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test getting image from a family."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    family_name = "ubuntu-2404-lts"
    project = "ubuntu-os-cloud"

    # Create mock image
    mock_image = MagicMock()
    mock_image.name = "ubuntu-2404-20240401"
    mock_image.creation_timestamp = "2024-04-01T12:00:00.000-07:00"
    mock_image.self_link = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-20240401"

    # Mock the images client - this needs to be a non-async method that returns the mock image directly
    # because the implementation doesn't await the get_from_family call
    gcp_instance_manager_n1_n2._images_client.get_from_family = MagicMock(return_value=mock_image)

    # Act
    image_uri = await gcp_instance_manager_n1_n2._get_image_from_family(family_name, project)

    # Assert
    assert image_uri == mock_image.self_link
    gcp_instance_manager_n1_n2._images_client.get_from_family.assert_called_once()
    request = gcp_instance_manager_n1_n2._images_client.get_from_family.call_args[1]["request"]
    assert request.family == family_name
    assert request.project == project


@pytest.mark.asyncio
async def test_get_image_from_family_error(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test error handling when getting image from a family fails."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    family_name = "nonexistent-family"
    project = "ubuntu-os-cloud"
    error_message = "Family not found"

    # Mock the images client to raise an exception - use MagicMock instead of AsyncMock
    gcp_instance_manager_n1_n2._images_client.get_from_family = MagicMock(
        side_effect=ValueError(error_message)
    )

    # Act & Assert
    with pytest.raises(ValueError, match=f"Could not find image in family {family_name}"):
        await gcp_instance_manager_n1_n2._get_image_from_family(family_name, project)


@pytest.mark.asyncio
async def test_get_default_image(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test getting the default Ubuntu 24.04 LTS image."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Create mock images
    mock_image1 = MagicMock()
    mock_image1.name = "ubuntu-2404-20240401"
    mock_image1.creation_timestamp = "2024-04-01T12:00:00.000-07:00"
    mock_image1.self_link = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-20240401"

    mock_image2 = MagicMock()
    mock_image2.name = "ubuntu-2404-20240501"
    mock_image2.creation_timestamp = "2024-05-01T12:00:00.000-07:00"  # Newer image
    mock_image2.self_link = "https://compute.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-20240501"

    # Mock the images client to return both images
    gcp_instance_manager_n1_n2._images_client.list = MagicMock(
        return_value=[mock_image1, mock_image2]
    )

    # Act
    image_uri = await gcp_instance_manager_n1_n2._get_default_image()

    # Assert
    assert image_uri == mock_image2.self_link  # Should return the newer image
    gcp_instance_manager_n1_n2._images_client.list.assert_called_once()
    request = gcp_instance_manager_n1_n2._images_client.list.call_args[1]["request"]
    assert request.project == "ubuntu-os-cloud"
    assert request.filter == "family = 'ubuntu-2404-lts'"


@pytest.mark.asyncio
async def test_get_default_image_no_images(
    gcp_instance_manager_n1_n2: GCPComputeInstanceManager, mock_credentials: MagicMock
) -> None:
    """Test error handling when no default images are found."""
    # Arrange
    gcp_instance_manager_n1_n2 = deepcopy_gcp_instance_manager(gcp_instance_manager_n1_n2)
    mock_credentials = copy.deepcopy(mock_credentials)
    # Mock the images client to return an empty list
    gcp_instance_manager_n1_n2._images_client.list = MagicMock(return_value=[])

    # Act & Assert
    with pytest.raises(ValueError, match="No Ubuntu 24.04 LTS image found"):
        await gcp_instance_manager_n1_n2._get_default_image()


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

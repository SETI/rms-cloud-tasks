"""
Configuration for pytest.
"""

import os
import sys
import tempfile
import pytest
import uuid
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Do not define a custom event_loop fixture here -
# pytest-asyncio already provides one, and redefining it
# causes deprecation warnings


@pytest.fixture
def config_file():
    """Create a temporary config file for tests."""
    # Create a temporary config file with valid provider configurations
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as tmp:
        tmp.write(
            """
        # Test configuration
        aws:
          queue_name: test-queue
          access_key: test-access-key
          secret_key: test-secret-key
          region: us-west-2

        gcp:
          queue_name: test-queue
          project_id: test-project-id

        azure:
          queue_name: test-queue
          subscription_id: test-subscription-id
          tenant_id: test-tenant-id
          client_id: test-client-id
          client_secret: test-client-secret
        """
        )
        tmp_path = tmp.name

    # Return the path to the config file
    try:
        yield tmp_path
    finally:
        # Clean up the temporary file
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


@pytest.fixture(params=["aws", "gcp", "azure"])
def provider(request):
    """Provide cloud provider name for tests."""
    return request.param


@pytest.fixture
def queue_name():
    """Generate a unique queue name for tests."""
    return f"test-queue-{uuid.uuid4()}"

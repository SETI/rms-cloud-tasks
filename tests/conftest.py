"""
Configuration for pytest.
"""
import os
import sys
import asyncio
import pytest
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Do not define a custom event_loop fixture here -
# pytest-asyncio already provides one, and redefining it
# causes deprecation warnings
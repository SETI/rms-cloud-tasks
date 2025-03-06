"""
Worker module for the cloud task processing system.

This module provides tools for integrating existing worker code with
the cloud task processing system. It abstracts away the details of
cloud provider integration, allowing any worker to process tasks from
cloud-based queues.
"""

from cloud_tasks.worker.cloud_adapter import (
    CloudTaskAdapter,
    run_cloud_worker
)

__all__ = [
    'CloudTaskAdapter',
    'run_cloud_worker',
]

"""
Worker module for the cloud task processing system.

This module provides tools for integrating existing worker code with
the cloud task processing system. It abstracts away the details of
cloud provider integration, allowing any worker to process tasks from
cloud-based queues.
"""

from .worker import Worker, WorkerData

__all__ = [
    "Worker",
    "WorkerData",
]

"""
Azure Service Bus implementation of the TaskQueue interface.
"""

import asyncio
import json
import logging
from datetime import timedelta
from typing import Any

import shortuuid
from azure.servicebus import ServiceBusClient, ServiceBusMessage  # type: ignore
from azure.servicebus.management import ServiceBusAdministrationClient  # type: ignore

from ..common.config import AzureConfig
from .queue_manager import QueueManager

# Azure Service Bus maximum lock duration in seconds
_MAX_VISIBILITY_TIMEOUT_SECONDS = 300


class AzureServiceBusQueue(QueueManager):
    """Azure Service Bus implementation of the TaskQueue interface."""

    def __init__(self, azure_config: AzureConfig, **kwargs: Any) -> None:
        """
        Initialize the Azure Service Bus queue with configuration.

        Parameters:
            azure_config: Azure configuration (AzureConfig). Expected keys include
                tenant_id, client_id, client_secret, namespace_name, queue_name;
                optional fields may include subscription_id, resource_group.
                connection_string may be used instead of client_secret/namespace_name.
            **kwargs: Ignored (e.g. visibility_timeout, exactly_once from create_queue).

        Raises:
            ValueError: If azure_config.queue_name is None or an empty string.
        """
        self._service_bus_client: ServiceBusClient | None = None
        self._admin_client: ServiceBusAdministrationClient | None = None
        self._queue_name: str | None = None
        self._connection_string = None
        self._logger = logging.getLogger(__name__)

        queue_name = azure_config.queue_name
        if not queue_name or not str(queue_name).strip():
            raise ValueError("azure_config.queue_name is required and must be non-empty")
        self._queue_name = queue_name

        try:
            # Construct connection string from config (tenant_id, client_id reserved for future auth)
            client_secret = azure_config.client_secret
            namespace_name = azure_config.namespace_name

            # Create connection string using SAS key (assuming it's provided in the config)
            if azure_config.connection_string:
                self._connection_string = azure_config.connection_string
            else:
                self._connection_string = (
                    f"Endpoint=sb://{namespace_name}.servicebus.windows.net/;"
                    f"SharedAccessKeyName=RootManageSharedAccessKey;"
                    f"SharedAccessKey={client_secret}"
                )

            # Create admin client for queue management
            self._admin_client = ServiceBusAdministrationClient.from_connection_string(
                self._connection_string
            )

            # Create service bus client for sending/receiving messages
            self._service_bus_client = ServiceBusClient.from_connection_string(
                conn_str=self._connection_string, logging_enable=True
            )

            # TODO Make lazy queue creation like gcp/aws
            # # Create queue if it doesn't exist
            # try:
            #     # Check if queue exists - Azure SDK uses get_queue rather than queue_exists
            #     await self._admin_client.get_queue(queue_name)
            # except Exception:
            #     # Create the queue if it doesn't exist
            #     await self._admin_client.create_queue(
            #         queue_name,
            #         max_delivery_count=10,  # Number of delivery attempts before dead-letter
            #         lock_duration=timedelta(seconds=30),  # Default lock duration in seconds
            #         max_size_in_megabytes=1024,  # 1GB queue size
            #         requires_duplicate_detection=True,  # Prevent duplicate messages
            #         duplicate_detection_history_time_window=timedelta(
            #             minutes=1
            #         ),  # 1 minute window for duplication detection
            #     )

        except Exception as e:
            self._logger.error(f"Failed to initialize Azure Service Bus queue: {str(e)}")
            raise

    def _ensure_initialized(self) -> None:
        """Ensure client and queue name are set; raise RuntimeError if not.

        Raises:
            RuntimeError: If Azure Service Bus queue is not initialized (message:
                "Azure Service Bus queue is not initialized") when
                self._service_bus_client or self._queue_name is None.
        """
        if self._service_bus_client is None or self._queue_name is None:
            raise RuntimeError("Azure Service Bus queue is not initialized")

    async def send_message(self, message: dict[str, Any], _quiet: bool = False) -> None:
        """Send a message to the queue (delegates to send_task with task_id from message).

        Parameters:
            message: Dict with optional "task_id" and "data"; "data" defaults to message.
            _quiet: Unused; present for interface compatibility.

        Returns:
            None.

        Raises:
            Exception: Exceptions from send_task or shortuuid are propagated.
        """
        task_id = message.get("task_id", shortuuid.uuid())
        data = message.get("data", message)
        await self.send_task(task_id, data)

    async def send_task(self, task_id: str, task_data: dict[str, Any]) -> None:
        """
        Send a task to the Service Bus queue.

        Args:
            task_id: Unique identifier for the task
            task_data: Task data to be processed
        """
        self._logger.debug(f"Sending task '{task_id}' to queue '{self._queue_name}'")

        message = {"task_id": task_id, "data": task_data}
        message_body = json.dumps(message)

        try:
            # Create a Service Bus message with properties
            service_bus_message = ServiceBusMessage(
                body=message_body,
                message_id=task_id,
                content_type="application/json",
                subject="task",
            )

            # Get the event loop
            loop = asyncio.get_event_loop()

            self._ensure_initialized()
            assert self._service_bus_client is not None
            assert self._queue_name is not None
            # Send message to queue in a thread pool
            async with self._service_bus_client:
                sender = self._service_bus_client.get_queue_sender(queue_name=self._queue_name)
                await loop.run_in_executor(None, sender.send_messages, service_bus_message)

        except Exception as e:
            self._logger.error(f"Failed to publish message for task {task_id}: {str(e)}")
            raise RuntimeError(f"Failed to publish task to Azure Service Bus: {str(e)}")

    async def receive_messages(
        self, max_count: int = 1, acknowledge: bool = True
    ) -> list[dict[str, Any]]:
        """Receive messages from the queue.

        Parameters:
            max_count: Maximum number of messages to receive.
            acknowledge: Unused; Azure lock renewal is done in receive_tasks. Callers
                must explicitly acknowledge via acknowledge_task with the lock_token.

        Returns:
            List of dicts with message_id, data, and receipt_handle (lock_token).
        """
        tasks = await self.receive_tasks(max_count=max_count)
        return [
            {
                "message_id": t.get("task_id", ""),
                "data": t.get("data", {}),
                "receipt_handle": t.get("lock_token"),
            }
            for t in tasks
        ]

    async def receive_tasks(
        self,
        max_count: int = 1,
        visibility_timeout: int = 30,  # TODO Default visibility timeout in seconds
    ) -> list[dict[str, Any]]:
        """
        Receive tasks from the Service Bus queue with a lock.

        Args:
            max_count: Maximum number of messages to receive
            visibility_timeout: Duration in seconds for message lock

        Returns:
            List of task dictionaries with task_id, data, and lock_token
        """
        self._logger.debug(f"Receiving up to {max_count} tasks from queue '{self._queue_name}'")

        try:
            tasks = []
            loop = asyncio.get_event_loop()

            self._ensure_initialized()
            assert self._service_bus_client is not None
            assert self._queue_name is not None
            # Create receiver for the queue
            async with self._service_bus_client:
                receiver = self._service_bus_client.get_queue_receiver(
                    queue_name=self._queue_name, max_wait_time=10
                )

                # Receive messages in a thread pool
                received_messages = await loop.run_in_executor(
                    None,
                    lambda: receiver.receive_messages(max_message_count=max_count, max_wait_time=5),
                )

                for message in received_messages:
                    # Parse message body
                    message_body = json.loads(message.body.decode("utf-8"))

                    # Renew lock with the specified visibility timeout in a thread pool
                    await loop.run_in_executor(
                        None,
                        lambda: receiver.renew_message_lock(message, timeout=visibility_timeout),
                    )

                    tasks.append(
                        {
                            "task_id": message_body["task_id"],
                            "data": message_body["data"],
                            "lock_token": message.lock_token,  # Used to complete/fail the task
                        }
                    )

            return tasks
        except Exception as e:
            self._logger.error(f"Error receiving tasks: {str(e)}")
            return []

    async def acknowledge_message(self, message_handle: Any) -> None:
        """Acknowledge a message (alias for acknowledge_task).

        Parameters:
            message_handle: Lock token from receive_tasks; passed to acknowledge_task.

        Raises:
            Exception: Exceptions from acknowledge_task are propagated.
        """
        await self.acknowledge_task(message_handle)

    async def acknowledge_task(self, task_handle: Any) -> None:
        """
        Mark a task as completed and remove from the queue.

        Args:
            task_handle: lock_token from receive_tasks
        """
        self._logger.debug(
            f"Completing task with lock_token '{task_handle}' on queue '{self._queue_name}'"
        )

        try:
            loop = asyncio.get_event_loop()

            self._ensure_initialized()
            assert self._service_bus_client is not None
            assert self._queue_name is not None
            async with self._service_bus_client:
                receiver = self._service_bus_client.get_queue_receiver(queue_name=self._queue_name)
                # Complete the message using its lock token in a thread pool
                await loop.run_in_executor(None, receiver.acknowledge_message, task_handle)
        except Exception as e:
            self._logger.error(f"Error completing task: {str(e)}")
            raise

    async def retry_message(self, message_handle: Any) -> None:
        """Retry a message (alias for retry_task).

        Parameters:
            message_handle: Lock token from receive_tasks; passed to retry_task.

        Raises:
            Exception: Exceptions from retry_task are propagated.
        """
        await self.retry_task(message_handle)

    async def retry_task(self, task_handle: Any) -> None:
        """
        Mark a task as failed, allowing it to be retried.

        Args:
            task_handle: lock_token from receive_tasks
        """
        self._logger.debug(
            f"Failing task with lock_token '{task_handle}' on queue '{self._queue_name}'"
        )

        try:
            loop = asyncio.get_event_loop()

            self._ensure_initialized()
            assert self._service_bus_client is not None
            assert self._queue_name is not None
            async with self._service_bus_client:
                receiver = self._service_bus_client.get_queue_receiver(queue_name=self._queue_name)
                # Abandon the message in a thread pool
                await loop.run_in_executor(None, receiver.abandon_message, task_handle)
        except Exception as e:
            self._logger.error(f"Error failing task: {str(e)}")
            raise

    async def get_queue_depth(self) -> int:
        """
        Get the current depth (number of messages) in the queue.

        Returns:
            Approximate number of messages in the queue
        """
        self._logger.debug(f"Getting queue depth for queue '{self._queue_name}'")

        try:
            loop = asyncio.get_event_loop()

            self._ensure_initialized()
            if self._admin_client is None:
                raise RuntimeError("Azure admin client is not initialized")
            admin_client = self._admin_client
            queue_name = self._queue_name
            # Get queue runtime properties in a thread pool
            queue_properties = await loop.run_in_executor(
                None, lambda: admin_client.get_queue_runtime_properties(queue_name)
            )

            return queue_properties.active_message_count

        except Exception as e:
            self._logger.error(f"Error getting queue depth: {str(e)}")
            return 0

    async def purge_queue(self) -> None:
        """Remove all messages from the queue by deleting and recreating it."""
        self._logger.debug(f"Purging queue '{self._queue_name}'")

        try:
            loop = asyncio.get_event_loop()

            self._ensure_initialized()
            if self._admin_client is None:
                raise RuntimeError("Azure admin client is not initialized")
            admin_client = self._admin_client
            queue_name = self._queue_name
            # Delete the queue if it exists in a thread pool
            await loop.run_in_executor(None, lambda: admin_client.delete_queue(queue_name))

            # Wait a moment for deletion to complete
            await asyncio.sleep(2)

            # Create a new queue with the same properties in a thread pool
            await loop.run_in_executor(
                None,
                lambda: admin_client.create_queue(
                    queue_name,
                    max_delivery_count=10,
                    lock_duration=timedelta(seconds=30),
                    max_size_in_megabytes=1024,
                    requires_duplicate_detection=True,
                    duplicate_detection_history_time_window=timedelta(minutes=1),
                ),
            )
        except Exception as e:
            self._logger.error(f"Error purging queue: {str(e)}")
            raise

    async def delete_queue(self) -> None:
        """Delete the Service Bus queue entirely."""
        self._logger.debug(f"Deleting queue '{self._queue_name}'")

        try:
            loop = asyncio.get_event_loop()

            self._ensure_initialized()
            if self._admin_client is None:
                raise RuntimeError("Azure admin client is not initialized")
            admin_client = self._admin_client
            queue_name = self._queue_name
            # Delete the queue in a thread pool
            await loop.run_in_executor(None, lambda: admin_client.delete_queue(queue_name))
            self._logger.info(f"Successfully deleted queue {self._queue_name}")
        except Exception as e:
            self._logger.error(f"Error deleting queue: {str(e)}")
            raise

    def get_max_visibility_timeout(self) -> int:
        """Return maximum visibility timeout in seconds (Azure default)."""
        return _MAX_VISIBILITY_TIMEOUT_SECONDS

    async def extend_message_visibility(
        self, message_handle: Any, timeout: int | None = None
    ) -> None:
        """No-op for interface compatibility; Azure lock renewal is done in receive_tasks.

        Parameters:
            message_handle: Unused; lock token from receive_tasks.
            timeout: Unused; visibility duration in seconds.

        Returns:
            None. No action is taken; lock renewal is handled in receive_tasks.
        """
        pass  # No-op; lock renewal is done in receive_tasks

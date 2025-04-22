Worker API Reference
==================

.. warning::
   This documentation is a placeholder. The Worker API is still under development.

Introduction
-----------

The Cloud Tasks Worker API provides a framework for implementing workers that process tasks from cloud provider queues. Workers run on compute instances and process tasks from the queue in parallel using Python's multiprocessing capabilities.

Basic Usage
----------

Here's a simple example of how to implement a worker:

.. code-block:: python

   from cloud_tasks.worker import Worker

   def process_task(task_id: str, task_data: dict) -> tuple[bool, str]:
       """Process a single task.

       Args:
           task_id: Unique identifier for the task
           task_data: Dictionary containing task data

       Returns:
           Tuple of (success: bool, result: str)
           - success: True if task processed successfully, False otherwise
           - result: String describing the result or error message
       """
       try:
           print(f"Processing task {task_id}")
           # Your processing logic here
           print(f"Task data: {task_data}")
           return True, "Task completed successfully"
       except Exception as e:
           return False, str(e)

   # Create and start the worker
   if __name__ == "__main__":
       worker = Worker(process_task)
       asyncio.run(worker.start())

Environment Variables
------------------

The worker is configured using the following environment variables:

Required Variables
~~~~~~~~~~~~~~~~

- ``RMS_CLOUD_TASKS_PROVIDER``: Cloud provider (AWS, GCP, or AZURE)
- ``RMS_CLOUD_TASKS_JOB_ID``: Unique identifier for the job
- ``RMS_CLOUD_TASKS_QUEUE_NAME``: Name of the task queue to process

Optional Variables
~~~~~~~~~~~~~~~

- ``RMS_CLOUD_TASKS_PROJECT_ID``: Project ID (required for GCP only)
- ``RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE``: Number of concurrent tasks to process (defaults to number of vCPUs)
- ``RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS``: Number of vCPUs available on the instance
- ``RMS_CLOUD_TASKS_VISIBILITY_TIMEOUT_SECONDS``: How long tasks remain invisible after being claimed (default: 30)
- ``RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD``: Time in seconds to wait for tasks to complete during shutdown (default: 120)
- ``RMS_CLOUD_WORKER_USE_NEW_PROCESS``: Whether to use a new process for each task (default: False)

Instance Information Variables (Set Automatically)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- ``RMS_CLOUD_TASKS_INSTANCE_TYPE``: Type of the compute instance
- ``RMS_CLOUD_TASKS_INSTANCE_MEM_GB``: Memory available in GB
- ``RMS_CLOUD_TASKS_INSTANCE_SSD_GB``: Local SSD storage in GB
- ``RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB``: Boot disk size in GB
- ``RMS_CLOUD_TASKS_INSTANCE_IS_SPOT``: Whether running on spot/preemptible instance
- ``RMS_CLOUD_TASKS_INSTANCE_PRICE``: Price per hour for the instance

Worker Features
-------------

Parallel Processing
~~~~~~~~~~~~~~~~

The worker uses Python's multiprocessing to achieve true parallelism:

- Creates one worker process per vCPU (or as specified by ``RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE``)
- Each process handles one task at a time
- Tasks are distributed automatically among processes
- Results are collected and reported back to the main process

Task Processing
~~~~~~~~~~~~~

Tasks are processed with the following guarantees:

- Automatic visibility timeout management
- Task acknowledgement after successful processing
- Failed task handling and reporting
- Graceful shutdown with task completion
- Spot instance termination handling

Health Checks and Monitoring
~~~~~~~~~~~~~~~~~~~~~~~~~

The worker includes built-in monitoring features:

- Automatic spot/preemptible instance termination detection
- Active task count tracking
- Task success/failure statistics
- Process health monitoring

Graceful Shutdown
~~~~~~~~~~~~~~~

The worker implements graceful shutdown handling:

- Catches SIGTERM and SIGINT signals
- Allows in-progress tasks to complete
- Configurable grace period for task completion
- Proper process cleanup and termination

API Reference
-----------

Worker Class
~~~~~~~~~~

.. code-block:: python

   class Worker:
       def __init__(self, user_worker_function: Callable[[str, Dict[str, Any]], Tuple[bool, str]]):
           """Initialize the worker.

           Args:
               user_worker_function: Function to process tasks. Should accept task_id (str) and
                   task_data (dict) arguments and return a tuple of (success: bool, result: str).
           """
           pass

       async def start(self) -> None:
           """Start the worker and begin processing tasks.

           This method will:
           1. Initialize the task queue connection
           2. Start worker processes
           3. Begin task processing
           4. Run until shutdown is requested
           """
           pass

Error Handling
------------

The worker implements comprehensive error handling:

- Task processing errors are caught and reported
- Failed tasks are properly acknowledged
- Process crashes are detected and handled
- Queue connection errors are handled with retries
- Graceful degradation on cloud API failures

Best Practices
------------

1. **Task Processing Function**
   - Keep the function stateless
   - Handle all exceptions
   - Return clear success/failure status
   - Include informative result messages

2. **Resource Management**
   - Close file handles and connections
   - Clean up temporary files
   - Release system resources
   - Monitor memory usage

3. **Error Handling**
   - Log errors with sufficient context
   - Include stack traces for debugging
   - Return meaningful error messages
   - Handle both expected and unexpected errors

4. **Performance**
   - Optimize CPU-intensive operations
   - Minimize memory allocations
   - Use appropriate batch sizes
   - Monitor processing times

Future Enhancements
-----------------

The following features are planned for future releases:

- Distributed worker coordination
- Enhanced metrics and monitoring
- Horizontal scaling support
- Worker middleware support
- Enhanced task routing
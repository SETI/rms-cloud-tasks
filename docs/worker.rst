Writing a Worker Task
=====================

Introduction
------------

The Cloud Tasks Worker API provides a framework for implementing worker programs that
process tasks from cloud provider queues or local task files. Workers run on compute
instances or a local workstation and process tasks from the queue (or local file) in
parallel using Python's multiprocessing capabilities. The framework automatically handles
the creation and destruction of processes, logging of task results, and graceful shutdown
when the queue is empty and the worker is idle. When run on spot/preemptible instances,
it also takes care of monitoring for the instance shutdown warning and notifying each
running worker process.

Basic Usage
----------

Here's a simple example of how to implement a worker:

.. code-block:: python

   import sys
   from cloud_tasks.worker import Worker

   def process_task(task_id: str, task_data: dict, worker: Worker) -> tuple[bool, str | dict]:
       """Process a single task.

       Args:
           task_id: Unique identifier for the task
           task_data: Dictionary containing task data; will have the fields:
               - "task_id": Unique identifier for the task
               - "data": Dictionary containing task data
           worker: Worker object (useful for retrieving information about the
               local environment and polling for shutdown notifications)

       Returns:
           Tuple of (requeue: bool, result: str or dict)
           - requeue: True if task failed in a way that it should be re-queded for some other
             process to try it again; False to indicate that the task is complete (whether
             it succeeded or failed) and should not be retried
           - result: String or dict describing the result; this will be sent to the local
             log file or the result queue to be picked up by the pool manager
       """
       try:
           print(f"Processing task {task_id}")
           # Your processing logic here
           print(f"Task data: {task_data}")
           return False, "Task completed successfully"
       except Exception as e:
           return False, str(e)  # Don't retry on failure

   # Create and start the worker
   if __name__ == "__main__":
       worker = Worker(process_task, args=sys.argv[1:])
       asyncio.run(worker.start())

Environment Variables and Command Line Arguments
------------------------------------------------

The worker is configured using the following environment variables and/or command line
arguments. All parameters will first be set from the command line arguments, and if not
specified, will then be set from the environment variables. If neither is available,
the parameter will be set to ``None``.

Tasks File
~~~~~~~~~~

--tasks TASKS_FILE      The name of a local file containing tasks to process; if not
                        specified, the worker will pull tasks from the cloud provider
                        queue (see below). The filename can also be a cloud storage
                        path like gs://bucket/file, s3://bucket/file, or
                        https://path/to/file.

If specified, the tasks file should be in the same format as read by the :ref:`load_queue_cmd`
command.

Parameters Required if Tasks File is Not Specified, Optional Otherwise
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

--provider PROVIDER     The cloud provider to use (AWS, GCP, or AZURE) [or ``RMS_CLOUD_TASKS_PROVIDER``]
--job-id JOB_ID         Unique identifier for the job [or ``RMS_CLOUD_TASKS_JOB_ID``]

Optional Parameters
~~~~~~~~~~~~~~~~~~~

--project-id PROJECT_ID          Project ID (required for GCP) [or ``RMS_CLOUD_TASKS_PROJECT_ID``]
--queue-name QUEUE_NAME          Name of the task queue to process (derived from job ID if not specified) [or ``RMS_CLOUD_TASKS_QUEUE_NAME``]
--instance-type INSTANCE_TYPE    Instance type running on this computer [or ``RMS_CLOUD_TASKS_INSTANCE_TYPE``]
--num-cpus N                     Number of vCPUs on this computer [or ``RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS``]
--memory MEMORY_GB               Memory in GB on this computer [or ``RMS_CLOUD_TASKS_INSTANCE_MEM_GB``]
--local-ssd LOCAL_SSD_GB         Local SSD in GB on this computer [or ``RMS_CLOUD_TASKS_INSTANCE_SSD_GB``]
--boot-disk BOOT_DISK_GB         Boot disk in GB on this computer [or ``RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB``]
--is-spot                        Whether running on spot/preemptible instance [or ``RMS_CLOUD_TASKS_INSTANCE_IS_SPOT``]
--price PRICE_PER_HOUR           Price per hour for the instance [or ``RMS_CLOUD_TASKS_INSTANCE_PRICE``]
--num-simultaneous-tasks N       Number of concurrent tasks to process (defaults to number of vCPUs, or 1 if not specified) [or ``RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE``]
--max-runtime SECONDS            Maximum runtime for a task in seconds [or ``RMS_CLOUD_TASKS_MAX_RUNTIME``]
--shutdown-grace-period SECONDS  Time in seconds to wait for tasks to complete during shutdown [or ``RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD``]
--use-new-process                Whether to use a new process for each task [or ``RMS_CLOUD_WORKER_USE_NEW_PROCESS``]

Worker Features
-------------

Parallel Processing
~~~~~~~~~~~~~~~~

The worker uses Python's multiprocessing to achieve true parallelism:

- Creates one worker process per vCPU (or as specified by ``--num-simultaneous-tasks``)
- Each process handles one task at a time
- Tasks are distributed automatically among processes
- Results are collected and reported back to the main process
- When a task if complete, the process is reused for the next task; but if the
  ``--use-new-process`` flag is set, the process is destroyed and a new process is created for
  each task. This is useful when you need to ensure that each task releases all of its
  resources, including allocated memory, open file handles, etc. before starting the next
  task.

Task Processing
~~~~~~~~~~~~~~~

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

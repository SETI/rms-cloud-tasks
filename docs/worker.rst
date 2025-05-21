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
-----------

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
           Tuple of (retry: bool, result: str or dict)
           - retry: False if task succeeded or failed in a way that it should not be
             re-queued for some other process to try it again; True to indicate that the
             task failed in a way that it should be re-queued for some other process to
             try it again
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


Returning a Result
-------------------

The top-level worker function should return a tuple of (``retry``, ``result``).
Returning a ``retry`` value of ``False`` fundamentally indicates that the task should not
be re-tried. This could mean it actually succeeded in whatever you wanted it to do, or it
failed in such a way that you don't want to retry it (for example, an unhandled exception
which is likely to recur on future attempts). Returning a ``retry`` value of ``True``
indicates that the task failed in a way that it should be re-queued for some other process
to try it again. This normally would indicate some kind of transient error, such as
running out of disk space or memory or hitting some other kind of temporary resource
limit that you expect to not repeat.

The ``result`` value can be a string or a dictionary. This value will be returned in the
results queue to the pool manager so that you can log it to a local file. For example,
you might return a dictionary that contains a flag indicating whether the task truly
succeeded or not, and a string message with more details.

If your worker process exits by calling ``sys.exit()`` or by crashing due to a system problem
or unhandled exception, it is automatically assumed to have failed in a way that should
be re-tried. If the program will always crash in the same way, this could conceivably
result in an infinite task loop where the task keeps getting re-queued and retried. This is
why it is important to monitor the returned results and abort the pool manager if no
progress is being made. This behavior can be changed with the ``--no-retry-on-crash`` command
line option or the ``RMS_CLOUD_TASKS_NO_RETRY_ON_CRASH`` environment variable.

Note that if you are using a local task file, the task manager will never re-queue a task.

.. _worker_environment_variables:

Environment Variables and Command Line Arguments
------------------------------------------------

The worker is configured using the following environment variables and/or command line
arguments. All parameters will first be set from the command line arguments, and if not
specified, will then be set from the environment variables. If neither is available,
the parameter will be set to ``None`` or the given default. When a worker is run on
a remote compute instance, these environment variables are set automatically based on
information in the Cloud Tasks configuration file (or command line arguments), or
from information derived from the instance type:

```
RMS_CLOUD_TASKS_PROVIDER
RMS_CLOUD_TASKS_PROJECT_ID
RMS_CLOUD_TASKS_JOB_ID
RMS_CLOUD_TASKS_QUEUE_NAME
RMS_CLOUD_TASKS_INSTANCE_TYPE
RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS
RMS_CLOUD_TASKS_INSTANCE_MEM_GB
RMS_CLOUD_TASKS_INSTANCE_SSD_GB
RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB
RMS_CLOUD_TASKS_INSTANCE_IS_SPOT
RMS_CLOUD_TASKS_INSTANCE_PRICE
RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE
RMS_CLOUD_TASKS_MAX_RUNTIME
```

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

--project-id PROJECT_ID                    Project ID (required for GCP) [or ``RMS_CLOUD_TASKS_PROJECT_ID``]
--queue-name QUEUE_NAME                    Name of the task queue to process (derived from job ID if not specified) [or ``RMS_CLOUD_TASKS_QUEUE_NAME``]
--instance-type INSTANCE_TYPE              Instance type running on this computer [or ``RMS_CLOUD_TASKS_INSTANCE_TYPE``]
--num-cpus N                               Number of vCPUs on this computer [or ``RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS``]
--memory MEMORY_GB                         Memory in GB on this computer [or ``RMS_CLOUD_TASKS_INSTANCE_MEM_GB``]
--local-ssd LOCAL_SSD_GB                   Local SSD in GB on this computer [or ``RMS_CLOUD_TASKS_INSTANCE_SSD_GB``]
--boot-disk BOOT_DISK_GB                   Boot disk in GB on this computer [or ``RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB``]
--is-spot                                  Whether running on spot/preemptible instance [or ``RMS_CLOUD_TASKS_INSTANCE_IS_SPOT``]
--price PRICE_PER_HOUR                     Price per hour for the instance [or ``RMS_CLOUD_TASKS_INSTANCE_PRICE``]
--num-simultaneous-tasks N                 Number of concurrent tasks to process (defaults to number of vCPUs, or 1 if not specified) [or ``RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE``]
--max-runtime SECONDS                      Maximum runtime for a task in seconds [or ``RMS_CLOUD_TASKS_MAX_RUNTIME``] (default 3600 seconds)
--shutdown-grace-period SECONDS            Time in seconds to wait for tasks to complete during shutdown [or ``RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD``] (default 30 seconds)
--tasks-to-skip TASKS_TO_SKIP              Number of tasks to skip before processing any from the queue [or ``RMS_CLOUD_TASKS_TO_SKIP``]
--max-num-tasks MAX_NUM_TASKS              Maximum number of tasks to process [or ``RMS_CLOUD_TASKS_MAX_NUM_TASKS``]
--simulate-spot-termination-after SECONDS  Number of seconds after worker start to simulate a spot termination notice [or ``RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_AFTER``]
--simulate-spot-termination-delay SECONDS  Number of seconds after a simulated spot termination notice to forcibly kill all running tasks [or ``RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_DELAY``]

.. _worker_spot_instances:

Handling Spot Instance Termination
----------------------------------

For some providers, it is possible to select instances that are preemptible (e.g. spot
instances). Such instances are usually dramatically cheaper than regular instances, but
they can be terminated at any time by the cloud provider with little notice. When using
spot instances, the worker will monitor for the instance to be terminated and will attempt
to notify all running worker processes so they can exit gracefully.

To simulate a spot termination notice and subsequent forced shutdown of the compute
instance, you can use the ``--simulate-spot-termination-after`` and
``--simulate-spot-termination-delay`` arguments or the
``RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_AFTER`` and
``RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_DELAY`` environment variables. This is useful
for testing the worker's shutdown behavior without waiting for an actual spot termination
notice, which is unpredictable.

It is recommended that a task check for impending termination before starting to commit
results to storage, as the writing and copying process may be interrupted by the
destruction of the instance, resulting in a partial write. This can be done by checking
the ``worker.received_termination_notice`` property. However, note that providers do not
guarantee a particular instance lifetime after the termination notice is sent, so a worker
must still be able to tolerate an unexpected shutdown at any point in its execution.

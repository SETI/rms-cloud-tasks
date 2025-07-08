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

   def process_task(task_id: str, task_data: dict, worker_data: WorkerData) -> tuple[bool, str | dict]:
       """Process a single task.

       Args:
           task_id: Unique identifier for the task
           task_data: Dictionary containing task data; will have the fields:
               - "task_id": Unique identifier for the task
               - "data": Dictionary containing task data
           worker_data: WorkerData object (useful for retrieving information about the
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
       print(f"Processing task {task_id}")
       # Your processing logic here
       print(f"Task data: {task_data}")
       return False, "Task completed successfully"

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

The ``result`` value can be a string or a JSON-serializable dictionary. This value will be
returned in the results queue to the pool manager so that you can log it to a local file.
For example, you might return a dictionary that contains a flag indicating whether the
task truly succeeded or not, and a string message with more details.

If the task does not complete successfully (meaning it returned a retry value and a result
data structure), there are three possibilities:

1. The task timed out (exceeded the time set by the ``--max-runtime`` option).
2. The task exited prematurely, e.g. due to a crash or by calling ``sys.exit()``.
3. The task raised an unhandled exception.

In each case, you have the option of deciding whether to automatically retry the task by
using the worker command line options ``--retry-on-timeout``, ``--retry-on-exit``, and
``--retry-on-exception``, or their corresponding environment variables. Note that if you
turn on retry for a particular type of failure, but your program will always fail in the
same way for a particular task, this could result in an infinite task loop where the task
keeps getting re-queued and retried. Thus these options should be used with caution. This
is also why it is important to monitor the returned results and abort the pool manager if
no progress is being made.

Note that if you are using a local task file, the task manager will never re-queue a task,
regardless of the retry options you set.


.. _worker_environment_variables:

Environment Variables and Command Line Arguments
------------------------------------------------

The worker is configured using the following environment variables and/or command line
arguments. All parameters will first be set from the command line arguments, and if not
specified, will then be set from the environment variables. If neither is available, the
parameter will be set to ``None`` or the given default. *When a worker is run on a remote
compute instance, the following subset of environment variables are set automatically
based on information in the Cloud Tasks configuration file (or command line arguments
given to ``manage_pool`` or ``run``), or from information derived from the instance type*:

.. code-block:: none

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
  RMS_CLOUD_TASKS_RETRY_ON_EXIT
  RMS_CLOUD_TASKS_RETRY_ON_EXCEPTION
  RMS_CLOUD_TASKS_RETRY_ON_TIMEOUT


Task File
~~~~~~~~~

--task-file TASK_FILE   The name of a local JSON or YAML file containing tasks to process; if not
                        specified, the worker will pull tasks from the cloud provider
                        queue (see below). The filename can also be a cloud storage
                        path like ``gs://bucket/file``, ``s3://bucket/file``, or
                        ``https://path/to/file``. If not specified, the task manager will pull
                        tasks from the cloud provider queue.

If specified, the task file should be in the same format as read by the :ref:`cli_load_queue_cmd`
command.


Overriding the Task Source
~~~~~~~~~~~~~~~~~~~~~~~~~~

The task source (either file or queue) can be overridden by passing a ``task_source``
argument to the ``Worker`` constructor. This can be a string or ``pathlib.Path`` or
``filecache.FCPath``, or a function that returns an iterator of tasks. If a filename is
passed, it will be treated as a path to a JSON or YAML file containing tasks. If a
function is passed, it will be called repeatedly to yield the tasks. If ``task_source``
is specified, the ``--task-file`` command line argument will be ignored.


Parameters Required if Task File or Task Source is Not Specified, Optional Otherwise
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

--provider PROVIDER     The cloud provider (AWS or GCP) to use to check for spot instance termination notices and for cloud-based queueing [or ``RMS_CLOUD_TASKS_PROVIDER``]
--job-id JOB_ID         Job ID; used to identify the cloud-based task queue name [or ``RMS_CLOUD_TASKS_JOB_ID``]

Optional Parameters
~~~~~~~~~~~~~~~~~~~

--project-id PROJECT_ID                    Project ID (required for GCP) [or ``RMS_CLOUD_TASKS_PROJECT_ID``]
--queue-name QUEUE_NAME                    Cloud-based task queue name; if not specified will be derived from the job ID [or ``RMS_CLOUD_TASKS_QUEUE_NAME``]
--exactly-once-queue                       If specified, task and event queue messages are guaranteed to be delivered exactly once to any recipient [or ``RMS_CLOUD_TASKS_EXACTLY_ONCE_QUEUE`` is "1" or "true"]
--no-exactly-once-queue                    If specified, task and event queue messages are delivered at least once, but could be delivered multiple times [or ``RMS_CLOUD_TASKS_EXACTLY_ONCE_QUEUE`` is "0" or "false"]
--event-log-file EVENT_LOG_FILE            File to write events to if --event-log-to-file is specified (defaults to "events.log") [or ``RMS_CLOUD_TASKS_EVENT_LOG_FILE``]
--event-log-to-file                        If specified, events will be written to the file specified by --event-log-file or $RMS_CLOUD_TASKS_EVENT_LOG_FILE (default if --task-file is specified) [or ``RMS_CLOUD_TASKS_EVENT_LOG_TO_FILE`` is "1" or "true"]
--no-event-log-to-file                     If specified, events will not be written to a file [or ``RMS_CLOUD_TASKS_EVENT_LOG_TO_FILE`` is "0" or "false"]
--event-log-to-queue                       If specified, events will be written to a cloud-based queue (default if --task-file is not specified) [or ``RMS_CLOUD_TASKS_EVENT_LOG_TO_QUEUE`` is "1" or "true"]
--no-event-log-to-queue                    If specified, events will not be written to a cloud-based queue [or ``RMS_CLOUD_TASKS_EVENT_LOG_TO_QUEUE`` is "0" or "false"]
--instance-type INSTANCE_TYPE              Instance type; optional information for the worker processes [or ``RMS_CLOUD_TASKS_INSTANCE_TYPE``]
--num-cpus N                               Number of vCPUs on this computer; optional information for the worker processes [or ``RMS_CLOUD_TASKS_INSTANCE_NUM_VCPUS``]
--memory MEMORY_GB                         Memory in GB on this computer; optional information for the worker processes [or ``RMS_CLOUD_TASKS_INSTANCE_MEM_GB``]
--local-ssd LOCAL_SSD_GB                   Local SSD in GB on this computer; optional information for the worker processes [or ``RMS_CLOUD_TASKS_INSTANCE_SSD_GB``]
--boot-disk BOOT_DISK_GB                   Boot disk size in GB on this computer; optional information for the worker processes [or ``RMS_CLOUD_TASKS_INSTANCE_BOOT_DISK_GB``]
--is-spot                                  If supported by the provider, specify that this is a spot instance and subject to unexpected termination [or ``RMS_CLOUD_TASKS_INSTANCE_IS_SPOT`` is "1" or "true"]
--no-is-spot                               If supported by the provider, specify that this is not a spot instance and is not subject to unexpected termination (default) [or ``RMS_CLOUD_TASKS_INSTANCE_IS_SPOT`` is "0" or "false"]
--price PRICE_PER_HOUR                     Price in USD/hour on this computer; optional information for the worker processes [or ``RMS_CLOUD_TASKS_INSTANCE_PRICE``]
--num-simultaneous-tasks N                 Number of concurrent tasks to process (defaults to number of vCPUs, or 1 if not specified) [or ``RMS_CLOUD_TASKS_NUM_TASKS_PER_INSTANCE``]
--max-runtime SECONDS                      Maximum allowed runtime in seconds; used to determine queue visibility timeout and to kill tasks that are running too long [or ``RMS_CLOUD_TASKS_MAX_RUNTIME``] (default 600 seconds)
--shutdown-grace-period SECONDS            How long to wait in seconds for processes to gracefully finish after shutdown (SIGINT, SIGTERM, or Ctrl-C) is requested before killing them (default 30) [or ``RMS_CLOUD_TASKS_SHUTDOWN_GRACE_PERIOD``]
--tasks-to-skip TASKS_TO_SKIP              Number of tasks to skip before processing any from the queue [or ``RMS_CLOUD_TASKS_TO_SKIP``]
--max-num-tasks MAX_NUM_TASKS              Maximum number of tasks to process [or ``RMS_CLOUD_TASKS_MAX_NUM_TASKS``]
--retry-on-exit                            If specified, retry tasks on premature exit [or ``RMS_CLOUD_TASKS_RETRY_ON_EXIT`` is "1" or "true"]
--no-retry-on-exit                         If specified, do not retry tasks on premature exit (default) [or ``RMS_CLOUD_TASKS_RETRY_ON_EXIT`` is "0" or "false"]
--retry-on-exception                       If specified, retry tasks on unhandled exception [or ``RMS_CLOUD_TASKS_RETRY_ON_EXCEPTION`` is "1" or "true"]
--no-retry-on-exception                    If specified, do not retry tasks on unhandled exception (default) [or ``RMS_CLOUD_TASKS_RETRY_ON_EXCEPTION`` is "0" or "false"]
--retry-on-timeout                         If specified, tasks will be retried if they exceed the maximum runtime specified by --max-runtime [or ``RMS_CLOUD_TASKS_RETRY_ON_TIMEOUT`` is "1" or "true"]
--no-retry-on-timeout                      If specified, tasks will not be retried if they exceed the maximum runtime specified by --max-runtime (default) [or ``RMS_CLOUD_TASKS_RETRY_ON_TIMEOUT`` is "0" or "false"]
--simulate-spot-termination-after SECONDS  Number of seconds after worker start to simulate a spot termination notice [or ``RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_AFTER``]
--simulate-spot-termination-delay SECONDS  Number of seconds after a simulated spot termination notice to forcibly kill all running tasks [or ``RMS_CLOUD_TASKS_SIMULATE_SPOT_TERMINATION_DELAY``]
--verbose                                  Set the console log level to DEBUG instead of INFO


Specifying Additional Arguments
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The worker can be configured to accept additional arguments. This is done by creating an ``argparse.ArgumentParser``,
populating it with the arguments you want to accept, and passing it to the ``Worker`` constructor. For example:

.. code-block:: python

   parser = argparse.ArgumentParser()
   parser.add_argument("--my-arg", type=str, required=True)
   worker = Worker(process_task, args=sys.argv[1:], argparser=parser)

It is important that these user-specified arguments not conflict with the arguments already
supported by ``Worker``.

The resulting parsed arguments can be accessed from the ``WorkerData`` object using the
``args`` attribute. For example:

.. code-block:: python

   val = worker_data.args.my_arg


.. _worker_logging_events:

Logging Events
--------------

Various events can be logged to a local file or a cloud-based queue. The events are written
in a structured format that can be parsed by the pool manager or other software to update
the status and results of the tasks. An example entry is:

.. code-block:: json

  {"timestamp": "2025-05-26T01:56:26.321172",
  "hostname": "rmscr-parallel-addition-job-0g23gxetnyyavtxjrul6gberr",
  "event_type": "task_completed",
  "task_id": "addition-task-009684",
  "elapsed_time": 0.13359451293945312,
  "retry": false,
  "result": "Success!"
  }

The ``timestamp`` and ``hostname`` fields are always present.

The ``event_type`` field can have the following values:

- ``task_completed``: Indicates that the task completed normally. The ``task_id`` field will
  contain the task ID as given in the task file. The ``retry`` and ``result``
  fields will contain the values returned by the task function. The ``elapsed_time`` field
  will contain the number of fractional seconds the task took to complete, including process
  creation and destruction overhead.
- ``task_timed_out``: Indicates that the task timed out (exceeded the time set by the
  ``--max-runtime`` option). The ``task_id`` field will contain the task ID as given in the
  task file. The ``elapsed_time`` field will contain the number of fractional seconds the
  task ran before being killed.
- ``task_exited``: Indicates that the task exited prematurely. The ``task_id`` field will
  contain the task ID as given in the task file. The ``elapsed_time`` and ``exit_code``
  fields will contain the number of seconds the task took to complete and the exit code of
  the task, respectively.
- ``non_fatal_exception``: Indicates that the task manager encountered an exception that
  was deemed non-fatal and continued to run. The ``exception`` field will contain the
  exception message. The ``stack_trace`` field will contain the stack trace of the exception.
- ``fatal_exception``: Indicates that the task manager encountered an exception that
  was deemed fatal and has exited. No further tasks will be processed and no events
  collected or reported. If this occurred on a cloud-based compute instance, be aware that
  the instance is now costing money without performing any work. The ``exception`` field
  will contain the exception message. The ``stack_trace`` field will contain the stack
  trace of the exception.
- ``spot_termination``: Indicates that the worker received a spot termination notice.
  No further tasks will be accepted and any existing tasks may be terminated prematurely
  if the instance is destroyed before they finish. Any existing tasks that complete before
  the instance is destroyed will have their results reported as usual.


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
the ``worker_data.received_termination_notice`` property. However, note that providers do not
guarantee a particular instance lifetime after the termination notice is sent, so a worker
must still be able to tolerate an unexpected shutdown at any point in its execution.


Running Workers on a Local Workstation
--------------------------------------

The workers can be run on a local workstation. This is useful for testing and debugging,
and also as a simple way to parallelize an existing program that does not require the
performance of cloud-based compute instances. When run locally, the top-level program
should be supplied the necessary command line arguments to specify the task source (such as
``--task-file``)
Parallel Addition Example
=========================

A simple example is included in the ``rms-cloud-tasks`` repo in the directory
``examples/parallel_addition``. The task accepts two integers, adds them together, and
stores the result in a local directory or cloud bucket. It can then delay a programmable
amount of time to simulate a task that takes more time and emphasize the need for
parallelism. The example includes a file describing 10,000 tasks. If the delay is set to 1
second, this means the complete set of tasks will require 10,000 CPU-seconds, or about 2.8
hours on a single CPU. Running with 100-fold parallelism will reduce the time to around
two minutes, plus the overhead of launching and terminating the instances and managing the
task processes.

Version 1: Simple Addition with Time Delays
-------------------------------------------

Specifying Tasks
~~~~~~~~~~~~~~~~

The task queue is stored in whatever queueing system is native to the cloud provider being used.
Tasks are loaded from a JSON file consisting of a list of dictionaries with the format:

.. code-block:: json

    [
      {
        "id": "task-name-1",
        "data": {
          "some_arg1": "value",
          "some_arg2": "value"
        }
      }
    ]

For example, the tasks for the addition example look like:

.. code-block:: json

    [
      {
        "id": "addition-task-000001",
        "data": {
          "num1": -84808,
          "num2": -71224
        }
      },
      {
        "id": "addition-task-000002",
        "data": {
          "num1": 511,
          "num2": -44483
        }
      }
    ]

Running the Tasks Locally
~~~~~~~~~~~~~~~~~~~~~~~~~

To run the tasks locally, you simply set the environment variables required by the task code
(``ADDITION_OUTPUT_DIR`` and ``ADDITION_TASK_DELAY``) and run the task code directly, specifying
the task file:

.. code-block:: bash

    git clone https://github.com/SETI/rms-cloud-tasks
    cd rms-cloud-tasks
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    pip install -e .
    export ADDITION_OUTPUT_DIR=results
    export ADDITION_TASK_DELAY=1
    python examples/parallel_addition/worker_addition.py --task-file examples/parallel_addition/addition_tasks.json

This will run the tasks one at a time, and each task will delay for 1 second before exiting. The result
will be similar to this:

.. code-block:: none

  2025-07-08 11:19:22.944 INFO - Configuration:
  2025-07-08 11:19:22.946 INFO -   Using local tasks file: "examples/parallel_addition/addition_tasks.json"
  2025-07-08 11:19:22.946 INFO -   Tasks to skip: None
  2025-07-08 11:19:22.946 INFO -   Maximum number of tasks: None
  2025-07-08 11:19:22.946 INFO -   Provider: None
  2025-07-08 11:19:22.946 INFO -   Project ID: None
  2025-07-08 11:19:22.946 INFO -   Job ID: None
  2025-07-08 11:19:22.946 INFO -   Queue name: None
  2025-07-08 11:19:22.946 INFO -   Exactly-once queue: False
  2025-07-08 11:19:22.946 INFO -   Event log to file: True
  2025-07-08 11:19:22.946 INFO -   Event log file: events.log
  2025-07-08 11:19:22.946 INFO -   Event log to queue: False
  2025-07-08 11:19:22.946 INFO -   Instance type: None
  2025-07-08 11:19:22.946 INFO -   Num CPUs: None
  2025-07-08 11:19:22.946 INFO -   Memory: None GB
  2025-07-08 11:19:22.946 INFO -   Local SSD: None GB
  2025-07-08 11:19:22.946 INFO -   Boot disk size: None GB
  2025-07-08 11:19:22.946 INFO -   Spot instance: None
  2025-07-08 11:19:22.946 INFO -   Price per hour: None
  2025-07-08 11:19:22.946 INFO -   Num simultaneous tasks (default): 1
  2025-07-08 11:19:22.946 INFO -   Maximum runtime: 600 seconds
  2025-07-08 11:19:22.946 INFO -   Shutdown grace period: 30 seconds
  2025-07-08 11:19:22.946 INFO -   Retry on exit: None
  2025-07-08 11:19:22.946 INFO -   Retry on exception: None
  2025-07-08 11:19:22.946 INFO -   Retry on timeout: None
  2025-07-08 11:19:22.947 INFO - Started single-task worker #0 (PID 2113064)
  2025-07-08 11:19:23.150 INFO - Worker #0: Started, processing task addition-task-000001
  2025-07-08 11:19:24.150 INFO - Worker #0: Completed task addition-task-000001 in 1.00 seconds, retry False
  2025-07-08 11:19:24.150 INFO - Worker #0: Exiting
  2025-07-08 11:19:24.250 INFO - Worker #0 reported task addition-task-000001 completed in 1.3 seconds with no retry; result: results/addition-task-000001.txt
  2025-07-08 11:19:24.251 INFO - Started single-task worker #1 (PID 2113090)
  2025-07-08 11:19:24.465 INFO - Worker #1: Started, processing task addition-task-000002
  2025-07-08 11:19:25.466 INFO - Worker #1: Completed task addition-task-000002 in 1.00 seconds, retry False
  2025-07-08 11:19:25.467 INFO - Worker #1: Exiting
  2025-07-08 11:19:25.554 INFO - Worker #1 reported task addition-task-000002 completed in 1.3 seconds with no retry; result: results/addition-task-000002.txt
  2025-07-08 11:19:25.555 INFO - Started single-task worker #2 (PID 2113165)
  2025-07-08 11:19:25.756 INFO - Worker #2: Started, processing task addition-task-000003
  2025-07-08 11:19:26.757 INFO - Worker #2: Completed task addition-task-000003 in 1.00 seconds, retry False
  2025-07-08 11:19:26.757 INFO - Worker #2: Exiting
  2025-07-08 11:19:26.758 INFO - Worker #2 reported task addition-task-000003 completed in 1.2 seconds with no retry; result: results/addition-task-000003.txt
  2025-07-08 11:19:26.759 INFO - Started single-task worker #3 (PID 2113169)
  2025-07-08 11:19:26.963 INFO - Worker #3: Started, processing task addition-task-000004
  2025-07-08 11:19:27.964 INFO - Worker #3: Completed task addition-task-000004 in 1.00 seconds, retry False
  2025-07-08 11:19:27.964 INFO - Worker #3: Exiting
  2025-07-08 11:19:28.064 INFO - Worker #3 reported task addition-task-000004 completed in 1.3 seconds with no retry; result: results/addition-task-000004.txt

To abort the task manager before all tasks are complete, type ``Ctrl-C`` **once**. This
will give the current tasks a chance to complete cleanly, and then the task manager will
exit. You can change how long to wait before the current tasks are complete with the
``--shutdown-grace-period`` option.

Note that while each task took exactly 1 second, the reported time was somewhat more; this is due
to the overhead of managing the task queue and spawning new worker processes.

The command ``pip install -e .`` in the above example is required to be able to import the
``cloud_tasks`` package when it wasn't installed by ``pip``. It allows you to use the
local copy of ``cloud_tasks`` that you cloned, which is necessary when running this
example code, because the example code is present in the same repo (you could also just do
a ``pip install rms-cloud-tasks`` instead and use the cloned repo solely for the example
source code).

If you want to run the tasks locally with more parallelism, you can use the
``--num-simultaneous-tasks`` option:

.. code-block:: bash

    python examples/parallel_addition/worker_addition.py --task-file examples/parallel_addition/addition_tasks.json --num-simultaneous-tasks 10

This will change the output to something like this:

.. code-block:: none

  2025-07-08 11:24:15.066 INFO - Started single-task worker #0 (PID 2121068)
  2025-07-08 11:24:15.066 INFO - Started single-task worker #1 (PID 2121069)
  2025-07-08 11:24:15.067 INFO - Started single-task worker #2 (PID 2121070)
  2025-07-08 11:24:15.067 INFO - Started single-task worker #3 (PID 2121071)
  2025-07-08 11:24:15.067 INFO - Started single-task worker #4 (PID 2121072)
  2025-07-08 11:24:15.068 INFO - Started single-task worker #5 (PID 2121073)
  2025-07-08 11:24:15.068 INFO - Started single-task worker #6 (PID 2121074)
  2025-07-08 11:24:15.068 INFO - Started single-task worker #7 (PID 2121075)
  2025-07-08 11:24:15.068 INFO - Started single-task worker #8 (PID 2121076)
  2025-07-08 11:24:15.069 INFO - Started single-task worker #9 (PID 2121077)
  2025-07-08 11:24:15.284 INFO - Worker #8: Started, processing task addition-task-000009
  2025-07-08 11:24:15.286 INFO - Worker #9: Started, processing task addition-task-000010
  2025-07-08 11:24:15.296 INFO - Worker #3: Started, processing task addition-task-000004
  2025-07-08 11:24:15.297 INFO - Worker #7: Started, processing task addition-task-000008
  2025-07-08 11:24:15.300 INFO - Worker #4: Started, processing task addition-task-000005
  2025-07-08 11:24:15.304 INFO - Worker #1: Started, processing task addition-task-000002
  2025-07-08 11:24:15.309 INFO - Worker #0: Started, processing task addition-task-000001
  2025-07-08 11:24:15.319 INFO - Worker #6: Started, processing task addition-task-000007
  2025-07-08 11:24:15.359 INFO - Worker #2: Started, processing task addition-task-000003
  2025-07-08 11:24:15.360 INFO - Worker #5: Started, processing task addition-task-000006
  2025-07-08 11:24:16.285 INFO - Worker #8: Completed task addition-task-000009 in 1.00 seconds, retry False
  2025-07-08 11:24:16.285 INFO - Worker #8: Exiting
  2025-07-08 11:24:16.287 INFO - Worker #9: Completed task addition-task-000010 in 1.00 seconds, retry False
  2025-07-08 11:24:16.287 INFO - Worker #9: Exiting


Running the Tasks in the Cloud
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Loading the Queue
+++++++++++++++++

To run the tasks in the cloud, you need to load the tasks into a cloud-based queue. This
is done by running the ``cloud_tasks`` command line program with the name of the cloud
provider and a job ID. These can also be specified in a configuration file. For Google
Cloud you also need to specify the project ID. For our sample addition task, we will get
the job ID from a configuration file and specify the provider and project ID on the
command line, since these are user-specific. The configuration file and tasks list are
available in the ``rms-cloud-tasks`` repo:

.. code-block:: bash

    git clone https://github.com/SETI/rms-cloud-tasks
    cd rms-cloud-tasks

Here is the command that loads the task queue:

.. code-block:: bash

    cloud_tasks load_queue --config examples/parallel_addition/config.yml --provider gcp --project-id <PROJECT_ID> --task-file examples/parallel_addition/addition_tasks.json

You should replace the ``<PROJECT_ID>`` with a project defined for your account.

This will create the queue, if it doesn't already exist, read the tasks from the given
JSON file, and place them in the queue. If the queue already exists, the tasks will be
added to those already there.

Running Tasks
+++++++++++++

Running tasks consists of:

- Choosing an optimal instance type based on given constraints
- Creating a specified number of instances; each instance will run a specified startup script
- Monitoring the instances to make sure they continue to run, and starting new instances as necessary
- Terminating the instances when the job is complete

These steps are performed automatically.

For Google Cloud, the permissions granted to compute instances are determined by a
:ref:`service account <gcp_service_account>`. This account can be specified in the configuration
file (``service_account:``) or on the command line using ``--service-account``.

Finally, the location of the output bucket needs to be specified in the startup script in
the configuration file, since that is user-specific. Change this line in the file
``examples/parallel_addition/config.yml`` before running ``manage_pool``:

.. code-block:: yaml

    export ADDITION_OUTPUT_DIR=gs://<BUCKET>/addition-results

Be sure that the bucket exists and that the service account you provide has write access to it.

Here is an example command that will find the cheapest compute instance in the specified region with
exactly 8 CPUs and at least 2 GB memory per CPU and create 5 of them.

.. code-block:: bash

    cloud_tasks manage_pool --config examples/parallel_addition/config.yml --provider gcp --project-id <PROJECT_ID> --service-account <SERVICE_ACCOUNT> --region us-central1 --min-cpu 8 --max-cpu 8 --min-memory-per-cpu 2 --max-instances 5 -v

You should replace the ``<PROJECT_ID>`` with the same project used above and ``<SERVICE_ACCOUNT>``
with the email address of the :ref:`service account <gcp_service_account>` you created.

The result will be similar to this:

.. code-block:: none

  2025-06-11 15:00:21.424 INFO - Loading configuration from examples/parallel_addition/config.yml
  2025-06-11 15:00:21.425 INFO - Starting pool management for job: parallel-addition-job
  2025-06-11 15:00:21.425 INFO - Provider configuration:
  2025-06-11 15:00:21.425 INFO -   Provider: GCP
  2025-06-11 15:00:21.425 INFO -   Region: us-central1
  2025-06-11 15:00:21.425 INFO -   Zone: None
  2025-06-11 15:00:21.425 INFO -   Job ID: parallel-addition-job
  2025-06-11 15:00:21.425 INFO -   Queue: parallel-addition-job
  2025-06-11 15:00:21.425 INFO - Instance type selection constraints:
  2025-06-11 15:00:21.425 INFO -   Instance types: None
  2025-06-11 15:00:21.425 INFO -   CPUs: 8 to 8
  2025-06-11 15:00:21.425 INFO -   Memory: None to None GB
  2025-06-11 15:00:21.425 INFO -   Memory per CPU: 2.0 to None GB
  2025-06-11 15:00:21.425 INFO -   Boot disk types: None
  2025-06-11 15:00:21.425 INFO -   Boot disk total size: 10.0 GB
  2025-06-11 15:00:21.425 INFO -   Boot disk base size: 0.0 GB
  2025-06-11 15:00:21.425 INFO -   Boot disk per CPU: None GB
  2025-06-11 15:00:21.425 INFO -   Boot disk per task: None GB
  2025-06-11 15:00:21.425 INFO -   Local SSD: None to None GB
  2025-06-11 15:00:21.425 INFO -   Local SSD per CPU: None to None GB
  2025-06-11 15:00:21.425 INFO -   Local SSD per task: None to None GB
  2025-06-11 15:00:21.425 INFO - Number of instances constraints:
  2025-06-11 15:00:21.425 INFO -   # Instances: 1 to 5
  2025-06-11 15:00:21.425 INFO -   Total CPUs: None to None
  2025-06-11 15:00:21.425 INFO -   CPUs per task: 1.0
  2025-06-11 15:00:21.425 INFO -     Tasks per instance: None to None
  2025-06-11 15:00:21.425 INFO -     Simultaneous tasks: None to None
  2025-06-11 15:00:21.425 INFO -   Total price per hour: None to $10.00
  2025-06-11 15:00:21.425 INFO -   Pricing: On-demand instances
  2025-06-11 15:00:21.425 INFO - Miscellaneous:
  2025-06-11 15:00:21.425 INFO -   Scaling check interval: 60 seconds
  2025-06-11 15:00:21.425 INFO -   Instance termination delay: 60 seconds
  2025-06-11 15:00:21.425 INFO -   Max runtime: 10 seconds
  2025-06-11 15:00:21.425 INFO -   Max parallel instance creations: 10
  2025-06-11 15:00:21.425 INFO -   Image: None
  2025-06-11 15:00:21.425 INFO -   Startup script:
  2025-06-11 15:00:21.425 INFO -     apt-get update -y
  2025-06-11 15:00:21.425 INFO -     apt-get install -y python3 python3-pip python3-venv git
  2025-06-11 15:00:21.425 INFO -     cd /root
  2025-06-11 15:00:21.425 INFO -     git clone https://github.com/SETI/rms-cloud-tasks.git
  2025-06-11 15:00:21.425 INFO -     cd rms-cloud-tasks
  2025-06-11 15:00:21.425 INFO -     python3 -m venv venv
  2025-06-11 15:00:21.425 INFO -     source venv/bin/activate
  2025-06-11 15:00:21.425 INFO -     pip install -e .
  2025-06-11 15:00:21.425 INFO -     pip install -r examples/parallel_addition/requirements.txt
  2025-06-11 15:00:21.425 INFO -     export ADDITION_OUTPUT_DIR=gs://<BUCKET_NAME>/addition-results
  2025-06-11 15:00:21.425 INFO -     export ADDITION_TASK_DELAY=1
  2025-06-11 15:00:21.425 INFO -     python3 examples/parallel_addition/worker_addition.py
  2025-06-11 15:00:21.425 INFO - Starting orchestrator
  2025-06-11 15:00:22.076 INFO - Initializing GCP Pub/Sub queue "parallel-addition-job" with project ID "<PROJECT_ID>"
  2025-06-11 15:00:22.076 INFO - Using default application credentials
  2025-06-11 15:00:23.982 INFO - Using current default image: https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-noble-amd64-v20250606
  2025-06-11 15:00:23.983 WARNING - No boot disk types specified; this will make all relevant types available and likely result in the selection of the slowest boot disk available
  [...]
  2025-06-11 15:00:35.412 INFO - || Selected instance type: e2-standard-8 (pd-standard) in us-central1-* at $0.268614/hour
  2025-06-11 15:00:35.412 INFO - ||   8 vCPUs, 32.0 GB RAM, no local SSD
  2025-06-11 15:00:35.412 INFO - || Derived boot disk size: 10.0 GB
  2025-06-11 15:00:35.412 INFO - || Derived number of tasks per instance: 8
  2025-06-11 15:00:35.412 INFO - Checking if scaling is needed...
  2025-06-11 15:00:36.124 INFO - Current queue depth: 10000
  [...]
  2025-06-11 15:00:39.365 INFO - No running instances found
  2025-06-11 15:00:39.365 INFO - Starting 5 new instances for an incremental price of $1.34/hour
  2025-06-11 15:00:51.905 INFO - Started on-demand instance 'rmscr-parallel-addition-job-4jusrwvupyetlyvej11cszf32' in zone 'us-central1-c'
  2025-06-11 15:00:53.015 INFO - Started on-demand instance 'rmscr-parallel-addition-job-730w4d0qfw20mt7qpskvfan4h' in zone 'us-central1-c'
  2025-06-11 15:01:36.712 INFO - Started on-demand instance 'rmscr-parallel-addition-job-1uu0epqsfoncbznvp9yikh933' in zone 'us-central1-f'
  2025-06-11 15:02:11.421 INFO - Started on-demand instance 'rmscr-parallel-addition-job-aln9ha10xq4zexj59i085l0tx' in zone 'us-central1-f'
  2025-06-11 15:02:11.798 INFO - Started on-demand instance 'rmscr-parallel-addition-job-4ufccfcywtpdgrtg9jdm4s83f' in zone 'us-central1-f'
  2025-06-11 15:02:11.798 INFO - Successfully provisioned 5 of 5 requested instances
  2025-06-11 15:03:11.863 INFO - Checking if scaling is needed...
  2025-06-11 15:03:19.008 INFO - Current queue depth: 10
  2025-06-11 15:03:23.936 INFO - Running instance summary:
  2025-06-11 15:03:23.936 INFO -   State       Instance Type             Boot Disk    vCPUs  Zone             Count  Total Price
  2025-06-11 15:03:23.936 INFO -   ---------------------------------------------------------------------------------------------
  2025-06-11 15:03:23.936 INFO -   running     e2-standard-8             pd-standard      8  us-central1-c        2        $0.54
  2025-06-11 15:03:23.936 INFO -   running     e2-standard-8             pd-standard      8  us-central1-f        3        $0.81
  2025-06-11 15:03:23.936 INFO -   ---------------------------------------------------------------------------------------------
  2025-06-11 15:03:23.936 INFO -   Total running/starting:                               40 (weighted)            5        $1.34
  2025-06-11 15:03:23.936 INFO -

.. note::
  ``manage_pool`` uses INFO logging which is turned off by default. Be sure to specify `-v` to
  see the output.

Monitoring the Results
++++++++++++++++++++++

By default, the task manager running on each instance will send events (task completed, task failed,
unhandled exception occurred, etc.) to the event queue. The ``monitor_event_queue`` command can be
used to read this queue and write the events to a file while also collecting statistics and
comparing the list of completed tasks against the original task list. This command should be run
in a separate terminal from the one running the ``manage_pool`` command. The ``manage_pool`` command
needs to continue to run to keep track of the running instances and to start new ones as needed
if existing instances are terminated. In addition, once the task queue is empty, ``manage_pool``
will terminate all instances (see below).

.. code-block:: bash

  cloud_tasks monitor_event_queue --config examples/parallel_addition/config.yml --project-id <PROJECT_ID> --output-file addition_events.log --task-file examples/parallel_addition/addition_tasks.json

This will start a real-time monitor that will produce an output similar to this:

.. code-block:: none

  Reading tasks from "examples/parallel_addition/addition_tasks.json"
  Reading previous events from "addition_events.log"
  Monitoring event queue 'parallel-addition-job-events' on GCP...

  Summary:
    10000 tasks have not been completed without retry

  {"timestamp": "2025-06-11T22:05:05.119663", "hostname": "rmscr-parallel-addition-job-1uu0epqsfoncbznvp9yikh933", "event_type": "task_completed", "task_id": "addition-task-002057", "elapsed_time": 1.1852774620056152, "retry": false, "result": "gs://rms-nav-test-addition/addition-results/addition-task-002057.txt"}
  {"timestamp": "2025-06-11T22:05:07.510640", "hostname": "rmscr-parallel-addition-job-1uu0epqsfoncbznvp9yikh933", "event_type": "task_completed", "task_id": "addition-task-002099", "elapsed_time": 2.007458209991455, "retry": false, "result": "gs://rms-nav-test-addition/addition-results/addition-task-002099.txt"}

  [...]

  Summary:
    9900 tasks have not been completed without retry
    Task event status:
      task_completed      (retry=False):    100
    Tasks completed: 100 in 276.28 seconds (2.76 seconds/task)
    Elapsed time statistics:
      Range:  1.10 to 2.54 seconds
      Mean:   1.42 +/- 0.36 seconds
      Median: 1.23 seconds
      90th %: 1.98 seconds
      95th %: 2.26 seconds

Eventually once all tasks have been completed, the output will look like this:

.. code-block:: none

  Summary:
    0 tasks have not been completed with retry=False
    21 tasks completed with retry=False more than once but shouldn't have
    Task event status:
      task_completed      (retry=False):  10000
    Tasks completed: 10000 in 507.27 seconds (0.05 seconds/task)
    Elapsed time statistics:
      Range:  1.08 to 19.36 seconds
      Mean:   1.34 +/- 0.85 seconds
      Median: 1.19 seconds
      90th %: 1.69 seconds
      95th %: 1.99 seconds
    Remaining tasks:

The "21 tasks completed with retry=False more than once but shouldn't have" is due to the
fact that the task queue will deliver each task at least once, but may deliver it more
than once, to a worker process. In this case 21 out of 10,000 tasks were repeated and
didn't need to be.

Terminating the Instances
+++++++++++++++++++++++++

Once the task queue is empty, ``manage_pool`` will start a termination timer that
allows any remaining tasks to finish, and then will terminate all instances.

.. code-block:: none

  2025-06-11 16:08:24.348 INFO - Current queue depth: 0
  2025-06-11 16:08:24.348 INFO - Queue is empty, starting termination timer
  2025-06-11 16:09:24.406 INFO - Checking if scaling is needed...
  2025-06-11 16:09:25.097 INFO - Current queue depth: 0
  2025-06-11 16:09:25.097 INFO - Queue has been empty for 60.7 seconds
  2025-06-11 16:09:25.097 INFO - TERMINATION TIMER EXPIRED - TERMINATING ALL INSTANCES
  2025-06-11 16:09:25.098 INFO - Terminating all instances
  2025-06-11 16:09:28.449 INFO - Terminating instance: rmscr-parallel-addition-job-4jusrwvupyetlyvej11cszf32
  2025-06-11 16:09:28.449 INFO - Terminating instance: rmscr-parallel-addition-job-730w4d0qfw20mt7qpskvfan4h
  2025-06-11 16:09:28.450 INFO - Terminating instance: rmscr-parallel-addition-job-1uu0epqsfoncbznvp9yikh933
  2025-06-11 16:09:28.451 INFO - Terminating instance: rmscr-parallel-addition-job-4ufccfcywtpdgrtg9jdm4s83f
  2025-06-11 16:09:28.452 INFO - Terminating instance: rmscr-parallel-addition-job-aln9ha10xq4zexj59i085l0tx
  2025-06-11 16:09:28.453 INFO - Job management complete
  2025-06-11 16:09:28.453 INFO - Scaling loop cancelled


Version 2: Addition with Exceptions and Timeouts
------------------------------------------------

A second version of the parallel addition example is provided in the same directory. This
example, ``worker_addition_exceptions.py``, adds the ability to raise exceptions and other
types of errors during task execution, allowing you to see how event monitoring works in
these situations and to experiment with the different ``--retry`` options. You should use
the configuration file ``config_exceptions.yml`` for this version if running in the cloud.
The following environment variables are added:

- ``ADDITION_EXCEPTION_PROBABILITY``: The probability (0-1) that a DivideByZeroError will be raised.
- ``ADDITION_TIMEOUT_PROBABILITY``: The probability (0-1) that a timeout will occur (the task will
  sleep for 100,000 seconds).
- ``ADDITION_EXIT_PROBABILITY``: The probability (0-1) that the task will exit prematurely with a
  non-zero exit code.


Version 3: Addition with a Task Factory
---------------------------------------

A third version of the parallel addition example is provided in the same directory. This
example, ``worker_addition_factory.py``, uses a task factory function to generate tasks
instead of an external task file or queue. The task factory function is defined in the
same file. The following environment variables are added:

- ``ADDITION_MAX_TASKS``: The maximum number of tasks to generate.

This version is most useful when run locally, since the lack of a task queue eliminates the
ability to distribute tasks to multiple instances. It is most simply run with:

.. code-block:: bash

  python3 examples/parallel_addition/worker_addition_factory.py

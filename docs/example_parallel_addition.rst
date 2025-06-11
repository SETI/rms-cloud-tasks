Parallel Addition Example
=========================

A simple example is included in the ``rms-cloud-tasks`` repo. The task accepts two integers, adds them
together, and stores the result in a cloud bucket. It can then delay a programmable amount of time
to simulate a task that takes more time and emphasize the need for parallelism. The example includes
a file describing 10,000 tasks. If the delay is set to 1 second, this means the complete set of
tasks will require 10,000 CPU-seconds, or about 2.8 hours on a single CPU. Running with 100-fold
parallelism will reduce the time to around two minutes.

Specifying Tasks
----------------

The task queue is stored in whatever queueing system is native to the cloud provider being used.
Tasks are specified using a JSON file consisting of a list of dictionaries with the format:

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

To load the tasks into the queue, you run the ``cloud_tasks`` command line program with, at
a minimum, the name of the cloud provider and a job ID. These can also be specified in a
configuration file. For Google Cloud you also need to specify the project ID. For our sample
addition task, we will get the job ID from a configuration file and specify the provider
and project ID on the command line, since these are user-specific. The configuration file
and tasks list are available in the ``rms-cloud-tasks`` repo:

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
-------------

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
  ``manage_pool`` uses info logging which is turned off by default. Be sure to specify `-v` to
  see the output.

Monitor the Results
-------------------

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

Terminate the Instances
-----------------------

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

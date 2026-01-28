Command Line Interface Reference
================================

This page provides a comprehensive reference for the Cloud Tasks command-line interface,
``cloud_tasks``. The general format of a command is:

.. code-block:: none

   cloud_tasks <command> <options>

To get a list of available commands, run:

.. code-block:: none

   cloud_tasks --help

To get a list of options for a specific command, run:

.. code-block:: none

   cloud_tasks <command> --help

Since many commands take similar options, we will start by listing the
shared options and then reference them as needed.


Command Line Options
--------------------


.. _cli_common_options:

Common Options
~~~~~~~~~~~~~~

All commands support these common options:

--config CONFIG        Path to configuration file (optional if no configuration file is needed)
--provider PROVIDER    Cloud provider (aws or gcp), overrides configuration file
--verbose, -v          Enable verbose output (-v for INFO, -vv for DEBUG, default is WARNING)


.. _cli_job_specific_options:

Job-Specific Options
~~~~~~~~~~~~~~~~~~~~

In addition to the common options, each job-specific command has additional options that
specify job-related information. They override any options in the configuration file (see
:ref:`config_provider_specific_options`).

--job-id JOB_ID            A unique identifier for the job
--queue-name QUEUE_NAME    The name of the task queue to use (derived from job ID if not provided;
                           only use this in special circumstances)
--region REGION            The region to use (derived from zone if not provided)
--zone ZONE                The zone to use (if not specified, all zones in a region will be used)
--exactly-once-queue       If specified, task and event queue messages are guaranteed to be delivered
                            exactly once to any recipient
--no-exactly-once-queue    If specified, task and event queue messages are delivered at least once,
                            but could be delivered multiple times


.. _cli_provider_specific_options:

Provider-Specific Options
~~~~~~~~~~~~~~~~~~~~~~~~~

In addition to the common options, each provider has additional options that are specific
to that provider. They override any options in the configuration file (see
:ref:`config_provider_specific_options`).

AWS
+++

--access-key ACCESS_KEY       The access key to use
--secret-key SECRET_KEY       The secret key to use

GCP
+++

--project-id PROJECT_ID                The ID of the project to use [Required for most operations]
--credentials-file CREDENTIALS_FILE    The path to a file containing the credentials to use; if not
                                       specified, the default credentials will be used
--service-account SERVICE_ACCOUNT      The service account to use; required for worker processes
                                       on cloud-based instances to have access to system resources [Required when creating
                                       instances]


.. _cli_instance_type_selection_options:

Instance Type Selection Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These options are used to constrain the instance types. They override any constraints
in the configuration file (see :ref:`config_compute_instance_options`).

--architecture ARCHITECTURE   The architecture to use; valid values are ``X86_64`` and ``ARM64``
                              (defaults to ``X86_64``)
--cpu-family CPU_FAMILY       The CPU family to use, for example ``Intel Cascade Lake`` or ``AMD Genoa``.
--min-cpu-rank MIN_CPU_RANK   The minimum CPU performance rank to use (0 is the slowest)
--max-cpu-rank MAX_CPU_RANK   The maximum CPU performance rank to use (0 is the slowest)
--instance-types TYPES        A single instance type or list of instance types to use;
                              instance types are specified using Python-style regular expressions
                              (if no anchor character like ``^`` or ``$`` is specified, the given
                              string will match any part of the instance type name)
--min-cpu N                   The minimum number of vCPUs per instance
--max-cpu N                   The maximum number of vCPUs per instance
--cpus-per-task N             The number of vCPUs per task; this is also used to configure
                              the worker process to limit the number of tasks that can be run
                              simultaneously on a single instance
--min-tasks-per-instance N    The minimum number of tasks per instance
--max-tasks-per-instance N    The maximum number of tasks per instance
--min-total-memory N          The minimum amount of memory in GB per instance
--max-total-memory N          The maximum amount of memory in GB per instance
--min-memory-per-cpu N        The minimum amount of memory per vCPU
--max-memory-per-cpu N        The maximum amount of memory per vCPU
--min-memory-per-task N       The minimum amount of memory per task
--max-memory-per-task N       The maximum amount of memory per task
--min-local-ssd N             The minimum amount of local extra SSD storage in GB per instance
--max-local-ssd N             The maximum amount of local extra SSD storage in GB per instance
--local-ssd-base-size N       The base size of the local extra SSD storage in GB per instance
--min-local-ssd-per-cpu N     The minimum amount of local extra SSD storage per vCPU
--max-local-ssd-per-cpu N     The maximum amount of local extra SSD storage per vCPU
--min-local-ssd-per-task N    The minimum amount of local extra SSD storage per task
--max-local-ssd-per-task N    The maximum amount of local extra SSD storage per task
--total-boot-disk-size N      The total size of the boot disk in GB per instance
--boot-disk-base-size N       The base size of the boot disk in GB per instance
--boot-disk-per-cpu N         The amount of boot disk per vCPU
--boot-disk-per-task N        The amount of boot disk per task
--boot-disk-types TYPES       The types of boot disks to use
--boot-disk-iops N            The number of provisioned IOPS for the boot disk, if applicable
--boot-disk-throughput N      The number of provisioned throughput in MB/s for the boot disk, if applicable


.. _cli_number_of_instances_options:

Number of Instances Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~

These options are used to constrain the number of instances. They override any constraints
in the configuration file (see :ref:`config_number_of_instances_options`).

--min-instances N             The minimum number of instances to use (defaults to 1)
--max-instances N             The maximum number of instances to use (defaults to 10)
--min-total-cpus N            The minimum total number of vCPUs to use
--max-total-cpus N            The maximum total number of vCPUs to use
--cpus-per-task N             The number of vCPUs per task; this is also used to configure
                              the worker process to limit the number of tasks that can be run
                              simultaneously on a single instance
--min-tasks-per-instance N    The minimum number of tasks per instance
--max-tasks-per-instance N    The maximum number of tasks per instance
--min-simultaneous-tasks N    The minimum number of tasks to run simultaneously
--max-simultaneous-tasks N    The maximum number of tasks to run simultaneously
--min-total-price-per-hour N  The minimum total price per hour to use
--max-total-price-per-hour N  The maximum total price per hour to use (defaults to 10)


.. _cli_vm_options:

VM Options
~~~~~~~~~~

These options are used to specify the type of VM to use. They override any options
in the configuration file (see :ref:`config_vm_options`).

--use-spot                    Use spot instances instead of on-demand instances


.. _cli_boot_options:

Boot Options
~~~~~~~~~~~~

These options are used to specify the boot process. They override any options
in the configuration file (see :ref:`config_boot_options`).

--startup-script-file FILE    The path to a file containing the startup script
--image IMAGE                 The image to use for the VM


.. _cli_worker_and_manage_pool_options:

Worker and Run Options
~~~~~~~~~~~~~~~~~~~~~~

These options are used to specify the worker and run process behavior. They override any
options in the configuration file (see :ref:`config_worker_and_manage_pool_options`).

--scaling-check-interval SECONDS       The interval to check for scaling opportunities
                                       (defaults to 60)
--instance-termination-delay SECONDS   The delay to wait before terminating an instance
                                       (defaults to 60)
--max-runtime SECONDS                  The maximum runtime for a task (defaults to 60)
--retry-on-exit                        If specified, tasks will be retried if the worker exits
                                       prematurely, e.g. due to a crash
--no-retry-on-exit                     If specified, tasks will not be retried if the worker exits
                                       prematurely, e.g. due to a crash (default)
--retry-on-exception                   If specified, tasks will be retried if the user function
                                       raises an unhandled exception
--no-retry-on-exception                If specified, tasks will not be retried if the user function
                                       raises an unhandled exception (default)
--retry-on-timeout                     If specified, tasks will be retried if they exceed the
                                       maximum runtime specified by --max-runtime
--no-retry-on-timeout                  If specified, tasks will not be retried if they exceed the
                                       maximum runtime specified by --max-runtime (default)


.. _cli_information_commands:

Information Commands
--------------------


.. _cli_list_regions:

list_regions
~~~~~~~~~~~~

List available regions, and optionally availability zones and other details, for a
provider.

.. code-block:: none

   cloud_tasks list_regions
     [Common options]
     [Provider-specific options]
     [Additional options]

Additional options:

--prefix PREFIX      Filter regions by name prefix
--zones              Show availability zones for each region
--detail             Show additional provider-specific information

Examples:

.. tabs::

   .. tab:: AWS

      .. code-block:: none

         XXX Update
         $ cloud_tasks list_regions --provider aws --detail --zones --prefix us-west
         Found 2 regions (filtered by prefix: us-west)

         Region                    Description
         ----------------------------------------------------------------------------------------------------
         us-west-1                 AWS Region us-west-1
         Availability Zones: us-west-1a, us-west-1b
         Opt-in Status: opt-in-not-required

         us-west-2                 AWS Region us-west-2
         Availability Zones: us-west-2a, us-west-2b, us-west-2c, us-west-2d
         Opt-in Status: opt-in-not-required

   .. tab:: GCP

      .. code-block:: none

         $ cloud_tasks list_regions --provider gcp --detail --zones --prefix us-west
         Found 4 regions (filtered by prefix: us-west)

         Region: us-west1
         Description: us-west1
         Zones: us-west1-a, us-west1-b, us-west1-c
         Endpoint: https://us-west1-compute.googleapis.com
         Status: UP

         Region: us-west2
         Description: us-west2
         Zones: us-west2-a, us-west2-b, us-west2-c
         Endpoint: https://us-west2-compute.googleapis.com
         Status: UP

         Region: us-west3
         Description: us-west3
         Zones: us-west3-a, us-west3-b, us-west3-c
         Endpoint: https://us-west3-compute.googleapis.com
         Status: UP

         Region: us-west4
         Description: us-west4
         Zones: us-west4-a, us-west4-b, us-west4-c
         Endpoint: https://us-west4-compute.googleapis.com
         Status: UP


.. _cli_list_images:

list_images
~~~~~~~~~~~

List available VM images.

.. code-block:: none

   cloud_tasks list_images
     [Common options]
     [Provider-specific options]
     [Additional options]

Additional options:

--user            Include user-created images; otherwise, only include system-provided
                  public images
--filter TEXT     Include only images containing ``TEXT`` in any field
--sort-by FIELDS  Sort the result by one or more comma-separated fields; available fields
                  are ``family``, ``name``, ``project``, ``source``. Prefix with ``-`` for
                  descending order. Partial field names like ``fam`` for ``family`` or ``proj``
                  for ``project`` are supported.
--limit N         Limit the number of results to the first ``N`` after sorting
--detail          Show detailed information

Examples:

.. tabs::

   .. tab:: AWS

      .. code-block:: none

         XXX Update
         $ cloud_tasks list_images --provider aws --filter sapcal --detail --sort-by=-name --limit 2
         Retrieving images...
         Found 2 filtered images for aws:

         Name                                                                             Source
         ------------------------------------------------------------------------------------------
         suse-sles-15-sp6-sapcal-v20250409-hvm-ssd-x86_64                                 AWS
         SUSE Linux Enterprise Server 15 SP6 for SAP CAL (HVM, 64-bit, SSD Backed)
         ID: ami-09b43f66ab9cce59a
         CREATION DATE: 2025-04-09T21:15:49.000Z    STATUS: available
         URL: N/A

         suse-sles-15-sp6-sapcal-v20250130-hvm-ssd-x86_64                                 AWS
         SUSE Linux Enterprise Server 15 SP6 for SAP CAL (HVM, 64-bit, SSD Backed)
         ID: ami-013778510a6146053
         CREATION DATE: 2025-01-31T12:06:46.000Z    STATUS: available
         URL: N/A


         To use a custom image with the 'run' command, use the --image parameter.
         For AWS, specify the AMI ID: --image ami-12345678

   .. tab:: GCP

      .. code-block:: none

         $ cloud_tasks list_images --provider gcp --filter centos --detail --sort-by=-name --limit 2
         Retrieving images...
         Found 2 filtered images for GCP:

         Family:  centos-stream-9
         Name:    centos-stream-9-v20250513
         Project: centos-cloud
         Source:  GCP
         CentOS, CentOS, Stream 9, x86_64 built on 20250513
         ID: 1983115583357351998       CREATION DATE: 2025-05-13T15:25:22.322-07:00       STATUS: READY
         URL: https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-stream-9-v20250513

         Family:  centos-stream-9-arm64
         Name:    centos-stream-9-arm64-v20250513
         Project: centos-cloud
         Source:  GCP
         CentOS, CentOS, Stream 9, aarch64 built on 20250513
         ID: 4641848378514110526       CREATION DATE: 2025-05-13T15:25:22.160-07:00       STATUS: READY
         URL: https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-stream-9-arm64-v20250513


         To use a custom image with the 'run' command, use the --image parameter.
         For GCP, specify the image family or full URI: --image ubuntu-2404-lts or --image https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-lts-amd64-v20240416

.. _cli_list_instance_types:

list_instance_types
~~~~~~~~~~~~~~~~~~~

List available instance types with pricing.

.. code-block:: none

   cloud_tasks list_instance_types
     [Common options]
     [Provider-specific options]
     [Instance type selection options]
     [VM options]
     [Additional options]

Additional options:

--filter TEXT     Include only images containing ``TEXT`` in any field
--sort-by FIELDS  Sort the result by one or more comma-separated fields; available fields
                  are ``name``, ``vcpu``, ``mem``, ``local_ssd``, ``storage``,
                  ``vcpu_price``, ``mem_price``, ``local_ssd_price``, ``boot_disk_price``,
                  ``boot_disk_iops_price``, ``boot_disk_throughput_price``,
                  ``price_per_cpu``, ``mem_per_gb_price``, ``local_ssd_per_gb_price``,
                  ``boot_disk_per_gb_price``, ``total_price``, ``total_price_per_cpu``,
                  ``zone``, ``processor_type``, ``performance_rank``, ``description``.
                  Prefix with ``-`` for descending order.
                  Partial field names like ``ram`` or ``mem`` for ``mem_gb`` or ``v`` for
                  ``vcpu`` are supported.
--limit N         Limit the number of results to the first ``N`` after sorting
--detail          Show detailed information

Examples:

.. tabs::

   .. tab:: AWS

      .. code-block:: none

         XXX Update
         $ cloud_tasks list_instance_types --provider aws --region us-west-1 --instance-types "m4.*" --sort-by=-cpu,-mem --limit 5
         Retrieving instance types...
         Retrieving pricing information...

         Instance Type                  Arch vCPU   Mem (GB)  LSSD (GB)  Disk (GB)  Total $/Hr         Zone
         -----------------------------------------------------------------------------------------------------------
         m4.16xlarge                  x86_64   64      256.0          0          0     $3.7440  us-west-1-*
         m4.10xlarge                  x86_64   40      160.0          0          0     $2.3400  us-west-1-*
         m4.4xlarge                   x86_64   16       64.0          0          0     $0.9360  us-west-1-*
         m4.2xlarge                   x86_64    8       32.0          0          0     $0.4680  us-west-1-*
         m4.xlarge                    x86_64    4       16.0          0          0     $0.2340  us-west-1-*

   .. tab:: GCP

      .. code-block:: none

         $ cloud_tasks list_instance_types --provider gcp --region us-central1 --instance-types "n.-.*" --sort-by=-cpu,-mem --limit 5
         Retrieving instance types...
         Retrieving pricing information...

         ┌─────────────────┬────────┬──────┬─────────┬───────┬─────────────┬──────────┬───────────────┐
         │ Instance Type   │ Arch   │ vCPU │     Mem │  Disk │ Boot        │  Total $ │ Zone          │
         │                 │        │      │    (GB) │  (GB) │ Disk Type   │    (/Hr) │               │
         ├─────────────────┼────────┼──────┼─────────┼───────┼─────────────┼──────────┼───────────────┤
         │ n1-ultramem-160 │ X86_64 │  160 │ 3844.00 │ 10.00 │ pd-standard │ $21.3453 │ us-central1-* │
         │ n1-ultramem-160 │ X86_64 │  160 │ 3844.00 │ 10.00 │ pd-balanced │ $21.3462 │ us-central1-* │
         │ n1-ultramem-160 │ X86_64 │  160 │ 3844.00 │ 10.00 │ pd-extreme  │ $21.6241 │ us-central1-* │
         │ n1-ultramem-160 │ X86_64 │  160 │ 3844.00 │ 10.00 │ pd-ssd      │ $21.3471 │ us-central1-* │
         │ n2-highmem-128  │ X86_64 │  128 │  864.00 │ 10.00 │ pd-standard │  $7.7075 │ us-central1-* │
         └─────────────────┴────────┴──────┴─────────┴───────┴─────────────┴──────────┴───────────────┘


.. _cli_job_management_commands:

Job Management Commands
-----------------------


.. _cli_run_cmd:

run
~~~

The ``run`` command handles the complete task execution workflow in a single command.
It automates: queue management, task loading, instance orchestration, event monitoring,
and cleanup.

**Fresh Run Mode** (default): Deletes existing SQLite database and queues, creates new ones,
loads tasks into the database and cloud queue, manages instances, monitors events, and
automatically terminates instances and deletes queues when all tasks complete. If the existing
queue has messages, the user will be prompted for confirmation unless ``--force`` is specified.

**Continue Mode** (``--continue``): Resumes from a previous interrupted run by reading the
SQLite database, draining pending events, and continuing to manage instances until completion.

.. code-block:: none

   cloud_tasks run
     [Common options]
     [Provider-specific options]
     [Job-specific options]
     [Instance type selection options]
     [Number of instances options]
     [VM options]
     [Boot options]
     [Worker and Manage Pool options]
     [Additional options]

Additional options:

--task-file TASK_FILE                 Path to task file (JSON or YAML); required for fresh runs,
                                      not used with --continue
--start-task N                        Skip tasks until this task number (1-based)
--limit N                             Maximum number of tasks to enqueue
--max-concurrent-queue-operations N   Maximum concurrent tasks to enqueue (default: 100)
--continue                            Resume from a previous interrupted run using the existing
                                      SQLite database and cloud state
--db-file DB_FILE                     Path to SQLite database file (default: {job_id}.db);
                                      can also be set in configuration file under run.db_file
--output-file OUTPUT_FILE             Optional file to write events to in JSON-lines format
                                      in addition to the SQLite database
--force, -f                           Force fresh run without confirmation even if queue has
                                      existing messages
--dry-run                             Do not actually load any tasks or create or delete any
                                      instances

**SQLite Database**: The database file (default: ``{job_id}.db``) persistently tracks task
status and events. It contains:

- **tasks table**: task_id, task_data, status, retry flag, timestamps, hostname, results,
  exceptions, exit codes
- **events table**: Raw event log from workers

**Task Completion**: A task is considered complete when it has any status with ``retry=False``.

**Keyboard Interrupt (Ctrl+C)**: When interrupted, you are prompted:

.. code-block:: none

   Received interrupt.

   Choose action:
     [T] Terminate all instances and delete queues
     [L] Leave instances running (can resume with --continue)
     [C] Cancel and continue running

   Enter choice (T/L/C):

**Final Report**: Upon completion, a comprehensive report is printed with:

- Task counts by status
- Total elapsed time and throughput (tasks/hour)
- Task execution time statistics (range, mean, median, 90th/95th percentile)
- Exception summaries with counts
- Spot termination tracking

Examples:

.. tabs::

   .. tab:: AWS

      .. code-block:: bash

         # Fresh run
         cloud_tasks run --config myconfig.yml --task-file tasks.json

         # Resume after crash
         cloud_tasks run --config myconfig.yml --continue

         # With event logging
         cloud_tasks run --config myconfig.yml --task-file tasks.json --output-file events.json

   .. tab:: GCP

      .. code-block:: bash

         # Fresh run
         cloud_tasks run --config myconfig.yml --task-file tasks.json

         # Resume after crash
         cloud_tasks run --config myconfig.yml --continue

         # With custom database file
         cloud_tasks run --config myconfig.yml --task-file tasks.json --db-file custom.db


.. _cli_monitor_event_queue:

monitor_event_queue
~~~~~~~~~~~~~~~~~~~

Monitor the event queue and update task database. This command is useful when running workers
locally (not using cloud-managed instances) but still want automated event monitoring and
SQLite-based task tracking.

.. code-block:: none

   cloud_tasks monitor_event_queue
     [Common options]
     [Job-specific options]
     [Additional options]

Additional options:

--db-file DB_FILE                     Path to SQLite database file (default: {job_id}.db)
--output-file OUTPUT_FILE             Optional file to write events to in JSON-lines format
--print-events                        Print events to stdout as they are received
--no-auto-complete                    Don't stop automatically when all tasks complete

**Use Case:**

This command is designed for scenarios where you want to run workers manually (e.g., for local
testing or debugging) but still use the automated event monitoring and task tracking infrastructure.

**Workflow:**

1. Create queues and database (without starting cloud instances):

   .. code-block:: bash

      cloud-tasks run --config config.yml --task-file tasks.json --dry-run

2. Start workers locally in separate terminals:

   .. code-block:: bash

      # Terminal 1
      python your_worker.py --config config.yml

      # Terminal 2
      python your_worker.py --config config.yml

3. Monitor progress in another terminal:

   .. code-block:: bash

      cloud-tasks monitor_event_queue --config config.yml

**Behavior:**

- Opens an existing SQLite database (created by ``run --dry-run`` or a previous run)
- Monitors the event queue for task completion events from workers
- Updates the database with task status, results, and statistics
- Prints periodic status summaries to the console
- Automatically stops when all tasks complete (unless ``--no-auto-complete`` is specified)
- Optionally writes events to a JSON-lines output file for archival

**Comparison with ``run`` command:**

- ``run``: Creates and manages cloud instances + monitors events (full automation)
- ``monitor_event_queue``: Only monitors events (for manual worker management)

Examples:

.. tabs::

   .. tab:: AWS

      .. code-block:: bash

         # Monitor with default settings
         cloud_tasks monitor_event_queue --config config.yml

         # Monitor with output file
         cloud_tasks monitor_event_queue --config config.yml --output-file events.jsonl

         # Monitor indefinitely (don't stop when tasks complete)
         cloud_tasks monitor_event_queue --config config.yml --no-auto-complete

   .. tab:: GCP

      .. code-block:: bash

         # Monitor with default settings
         cloud_tasks monitor_event_queue --config config.yml

         # Monitor with custom database
         cloud_tasks monitor_event_queue --config config.yml --db-file my-job.db

         # Monitor and print events to stdout
         cloud_tasks monitor_event_queue --config config.yml --print-events


.. _cli_status_cmd:

status
~~~~~~

Check the status of a running job.

.. code-block:: none

   cloud_tasks status
     [Common options]
     [Provider-specific options]
     [Job-specific options]

Examples:

.. tabs::

   .. tab:: AWS

      TODO

   .. tab:: GCP

      .. code-block:: none

         $ cloud_tasks status --provider gcp --project my-project --job-id my-job --region us-central1
         Checking job status for job 'my-job'
         Running instance summary:
         State       Instance Type             vCPUs  Zone             Count  Total Price
         --------------------------------------------------------------------------------
         running     e2-micro                      2  us-central1-a        1        $0.05
         running     e2-micro                      2  us-central1-b        1        $0.05
         running     e2-micro                      2  us-central1-c        1        $0.05
         running     e2-micro                      2  us-central1-f        2        $0.09
         --------------------------------------------------------------------------------
         Total running/starting:                  10 (weighted)            5        $0.23

         Current queue depth: 10


.. _cli_stop_cmd:

stop
~~~~

Stop a job and terminate its instances.

.. code-block:: none

   cloud_tasks stop
     [Common options]
     [Provider-specific options]
     [Job-specific options]
     [Additional options]

Additional options:

--purge-queue           Purge the task queue after stopping instances

Examples:

.. tabs::

   .. tab:: AWS

      TODO

   .. tab:: GCP

      .. code-block:: none

         $ cloud_tasks stop --provider gcp --project my-project --job-id my-job --region us-central1
         Stopping job 'my-job'...this could take a few minutes
         Job 'my-job' stopped


.. _cli_list_running_instances:

list_running_instances
~~~~~~~~~~~~~~~~~~~~~~

List currently running instances. By default only active instances created by Cloud Tasks
are shown. If only a region is specified, instances in all zones in that region are shown. If a
zone is specified, only instances in that zone are shown.

.. code-block:: none

   cloud_tasks list_running_instances
     [Common options]
     [Provider-specific options]
     [Additional options]

Additional options:

--job-id JOB_ID         Filter by job ID
--all-instances         Show all instances including ones that were not created by Cloud Tasks
--include-terminated    Include terminated instances
--sort-by FIELDS        Sort results by comma-separated fields (e.g.,
                        "state,type" or "-created,id"). Available fields: id, type, state,
                        zone, creation_time. Prefix with "-" for descending order. Partial
                        field names like "t" for "type" or "s" for "state" are supported.
--detail                Show detailed information

Examples:

.. tabs::

   .. tab:: AWS

      TODO

   .. tab:: GCP

      .. code-block:: none

         $ cloud_tasks list_running_instances --provider gcp --project my-project --region us-central1 --all-instances --include-terminated
         Listing all instances including ones not created by cloud tasks

         ┌────────┬────────────────────────────────────────┬───────────────┬────────────┬───────────────┬───────────────────────────────┐
         │ Job ID │ ID                                     │ Type          │ State      │ Zone          │ Created                       │
         ├────────┼────────────────────────────────────────┼───────────────┼────────────┼───────────────┼───────────────────────────────┤
         │ N/A    │ instance-20250611-000830               │ n2-standard-2 │ running    │ us-central1-c │ 2025-06-10T17:08:56.319-07:00 │
         │ my-job │ rmscr-my-job-1kbha0cmxqz9snn27nznudpog │ e2-micro      │ running    │ us-central1-a │ 2025-06-10T17:05:30.915-07:00 │
         │ my-job │ rmscr-my-job-1zedunr983dvxnboyzc1d9va5 │ e2-micro      │ running    │ us-central1-a │ 2025-06-10T17:05:32.243-07:00 │
         │ my-job │ rmscr-my-job-2lkp755rhnael6vpo1glft2up │ e2-micro      │ terminated │ us-central1-a │ 2025-06-10T17:05:32.013-07:00 │
         │ my-job │ rmscr-my-job-5wleia6eb1yujw94ha1zkbk7y │ e2-micro      │ running    │ us-central1-f │ 2025-06-10T17:05:31.414-07:00 │
         │ my-job │ rmscr-my-job-eh98we6vp96atd7lytol78fp3 │ e2-micro      │ running    │ us-central1-f │ 2025-06-10T17:05:31.241-07:00 │
         └────────┴────────────────────────────────────────┴───────────────┴────────────┴───────────────┴───────────────────────────────┘

         Summary: 6 total instances
         5 running
         1 terminated


Queue Management Commands
-------------------------


.. _cli_show_queue_cmd:

show_queue
~~~~~~~~~~

Show information about a task queue. Note that some providers do not provide an accurate
count of messages remaining in a queue.

.. code-block:: none

   cloud_tasks show_queue
     [Common options]
     [Job-specific options]
     [Provider-specific options]
     [Additional options]

Additional options:

--detail          Show a sample message

Examples:

.. tabs::

   .. tab:: AWS

      .. code-block:: none

         $ cloud_tasks show_queue --provider aws --job-id my-job --detail
         Checking queue depth for 'my-job'...
         Current depth: 10000 message(s)

         Attempting to peek at first message...

         --------------------------------------------------
         SAMPLE MESSAGE
         --------------------------------------------------
         Task ID: addition-task-000035
         Receipt Handle: AQEBt0nqkqnpbta3H0OV62eJGwx6do5rXY8MW+NbGlnhwE0Etz...

         Data:
         {
         "num1": 1438,
         "num2": 49332
         }

         Note: Message was not removed from the queue.

   .. tab:: GCP

      .. code-block:: none

         $ cloud_tasks show_queue --provider gcp --job-id my-job --project my-project --detail
         Checking queue depth for 'my-job'...
         Current depth: 10

         Attempting to peek at first message...

         --------------------------------------------------
         SAMPLE MESSAGE
         --------------------------------------------------
         Task ID: addition-task-000011
         Ack ID: RkhRNxkIaFEOT14jPzUgKEUWAggUBXx9S1tTNA0UKRpQCh0dfW...

         Data:
         {
         "num1": 60977,
         "num2": 24891
         }

         Note: Message was not removed from the queue.


.. _cli_purge_queue_cmd:

purge_queue
~~~~~~~~~~~

Remove all messages from the task and event queues. This allows you to start fresh by loading
new tasks.

.. code-block:: none

   cloud_tasks purge_queue
     [Common options]
     [Job-specific options]
     [Provider-specific options]
     [Additional options]

Additional options:

--task-queue-only      Purge only the task queue (not the event queue)
--event-queue-only     Purge only the event queue (not the task queue)
--force                Purge without confirmation

Examples:

.. tabs::

   .. tab:: AWS

      .. code-block:: none

         XXX Update this
         $ cloud_tasks purge_queue --provider aws --job-id my-job

         WARNING: This will permanently delete all 10000+ messages from queue 'my-job' on 'AWS'.
         Type 'EMPTY my-job' to confirm: EMPTY my-job
         Emptying queue 'my-job'...
         Queue 'my-job' has been emptied. Removed 10000+ message(s).

   .. tab:: GCP

      .. code-block:: none

         ❯ cloud_tasks purge_queue --provider gcp --job-id my-job --project my-project

         WARNING: This will permanently delete all 10+ messages from queue 'my-job' on 'GCP'.
         Type 'EMPTY my-job' to confirm: EMPTY my-job
         Emptying queue 'my-job'...
         Queue 'my-job' has been emptied. Removed 10+ message(s).


.. _cli_delete_queue_cmd:

delete_queue
~~~~~~~~~~~~

Delete the task and event queues and their infrastructure. This permanently frees up the
resources used by the queues. Only do this if there are no processes running that use the
queues.

.. code-block:: none

   cloud_tasks purge_queue
     [Common options]
     [Job-specific options]
     [Provider-specific options]
     [Additional options]

Additional options:

--task-queue-only      Delete only the task queue (not the event queue)
--event-queue-only     Delete only the event queue (not the task queue)
--force                Delete without confirmation

Examples:

.. tabs::

   .. tab:: AWS

      .. code-block:: none

         $ cloud_tasks delete_queue --provider aws --job-id my-job

         WARNING: This will permanently delete the queue 'my-job' from AWS.
         This operation cannot be undone and will remove all infrastructure.
         Type 'DELETE my-job' to confirm: DELETE my-job
         Deleting queue 'my-job' from AWS...
         Queue 'my-job' has been deleted.

         WARNING: This will permanently delete the queue 'my-job-results' from AWS.
         This operation cannot be undone and will remove all infrastructure.
         Type 'DELETE my-job-results' to confirm: DELETE my-job-results
         Deleting queue 'my-job-results' from AWS...
         Queue 'my-job-results' has been deleted.

   .. tab:: GCP

      .. code-block:: none

         $ cloud_tasks delete_queue --provider gcp --job-id my-job --project my-project

         WARNING: This will permanently delete the queue 'my-job' from GCP.
         This operation cannot be undone and will remove all infrastructure.
         Type 'DELETE my-job' to confirm: DELETE my-job
         Deleting queue 'my-job' from GCP...
         Queue 'my-job' has been deleted.

         WARNING: This will permanently delete the queue 'my-job-results' from GCP.
         This operation cannot be undone and will remove all infrastructure.
         Type 'DELETE my-job-results' to confirm: DELETE my-job-results
         Deleting queue 'my-job-results' from GCP...
         Queue 'my-job-results' has been deleted.


Exit Status
-----------

The CLI returns the following exit codes:

* 0 - Success
* 1 - Error occurred during command execution

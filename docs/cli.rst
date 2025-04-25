Command Line Interface Reference
================================

This page provides a comprehensive reference for the Cloud Tasks command-line interface,
``cloud_tasks``. Since many commands take similar options, we will start by listing the
shared options and then reference them as needed.

Common Options
--------------

All commands support these common options:

--config CONFIG        Path to configuration file (optional if no configuration file is needed)
--provider PROVIDER    Cloud provider (aws, gcp, or azure), overrides configuration file
--verbose, -v          Enable verbose output (-v for warning, -vv for info, -vvv for debug)

Job-Specific Options
---------------------

In addition to the common options, each job-specific command has additional options that
specify job-related information. They override any options in the configuration file (see
:ref:`config_provider_specific_options`).

--job-id JOB_ID            A unique identifier for the job
--queue-name QUEUE_NAME    The name of the task queue to use (derived from job ID if not provided)

Provider-Specific Options
-------------------------

In addition to the common options, each provider has additional options that are specific
to that provider. They override any options in the configuration file (see
:ref:`config_provider_specific_options`).

AWS
~~~

--access-key ACCESS_KEY       The access key to use
--secret-key SECRET_KEY       The secret key to use

GCP
~~~

--project-id PROJECT_ID                The ID of the project to use [Required for most operations]
--credentials-file CREDENTIALS_FILE    The path to a file containing the credentials to use; if not
                                       specified, the default credentials will be used
--service-account SERVICE_ACCOUNT      The service account to use; required for worker processes
                                       on cloud-based instances to have access to system resources [Required when creating
                                       instances]

Azure
~~~~~

--subscription-id SUBSCRIPTION_ID    The ID of the subscription to use
--tenant-id TENANT_ID                The ID of the tenant to use
--client-id CLIENT_ID                The ID of the client to use
--client-secret CLIENT_SECRET        The secret to use

Instance Type Selection Options
-------------------------------

These options are used to constrain the instance types. They override any constraints
in the configuration file (see :ref:`config_compute_instance_options`).

--architecture ARCHITECTURE   The architecture to use; valid values are ``X86_64`` and ``ARM64``
                              (defaults to ``X86_64``)
--min-cpu N                   The minimum number of vCPUs per instance
--max-cpu N                   The maximum number of vCPUs per instance
--min-total-memory N          The minimum amount of memory in GB per instance
--max-total-memory N          The maximum amount of memory in GB per instance
--min-memory-per-cpu N        The minimum amount of memory per vCPU
--max-memory-per-cpu N        The maximum amount of memory per vCPU
--min-local-ssd N             The minimum amount of local extra SSD storage in GB per instance
--max-local-ssd N             The maximum amount of local extra SSD storage in GB per instance
--min-local-ssd-per-cpu N     The minimum amount of local extra SSD storage per vCPU
--max-local-ssd-per-cpu N     The maximum amount of local extra SSD storage per vCPU
--min-boot-disk N             The minimum amount of boot disk in GB per instance
--max-boot-disk N             The maximum amount of boot disk in GB per instance
--min-boot-disk-per-cpu N     The minimum amount of boot disk per vCPU
--max-boot-disk-per-cpu N     The maximum amount of boot disk per vCPU
--instance-types TYPES        A single instance type or list of instance types to use;
                              instance types are specified using Python-style regular expressions
                              (if no anchor character like ``^`` or ``$`` is specified, the given
                              string will match any part of the instance type name)

Number of Instances Options
---------------------------

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
--max-total-price-per-hour N  The maximum total price per hour to use

VM Options
----------

These options are used to specify the type of VM to use. They override any options
in the configuration file (see :ref:`config_vm_options`).

--use-spot                    Use spot instances instead of on-demand instances

Boot Options
------------

These options are used to specify the boot process. They override any options
in the configuration file (see :ref:`config_boot_options`).

--startup-script-file FILE    The path to a file containing the startup script
--image IMAGE                 The image to use for the VM

Worker and Manage Pool Options
------------------------------

These options are used to specify the worker and manage_pool processes. They override any
options in the configuration file (see :ref:`config_worker_and_manage_pool_options`).

--scaling-check-interval SECONDS       The interval to check for scaling opportunities
                                       (defaults to 60)
--instance-termination-delay SECONDS   The delay to wait before terminating an instance
                                       (defaults to 60)
--max-runtime SECONDS                  The maximum runtime for a task (defaults to 60)
--worker-use-new-process               Use a new process for each task instead of reusing the
                                       same process (defaults to ``False``)

Information Commands
--------------------

list_regions
~~~~~~~~~~~~

List available regions, and optionally availability zones and other details, for a
provider.

.. code-block:: none

   cloud_tasks list_regions
      [Common options]
      [Additional options]

Additional options:

--prefix PREFIX      Filter regions by name prefix
--zones              Show availability zones for each region
--detail             Show additional provider-specific information

Example:

.. code-block:: none

   $ cloud_tasks list_regions --provider gcp --detail --zones --prefix africa
   Found 1 regions (filtered by prefix: africa)

   Region                    Description
   ----------------------------------------------------------------------------------------------------
   africa-south1             africa-south1
   Availability Zones: africa-south1-a, africa-south1-b, africa-south1-c
   Endpoint: https://africa-south1-compute.googleapis.com
   Status: UP

list_images
~~~~~~~~~~~

List available VM images.

.. code-block:: none

   cloud_tasks list_images
      [Common options]
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

Example:

.. code-block:: none

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


   To use a custom image with the 'run' or 'manage_pool' commands, use the --image parameter.
   For AWS, specify the AMI ID: --image ami-12345678

list_instance_types
~~~~~~~~~~~~~~~~~~~

List available instance types with pricing.

.. code-block:: none

   cloud_tasks list_instance_types
      [Common options]
      [Instance type selection options]
      [Additional options]

Additional options:

--region REGION   Region to use, overrides configuration file
--filter TEXT     Include only images containing ``TEXT`` in any field
--sort-by FIELDS  Sort the result by one or more comma-separated fields; available fields
                  are ``name``, ``vcpu``, ``mem``, ``local_ssd``, ``storage``,
                  ``vcpu_price``, ``mem_price``, ``local_ssd_price``, ``storage_price``,
                  ``price_per_cpu``, ``mem_per_gb_price``, ``local_ssd_per_gb_price``,
                  ``storage_per_gb_price``, ``total_price``, ``total_price_per_cpu``,
                  ``zone``, ``description``. Prefix with ``-`` for descending order.
                  Partial field names like ``ram`` or ``mem`` for ``mem_gb`` or ``v`` for
                  ``vcpu`` are supported.
--limit N         Limit the number of results to the first ``N`` after sorting
--detail          Show detailed information

Example:

.. code-block:: none

   $ cloud_tasks list_instance_types --provider gcp --region us-central1 --instance-types "n.-.*" --sort-by=-cpu,-mem --limit 5
   Retrieving instance types...
   Retrieving pricing information...

   Instance Type                  Arch vCPU   Mem (GB)  LSSD (GB)  Disk (GB)  Total $/Hr         Zone
   -----------------------------------------------------------------------------------------------------------
   n1-ultramem-160              X86_64  160     3844.0          0          0    $21.3448  us-central1-*
   n2-highmem-128               X86_64  128      864.0          0          0     $7.7070  us-central1-*
   n2-standard-128              X86_64  128      512.0          0          0     $6.2156  us-central1-*
   n1-megamem-96                X86_64   96     1433.6          0          0     $9.1088  us-central1-*
   n2-highmem-96                X86_64   96      768.0          0          0     $6.2887  us-central1-*

list_running_instances
~~~~~~~~~~~~~~~~~~~~~~

List currently running instances.

.. code-block:: none

   python -m cloud_tasks.cli list_running_instances
     --config CONFIG
     --provider {aws,gcp,azure}
     [--job-id JOB_ID]    Filter by job ID
     [--all-instances]    Show all instances including non-cloud-tasks ones
     [--include-terminated] Include terminated instances
     [--sort-by FIELDS]   Sort by comma-separated fields
     [--detail]          Show detailed information
     [--region REGION]   Specific region to use
     [--zone ZONE]       Specific zone to use
     [--verbose]

This command displays:

- Instance IDs, types, and states
- Creation timestamps
- Associated tags (like job ID and role)
- Summary information (total instances, running vs. starting)
- Detailed information in verbose mode (``--verbose``)

Example:

.. code-block:: bash

   python -m cloud_tasks.cli list_running_instances \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --job-id my-job-id

Job Management Commands
-----------------------

run
~~~

Run a job with automatic instance management.

.. code-block:: none

   python -m cloud_tasks.cli run
     --config CONFIG
     --provider {aws,gcp,azure}
     --job-id JOB_ID
     --queue-name QUEUE_NAME
     --tasks TASKS_FILE      Path to tasks file (JSON or YAML)
     --start-task N         Skip tasks until this task number (1-based)
     --limit N              Maximum number of tasks to enqueue
     --max-concurrent-tasks N Maximum concurrent tasks to enqueue (default: 100)
     --cpu CPU              Number of CPU cores required
     --memory MEMORY        Memory required in GB
     --disk DISK            Disk space required in GB
     --min-instances MIN    Minimum number of instances to maintain
     --max-instances MAX    Maximum number of instances allowed
     --min-total-cpus N     Filter instance types by min total vCPUs
     --max-total-cpus N     Filter instance types by max total vCPUs
     --min-total-price-per-hour N  Filter by min total price per hour
     --max-total-price-per-hour N  Filter by max total price per hour
     --cpus-per-task N      Number of vCPUs per task
     --min-tasks-per-instance N  Minimum tasks per instance
     --max-tasks-per-instance N  Maximum tasks per instance
     --architecture {x86_64,arm64}  CPU architecture to use
     --min-cpu N            Filter by min vCPUs
     --max-cpu N            Filter by max vCPUs
     --min-total-memory N   Filter by min total memory (GB)
     --max-total-memory N   Filter by max total memory (GB)
     --min-memory-per-cpu N Filter by min memory per vCPU (GB)
     --max-memory-per-cpu N Filter by max memory per vCPU (GB)
     --min-local-ssd N      Filter by min local SSD (GB)
     --max-local-ssd N      Filter by max local SSD (GB)
     --min-local-ssd-per-cpu N  Filter by min local SSD per vCPU
     --max-local-ssd-per-cpu N  Filter by max local SSD per vCPU
     --min-boot-disk N      Filter by min boot disk (GB)
     --max-boot-disk N      Filter by max boot disk (GB)
     --min-boot-disk-per-cpu N  Filter by min boot disk per vCPU
     --max-boot-disk-per-cpu N  Filter by max boot disk per vCPU
     [--use-spot]          Use spot/preemptible instances
     [--region REGION]     Specific region to use
     [--zone ZONE]         Specific zone to use
     [--startup-script-file FILE] Path to startup script file
     [--instance-types TYPES]  Space-separated instance type families
     [--image IMAGE]       VM image ID or name
     [--task-timeout SECONDS]  Maximum time for a task to complete
     [--instance-timeout SECONDS] Maximum time for an instance to run
     [--scaling-check-interval SECONDS] Interval between scaling checks (default: 60)
     [--instance-termination-delay SECONDS] Delay before terminating instances (default: 60)
     [--max-runtime SECONDS] Maximum runtime for a worker job (default: 60)
     [--worker-use-new-process] Use new process for each task (default: False)
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli run \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --job-id my-processing-job \
     --queue-name my-task-queue \
     --tasks tasks.json \
     --cpu 2 \
     --memory 4 \
     --disk 20 \
     --min-instances 1 \
     --max-instances 10 \
     --use-spot \
     --region us-west-2 \
     --startup-script-file setup.sh \
     --instance-types "t3 m5" \
     --image ami-123456 \
     --task-timeout 3600 \
     --instance-timeout 7200

status
~~~~~~

Check the status of a running job.

.. code-block:: none

   python -m cloud_tasks.cli status
     --config CONFIG
     --provider {aws,gcp,azure}
     --job-id JOB_ID
     [--region REGION]     Specific region to use
     [--zone ZONE]         Specific zone to use
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli status \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --job-id my-job-id

stop
~~~~

Stop a job and terminate its instances.

.. code-block:: none

   python -m cloud_tasks.cli stop
     --config CONFIG
     --provider {aws,gcp,azure}
     --job-id JOB_ID
     [--purge-queue]      Purge the queue after stopping
     [--force]           Stop without confirmation
     [--region REGION]   Specific region to use
     [--zone ZONE]       Specific zone to use
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli stop \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --job-id my-job-id \
     --force

manage_pool
~~~~~~~~~~~

Adjust the instance pool size for a running job.

.. code-block:: none

   python -m cloud_tasks.cli manage_pool
     --config CONFIG
     --provider {aws,gcp,azure}
     --job-id JOB_ID
     --min-instances MIN    New minimum instances
     --max-instances MAX    New maximum instances
     --min-total-cpus N     Filter instance types by min total vCPUs
     --max-total-cpus N     Filter instance types by max total vCPUs
     --min-total-price-per-hour N  Filter by min total price per hour
     --max-total-price-per-hour N  Filter by max total price per hour
     --cpus-per-task N      Number of vCPUs per task
     --min-tasks-per-instance N  Minimum tasks per instance
     --max-tasks-per-instance N  Maximum tasks per instance
     --architecture {x86_64,arm64}  CPU architecture to use
     [--image IMAGE]       VM image to use
     [--startup-script-file FILE] Path to startup script file
     [--scaling-check-interval SECONDS] Interval between scaling checks (default: 60)
     [--instance-termination-delay SECONDS] Delay before terminating instances (default: 60)
     [--max-runtime SECONDS] Maximum runtime for a worker job (default: 60)
     [--worker-use-new-process] Use new process for each task (default: False)
     [--use-spot]         Use spot/preemptible instances
     [--region REGION]    Specific region to use
     [--zone ZONE]        Specific zone to use
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli manage_pool \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --job-id my-job-id \
     --min-instances 1 \
     --max-instances 10

Queue Management Commands
-------------------------

load_queue
~~~~~~~~~~

Load tasks into a queue.

.. code-block:: none

   python -m cloud_tasks.cli load_queue
     --config CONFIG
     --provider {aws,gcp,azure}
     --queue-name QUEUE_NAME
     --tasks TASKS_FILE    Path to tasks file (JSON or YAML)
     [--start-task N]     Skip tasks until this task number (1-based)
     [--limit N]          Maximum number of tasks to enqueue
     [--max-concurrent-tasks N] Maximum concurrent tasks to enqueue (default: 100)
     [--region REGION]    Specific region to use
     [--zone ZONE]        Specific zone to use
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli load_queue \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --queue-name my-task-queue \
     --tasks tasks.json

show_queue
~~~~~~~~~~

Show information about a task queue.

.. code-block:: none

   python -m cloud_tasks.cli show_queue
     --config CONFIG
     --provider {aws,gcp,azure}
     --queue-name QUEUE_NAME
     [--detail]          Show a sample message
     [--region REGION]   Specific region to use
     [--zone ZONE]       Specific zone to use
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli show_queue \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --queue-name my-task-queue \
     --verbose

purge_queue
~~~~~~~~~~~

Remove all messages from a queue.

.. code-block:: none

   python -m cloud_tasks.cli purge_queue
     --config CONFIG
     --provider {aws,gcp,azure}
     --queue-name QUEUE_NAME
     [--force]           Purge without confirmation
     [--region REGION]   Specific region to use
     [--zone ZONE]       Specific zone to use
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli purge_queue \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --queue-name my-task-queue \
     --force

delete_queue
~~~~~~~~~~~~

Delete a queue and its infrastructure.

.. code-block:: none

   python -m cloud_tasks.cli delete_queue
     --config CONFIG
     --provider {aws,gcp,azure}
     --queue-name QUEUE_NAME
     [--force]           Delete without confirmation
     [--region REGION]   Specific region to use
     [--zone ZONE]       Specific zone to use
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli delete_queue \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --queue-name my-task-queue \
     --force

show_queue_depth
~~~~~~~~~~~~~~~~

Display the current depth of a task queue.

.. code-block:: none

   python -m cloud_tasks.cli show_queue_depth
     --config CONFIG
     --provider {aws,gcp,azure}
     --queue-name QUEUE_NAME
     [--verbose]

With the ``--verbose`` flag, the command will also attempt to peek at the first message in the queue without removing it, displaying its contents.

Example:

.. code-block:: bash

   python -m cloud_tasks.cli show_queue_depth \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --queue-name my-task-queue

empty_queue
~~~~~~~~~~~

Remove all messages from a queue.

.. code-block:: none

   python -m cloud_tasks.cli empty_queue
     --config CONFIG
     --provider {aws,gcp,azure}
     --queue-name QUEUE_NAME
     [--force]           Skip confirmation prompt
     [--region REGION]   Specific region to use
     [--zone ZONE]       Specific zone to use
     [--verbose]

This command:

- Shows the current queue depth before emptying
- Prompts for confirmation (unless ``--force`` is used)
- Purges all messages from the queue
- Verifies the queue is empty after the operation
- Provides a warning if messages remain after purging (e.g., in-flight messages)

.. warning::
   Use this command with caution as it permanently deletes all messages in the queue.

Example:

.. code-block:: bash

   python -m cloud_tasks.cli empty_queue \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --queue-name my-task-queue \
     --force


Exit Status
-----------

The CLI returns the following exit codes:

* 0 - Success
* 1 - Error occurred during command execution

Environment Variables
---------------------

Cloud Tasks can use environment variables for credentials:

- AWS: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
- GCP: GOOGLE_APPLICATION_CREDENTIALS
- Azure: AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET

Troubleshooting
---------------

Common Issues
~~~~~~~~~~~~~

1. **Connection Errors**: Ensure your credentials and network settings are correct
2. **Permission Denied**: Verify the provided credentials have sufficient permissions
3. **Resource Not Found**: Check that the specified queues, regions, or resources exist

.. note::
   For more detailed error messages, use the ``--verbose`` flag.

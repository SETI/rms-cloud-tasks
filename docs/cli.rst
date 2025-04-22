Command Line Interface Reference
===============================

This page provides a comprehensive reference for the Cloud Tasks command-line interface.

Common Options
------------

Most commands support these common options:

.. code-block:: none

   --config CONFIG         Path to configuration file (default: cloud_tasks_config.yaml)
   --provider {aws,gcp,azure}  Cloud provider
   --queue-name QUEUE_NAME  Name of the task queue
   --verbose, -v           Enable verbose output (-v for warning, -vv for info, -vvv for debug)

Job Management Commands
--------------------

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
~~~~~~~~~~

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
-----------------------

load_queue
~~~~~~~~~

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
~~~~~~~~~

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
~~~~~~~~~~

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
~~~~~~~~~~~

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

Information Commands
-----------------

list_regions
~~~~~~~~~~~

List available regions for a provider.

.. code-block:: none

   python -m cloud_tasks.cli list_regions
     --config CONFIG
     --provider {aws,gcp,azure}
     [--prefix PREFIX]    Filter regions by name prefix
     [--zones]           Show availability zones for each region
     [--detail]          Show additional provider-specific information
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli list_regions \
     --config cloud_tasks_config.yaml \
     --provider aws

list_images
~~~~~~~~~~

List available VM images.

.. code-block:: none

   python -m cloud_tasks.cli list_images
     --config CONFIG
     --provider {aws,gcp,azure}
     [--user]            Include user-created images
     [--filter TEXT]     Filter images containing text
     [--limit N]         Maximum number of results
     [--sort-by FIELDS]  Comma-separated sort fields (e.g., "name,source")
     [--detail]          Show detailed information
     [--region REGION]   Specific region to use
     [--zone ZONE]       Specific zone to use
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli list_images \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --sort-by "name,source"

list_instance_types
~~~~~~~~~~~~~~~~~

List available instance types with pricing.

.. code-block:: none

   python -m cloud_tasks.cli list_instance_types
     --config CONFIG
     --provider {aws,gcp,azure}
     [--instance-types TYPES]  Space-separated instance type families
     [--size-filter FILTER]    Size filter in format "cpu:memory:disk"
     [--limit N]               Maximum number of results
     [--use-spot]             Show spot/preemptible pricing
     [--sort-by FIELDS]       Comma-separated sort fields
     [--filter TEXT]          Filter types containing text
     [--detail]              Show detailed information
     [--architecture {x86_64,arm64}] CPU architecture to use
     [--min-cpu N]           Filter by min vCPUs
     [--max-cpu N]           Filter by max vCPUs
     [--min-total-memory N]  Filter by min total memory (GB)
     [--max-total-memory N]  Filter by max total memory (GB)
     [--min-memory-per-cpu N] Filter by min memory per vCPU (GB)
     [--max-memory-per-cpu N] Filter by max memory per vCPU (GB)
     [--min-local-ssd N]     Filter by min local SSD (GB)
     [--max-local-ssd N]     Filter by max local SSD (GB)
     [--min-local-ssd-per-cpu N] Filter by min local SSD per vCPU
     [--max-local-ssd-per-cpu N] Filter by max local SSD per vCPU
     [--min-boot-disk N]     Filter by min boot disk (GB)
     [--max-boot-disk N]     Filter by max boot disk (GB)
     [--min-boot-disk-per-cpu N] Filter by min boot disk per vCPU
     [--max-boot-disk-per-cpu N] Filter by max boot disk per vCPU
     [--region REGION]       Specific region to use
     [--zone ZONE]           Specific zone to use
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli list_instance_types \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --instance-types "t3 m5" \
     --size-filter "2:4:10" \
     --limit 10 \
     --use-spot \
     --sort-by "price,vcpu"

list_running_instances
~~~~~~~~~~~~~~~~~~~~

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

Example:

.. code-block:: bash

   python -m cloud_tasks.cli list_running_instances \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --job-id my-job-id

Exit Status
----------

The CLI returns the following exit codes:

* 0 - Success
* 1 - Error occurred during command execution

Environment Variables
------------------

Cloud Tasks can use environment variables for credentials:

- AWS: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
- GCP: GOOGLE_APPLICATION_CREDENTIALS
- Azure: AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET

Troubleshooting
-------------

Common Issues
~~~~~~~~~~~

1. **Connection Errors**: Ensure your credentials and network settings are correct
2. **Permission Denied**: Verify the provided credentials have sufficient permissions
3. **Resource Not Found**: Check that the specified queues, regions, or resources exist

For more detailed error messages, use the ``--verbose`` flag.
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
   --verbose, -v           Enable verbose output

Queue Management Commands
-----------------------

load_queue
~~~~~~~~~

Load tasks into a queue without starting instances.

.. code-block:: none

   python -m cloud_tasks.cli load_queue
     --config CONFIG
     --provider {aws,gcp,azure}
     --queue-name QUEUE_NAME
     --tasks TASKS_FILE      Path to tasks file (JSON or YAML)
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

Show the current depth of a task queue's contents.

.. code-block:: none

   python -m cloud_tasks.cli show_queue
     --config CONFIG
     --provider {aws,gcp,azure}
     --queue-name QUEUE_NAME
     [--detail]             Attempt to show a sample message
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli show_queue \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --queue-name my-task-queue \
     --detail

purge_queue
~~~~~~~~~~

Purge a task queue by removing all messages.

.. code-block:: none

   python -m cloud_tasks.cli purge_queue
     --config CONFIG
     --provider {aws,gcp,azure}
     --queue-name QUEUE_NAME
     [--force, -f]          Purge the queue without confirmation prompt
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

Permanently delete a task queue and its infrastructure.

.. code-block:: none

   python -m cloud_tasks.cli delete_queue
     --config CONFIG
     --provider {aws,gcp,azure}
     --queue-name QUEUE_NAME
     [--force, -f]          Delete the queue without confirmation prompt
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli delete_queue \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --queue-name my-task-queue \
     --force

Information Gathering Commands
----------------------------

list_regions
~~~~~~~~~~~

List available regions for the specified provider.

.. code-block:: none

   python -m cloud_tasks.cli list_regions
     --config CONFIG
     --provider {aws,gcp,azure}
     [--prefix PREFIX]      Filter regions to only show those starting with this prefix
     [--zones]              Show availability zones for each region
     [--detail]             Show additional provider-specific information
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli list_regions \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --prefix us- \
     --zones \
     --detail

list_images
~~~~~~~~~~

List available VM images for the specified provider.

.. code-block:: none

   python -m cloud_tasks.cli list_images
     --config CONFIG
     --provider {aws,gcp,azure}
     [--user]                Include user-created images in the list
     [--filter FILTER]       Filter images containing this text in any field
     [--limit LIMIT]         Limit the number of images displayed
     [--sort-by SORT_BY]     Sort results by comma-separated fields
     [--detail]              Show detailed information about each image
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli list_images \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --filter "ubuntu" \
     --limit 10 \
     --sort-by "name,creation_date" \
     --detail

list_instance_types
~~~~~~~~~~~~~~~~~

List compute instance types for the specified provider with pricing information.

.. code-block:: none

   python -m cloud_tasks.cli list_instance_types
     --config CONFIG
     --provider {aws,gcp,azure}
     [--min-cpu MIN_CPU]                Minimum number of vCPUs
     [--max-cpu MAX_CPU]                Maximum number of vCPUs
     [--min-total-memory MIN_MEMORY]    Minimum amount of total memory (GB)
     [--max-total-memory MAX_MEMORY]    Maximum amount of total memory (GB)
     [--min-memory-per-cpu MIN_MEM_CPU] Minimum memory (GB) per vCPU
     [--max-memory-per-cpu MAX_MEM_CPU] Maximum memory (GB) per vCPU
     [--min-disk MIN_DISK]              Minimum disk (GB)
     [--max-disk MAX_DISK]              Maximum disk (GB)
     [--min-disk-per-cpu MIN_DISK_CPU]  Minimum disk (GB) per vCPU
     [--max-disk-per-cpu MAX_DISK_CPU]  Maximum disk (GB) per vCPU
     [--instance-types TYPE [TYPE ...]]  Filter instance types by name prefix
     [--use-spot]                       Show spot/preemptible pricing
     [--region REGION]                  Specific region to check
     [--zone ZONE]                      Specific zone to check
     [--filter FILTER]                  Filter instance types containing this text
     [--limit LIMIT]                    Limit the number displayed
     [--sort-by SORT_BY]                Sort results by comma-separated fields
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli list_instance_types \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --min-cpu 2 \
     --min-total-memory 4 \
     --instance-types t3 m5 \
     --use-spot \
     --region us-west-2 \
     --limit 10 \
     --sort-by "total_price,vcpu"

list_running_instances
~~~~~~~~~~~~~~~~~~~~

List currently running instances for the specified provider.

.. code-block:: none

   python -m cloud_tasks.cli list_running_instances
     --config CONFIG
     --provider {aws,gcp,azure}
     [--job-id JOB_ID]      Filter instances by job ID
     [--region REGION]      Filter instances by region
     [--verbose]

Example:

.. code-block:: bash

   python -m cloud_tasks.cli list_running_instances \
     --config cloud_tasks_config.yaml \
     --provider aws \
     --job-id my-job-id \
     --verbose

Exit Status
----------

The CLI returns the following exit codes:

* 0 - Success
* 1 - Error occurred during command execution

Logging
-------

Cloud Tasks includes a custom logging system that provides:

- Consistent timestamp format across all components
- Millisecond precision in log timestamps (3 digits)
- Configurable log levels through the `--verbose` flag

Advanced Usage
-------------

Environment Variables
~~~~~~~~~~~~~~~~~~

Cloud Tasks can use environment variables for credentials:

- AWS: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
- GCP: GOOGLE_APPLICATION_CREDENTIALS
- Azure: AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET

Troubleshooting
--------------

Common Issues
~~~~~~~~~~~

1. **Connection Errors**: Ensure your credentials and network settings are correct.
2. **Permission Denied**: Verify the provided credentials have sufficient permissions.
3. **Resource Not Found**: Check that the specified queues, regions, or resources exist.

For more detailed error messages, use the `--verbose` flag.
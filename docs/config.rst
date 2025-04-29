.. _config:

Configuration and Instance Selection
====================================

Cloud Tasks has a flexible configuration system. Options can be specified in a YAML-format
configuration file or on the command line, and very few options are required for basic use.

The configuration file supports global options for system configuration, options for
selecting compute instances and running jobs, and provider-specific options including
authentication and job options that can override the global options. If provider-specific
options are specified, the one for the

A configuration file has the following structure:

.. code-block:: yaml

    [Global options]
    run:
      [Run options]

    aws:
      [AWS-specific options]
      [Run options]

    gcp:
      [GCP-specific options]
      [Run options]

    azure:
      [Azure-specific options]
      [Run options]

Global Options
--------------

The available global options are:

* ``provider``: The cloud provider to use (select one of ``aws``, ``gcp``, ``azure``)

The ``provider`` option must be specified either in the configuration file or on the
command line. In addition to detemining which cloud provider to contact, it is used to
determine which provider-specific options in the configuration file are relevant.

Run Options
-----------

.. _config_compute_instance_options:

Options to select a compute instance type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Generally speaking, within the constraints provided, the system will attempt to use the
lowest cost per vCPU with the maximum number of vCPUs per instance. This will tend to
choose the cheapest (and probably worst-performing) instance type, the least memory, and
the least disk space. If you need specific performance, specify the instance types you are
willing to accept as a regular expression. For example, to allow all GCP "N2" instances,
specify ``instance_types: "^n2-.*"``. This will still give the system freedom to choose the
best instance type within that family given the other constraints. Note that it is quite
possible to over-constrain the system such that no instance types meet the requirements.

* CPU and Memory

  * ``architecture``: The architecture to use; valid values are ``X86_64`` and ``ARM64``
    (defaults to ``X86_64``)
  * ``min_cpu``: The minimum number of vCPUs per instance
  * ``max_cpu``: The maximum number of vCPUs per instance
  * ``min_total_memory``: The minimum amount of memory in GB per instance
  * ``max_total_memory``: The maximum amount of memory in GB per instance
  * ``min_memory_per_cpu``: The minimum amount of memory per vCPU
  * ``max_memory_per_cpu``: The maximum amount of memory per vCPU

* Storage (the boot disk is usually a standard hard drive, not an SSD; some instance
  types have additional local SSD storage)

  * ``min_local_ssd``: The minimum amount of local extra SSD storage in GB per instance
  * ``max_local_ssd``: The maximum amount of local extra SSD storage in GB per instance
  * ``min_local_ssd_per_cpu``: The minimum amount of local extra SSD storage per vCPU
  * ``max_local_ssd_per_cpu``: The maximum amount of local extra SSD storage per vCPU
  * ``min_boot_disk``: The minimum amount of boot disk in GB per instance
  * ``max_boot_disk``: The maximum amount of boot disk in GB per instance
  * ``min_boot_disk_per_cpu``: The minimum amount of boot disk per vCPU
  * ``max_boot_disk_per_cpu``: The maximum amount of boot disk per vCPU

* Instance Types

  * ``instance_types``: A single instance type or list of instance types to use;
    instance types are specified using Python-style regular expressions (if no
    anchor character like ``^`` or ``$`` is specified, the given string will match
    any part of the instance type name)

.. _config_number_of_instances_options:

Options to constrain the number of instances
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Generally speaking, the system will attempt to use the maximum number of instances allowed
based on the various ``max_`` constraints, and then will verify that the ``min_`` constraints
have not been violated. Note that it is quite possible to over-constrain the system such that
no number of instances meet the requirements.

* ``min_instances``: The minimum number of instances to use (defaults to 1)
* ``max_instances``: The maximum number of instances to use (defaults to 10)
* ``min_total_cpus``: The minimum total number of vCPUs to use
* ``max_total_cpus``: The maximum total number of vCPUs to use
* ``cpus_per_task``: The number of vCPUs per task; this is also used to configure
  the worker process to limit the number of tasks that can be run simultaneously
  on a single instance
* ``min_tasks_per_instance``: The minimum number of tasks per instance
* ``max_tasks_per_instance``: The maximum number of tasks per instance
* ``min_simultaneous_tasks``: The minimum number of tasks to run simultaneously
* ``max_simultaneous_tasks``: The maximum number of tasks to run simultaneously
* ``min_total_price_per_hour``: The minimum total price per hour to use
* ``max_total_price_per_hour``: The maximum total price per hour to use

.. _config_vm_options:

Options to specify the type of VM
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``use_spot``: Use spot instances instead of on-demand instances; spot instances
  are cheaper but may be terminated by the cloud provider with little notice

.. _config_boot_options:

Options to specify the boot process
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* A startup script must be specified when creating new instances. It can be
  specified either directly inline in the configuration file, or by providing a path to
  a file containing the startup script. Either one can be used, but not both.

  * ``startup_script``: The startup script to use (this can not be overridden from the
    command line because it is assumed that any startup script would be too long
    to pass as a command line argument)
  * ``startup_script_file``: The path to a file containing the startup script

* ``image``: The image to use for the VM

.. _config_worker_and_manage_pool_options:

Options to specify the worker and manage_pool processes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``scaling_check_interval``: The interval to check for scaling opportunities (defaults to 60)
* ``instance_termination_delay``: The delay to wait before terminating an instance
  (defaults to 60)
* ``max_runtime``: The maximum runtime for a task (defaults to 60)
* ``worker_use_new_process``: Use a new process for each task instead of reusing the
  same process (defaults to ``False``)

.. _config_provider_specific_options:

Provider-Specific Options
-------------------------

The available provider-specific options are:

* All providers

  * ``job_id``: The ID of the job to run; required for most operations
  * ``queue_name``: The name of the queue to use; derived from ``job_id`` if not specified
  * ``region``: The region to use; required for most operations
  * ``zone``: The zone to use; will be automatically selected if not specified

* AWS

  * ``access_key``: The access key to use
  * ``secret_key``: The secret key to use

* GCP

  * ``project_id``: The ID of the project to use [Required for most operations]
  * ``credentials_file``: The path to a file containing the credentials to use; if not
    specified, the default credentials will be used
  * ``service_account``: The service account to use; required for worker processes
    on cloud-based instances to have access to system resources [Required when creating
    instances]

* Azure

  * ``subscription_id``: The ID of the subscription to use
  * ``tenant_id``: The ID of the tenant to use
  * ``client_id``: The ID of the client to use
  * ``client_secret``: The secret to use

In addition, all run options can be specified in a provider-specific section, in which
case they will override the global run options, if any.

Command Line Overrides
----------------------

You can override any configuration value from the command line:

.. code-block:: bash

    python -m cloud_tasks run \
      --config config.yaml \
      --tasks tasks.json \
      --provider aws \                 # Override provider setting
      --min-cpu 8 \                    # Override min_cpu setting
      --min-memory-per-cpu 16 \        # Override min_memory_per_cpu setting
      --min-boot-disk 100 \            # Override min_boot_disk setting
      --image ami-0123456789abcdef0 \  # Override image setting
      --job-id my-processing-job \     # Override job_id setting
      --instance-types t3- m5-         # Restrict to t3 and m5 instance families

.. note::
   Priority of settings is: Command Line > Provider-Specific Config > Global Run Config > System Defaults

Examples
--------

The Simplest Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~

For GCP, the simplest configuration consists of a provider name, a job ID, a project ID,
a region, and a startup script.

.. code-block:: yaml

    provider: gcp
    gcp:
      job_id: my-processing-job
      project_id: my-project-id
      region: us-central1
      startup_script: |
        #!/bin/bash
        echo "Hello, world!"

.. code-block:: bash

    $ cloud_tasks manage_pool --config config.yaml

Given the lack of any other constraints, the system will select the ``e2-highcpu-32``
instance type. This is the lowest-memory version of GCP's most economical instance type,
costing $0.024736/vCPU/hour as of this writing. It selects the 32-vCPU version, which is
the maximum number of vCPUs available in a single instance for the ``e2`` family, to
minimize the number of instances that need to be started and managed.

With the exception of the startup script, this could also be specified entirely on the
command line:

.. code-block:: yaml

    gcp:
      startup_script: |
        #!/bin/bash
        echo "Hello, world!"

.. code-block:: bash

    $ cloud_tasks manage_pool \
      --config config.yaml \
      --provider gcp \
      --job-id my-processing-job \
      --project-id my-project-id \
      --region us-central1

and if the startup script was present in a file, no configuration file would be needed
at all:

.. code-block:: bash

    $ cloud_tasks manage_pool \
      --provider gcp \
      --job-id my-processing-job \
      --project-id my-project-id \
      --region us-central1 \
      --startup-script-file startup.sh

With no further constraints, the system will create the default maximum number of instances, 10,
which will result in the creation of 320 vCPUs and a burn rate of $7.92/hour.

Constraining the Instance Type and Containing Costs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example uses more sophisticated constraints to limit the instance types and number of
instances to use. First, we want to use slightly higher-performance processors and choose
the ``N2`` series. We want to limit instance types to those that have at least 8 but not
more than 40 vCPUs. We might choose these numbers to balance parallelism with the network
and disk bandwidth available on a single instance. At the same time, we know that our
tasks are themselves parallel internally, and require 4 vCPUs per task for optimal
performance. They also require memory of at least 32 GB per task, which is 8 GB per vCPU.
Finally, since we have a large number of tasks to process but our task code is still
experimental, we are concerned about starting too many instances at once and thus having a
high burn rate in case something goes wrong and we want to stop the job in the middle when
we detect a problem. We set limits of 20 instances total, 100 simultaneous tasks, and a burn
rate of $15.00 per hour. Whichever of these is most constraining will determine the total
number of instances that will be started.

.. code-block:: yaml

    provider: gcp
    gcp:
      job_id: my-processing-job
      project_id: rfrench
      region: us-central1
      instance_types: "^n2-.*"
      min_cpu: 8
      max_cpu: 40
      min_memory_per_cpu: 8
      max_instances: 20
      cpus_per_task: 4
      max_simultaneous_tasks: 100
      max_total_price_per_hour: 15.00
      startup_script: |
        #!/bin/bash
        echo "Hello, world!"

In this case, the system starts by looking at all available ``n2-``, ``n3-``, and ``n4-``
instance types that meet our vCPU and memory constraints while minimizing price per vCPU.
This results in the selection of ``n4-highmem-32`` as the optimal instance type with the
lowest cost of $0.062194/vCPU/hour while supporting the most vCPUs in a single instance.
For the number of instances, the system starts with the maximum allowed, 20. However, with
a maximum of 100 simultaneous tasks, 32 vCPUs, and 4 vCPUs per task, this is reduced to 12.
Finally, at a cost of $1.99/hour for each instance, the price limit of $25.00 per hour
sets the final number of instances to 7 for a total cost of $13.93/hour.

.. _config:

Configuration and Instance Selection
====================================

Cloud Tasks has a flexible configuration system. Options can be specified in a YAML-format
configuration file or on the command line, and very few options are required for basic use.

The configuration file supports global options for system configuration, options for
selecting compute instances and running jobs, and provider-specific options including
authentication and job options that can override the other options.

A configuration file has the following structure (all sections are optional):

.. code-block:: yaml

    [Global options]

    run:
      [Run options]

    aws:
      [AWS-specific options]
      [AWS-specific run options]

    gcp:
      [GCP-specific options]
      [GCP-specific run options]


Global Options
--------------

The available global options are:

* ``provider``: The cloud provider to use (select one of ``aws`` or ``gcp``)

The ``provider`` option must be specified either in the configuration file or on the
command line. In addition to detemining which cloud provider to contact, it is used to
determine which provider-specific options in the configuration file are relevant.

Run Options
-----------

Run options come in several flavors:

* :ref:`config_compute_instance_options`
* :ref:`config_number_of_instances_options`
* :ref:`config_vm_options`
* :ref:`config_boot_options`
* :ref:`config_worker_and_manage_pool_options`

.. _config_compute_instance_options:

Options to select a compute instance type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Generally speaking, within the constraints provided, the system will attempt to use the
instance type with the lowest cost per vCPU with the maximum number of vCPUs per instance.
This results in needing the fewest instances to get the job done, since each instance can
do maximal work; this may or may not be an appropriate choice for your workload (for
example, having a large number of vCPUs, and thus simultaneosly running tasks, may result
in the tasks being throttled by the network or disk bandwidth). With no constraints, the
system will tend to choose the cheapest (and probably worst-performing) instance type with
the least memory, the least disk space, and the slowest disk type. *Thus, while no
constraints are required, it is recommended to specify at least some minimal constraints
to avoid selecting the worst possible instance type.*

If you need specific performance, specify the instance types you are willing to accept as
a regular expression. For example, to allow all GCP "N2" instances, specify
``instance_types: "^n2-.*"``. This will still give the system freedom to choose the best
instance type within that family given the other constraints. Alternatively, you can
specify ``cpu_family``, ``min_cpu_rank``, or ``max_cpu_rank`` if you don't want to look up
the specific instance types that are relevant to your needs. For example, ``min_cpu_rank:
21`` will specify a fast processor (Intel Sapphire Rapids or better). Note that it is
quite possible to over-constrain the system such that no instance types meet the
requirements.

Many attributes can be specified in multiple ways. For example, the minimum amount of
memory can be specified using ``min_total_memory``, ``min_memory_per_cpu``, or
``min_memory_per_task``. Multiple constraints can be specified for the same attribute and
the system will use the most-constraining value.

To get a list of the available instance types and their attributes, including number of
vCPUs, amount of memory, CPU family and performance rank, price, etc. you can use the
:ref:`cli_list_instance_types` command line command. Include the ``--detail`` option to see
all available attributes.


General Constraints
+++++++++++++++++++

* ``architecture``: The architecture to use; valid values are ``X86_64`` and ``ARM64``
  (defaults to ``X86_64``)
* ``cpu_family``: The CPU family to use, for example ``Intel Cascade Lake`` or ``AMD Genoa``.
* ``min_cpu_rank``: The minimum CPU performance rank to use (0 is the slowest)
* ``max_cpu_rank``: The maximum CPU performance rank to use (0 is the slowest)
* ``instance_types``: A single instance type or list of instance types to use;
  instance types are specified using Python-style regular expressions (if no
  anchor character like ``^`` or ``$`` is specified, the given string will match
  any part of the instance type name)


CPU
+++

* ``min_cpu``: The minimum number of vCPUs per instance
* ``max_cpu``: The maximum number of vCPUs per instance

* Derived from instance task information (the number of CPUs = cpus_per_task * tasks_per_instance)

  * ``cpus_per_task``: The number of vCPUs per task (defaults to 1)
  * ``min_tasks_per_instance``: The minimum number of tasks per instance
  * ``max_tasks_per_instance``: The maximum number of tasks per instance


Memory
++++++

* ``min_total_memory``: The minimum amount of memory in GB per instance
* ``max_total_memory``: The maximum amount of memory in GB per instance

* Per-CPU constraints

  * ``min_memory_per_cpu``: The minimum amount of memory in GB per vCPU
  * ``max_memory_per_cpu``: The maximum amount of memory in GB per vCPU

* Per-task constraints (these are the same as the per-CPU constraints and simply use the
  ``cpus_per_task`` value as a conversion factor)

  * ``cpus_per_task``: The number of vCPUs per task (defaults to 1)
  * ``min_memory_per_task``: The minimum amount of memory in GB per task
  * ``max_memory_per_task``: The maximum amount of memory in GB per task

SSD Storage
+++++++++++

Some instance types have additional local SSD storage in addition to whatever volume is
mounted as the boot disk and these constraints apply to them. By specifying a minimum SSD
size you are also constraining the instance type to those that have an extra SSD attached.

* ``min_local_ssd``: The minimum amount of local extra SSD storage in GB per instance
* ``max_local_ssd``: The maximum amount of local extra SSD storage in GB per instance

* Per-CPU constraints - the total amount of storage will be the sum of the base size and
  the product of the number of vCPUs and the per-CPU amount; the base size is optional,
  and defaults to 0

  * ``local_ssd_base_size``: The amount of local extra SSD storage in GB present before
    allocating additional space per vCPU
  * ``min_local_ssd_per_cpu``: The minimum amount of local extra SSD storage in GB per vCPU
  * ``max_local_ssd_per_cpu``: The maximum amount of local extra SSD storage in GB per vCPU

* Per-task constraints (these are the same as the per-CPU constraints and simply use the
  ``cpus_per_task`` value as a conversion factor)

  * ``cpus_per_task``: The number of vCPUs per task (defaults to 1)
  * ``local_ssd_base_size``: The amount of local extra SSD storage in GB present before
    allocating additional space per task
  * ``min_local_ssd_per_task``: The minimum amount of local extra SSD storage in GB per task
  * ``max_local_ssd_per_task``: The maximum amount of local extra SSD storage in GB per task

Boot Disk
+++++++++

The boot disk size and type is configurable at instance creation time and is not an
intrinsic property of a provider's instance type. As such, there are no "constraints" on
the boot disk size. Instead, there are simply ways to specify the size and type of the
boot disk you want.

The boot disk size can either be a single absolute value:

* ``total_boot_disk_size``: The size of the boot disk in GB (defaults to 10 GB)

or a per-CPU value:

* ``boot_disk_base_size``: The amount of boot disk in GB present before allocating additional
  space per vCPU (defaults to 0)
* ``boot_disk_per_cpu``: The amount of boot disk in GB per vCPU (defaults to 0)

or a per-task value:

* ``cpus_per_task``: The number of vCPUs per task (defaults to 1)
* ``boot_disk_base_size``: The amount of boot disk in GB present before allocating additional
  space per task (defaults to 0)
* ``boot_disk_per_task``: The amount of boot disk in GB per task (defaults to 0)

If more than one size is specified, the maximum of the values will be used. If no values are
specified, a default appropriate to the provider will be used.

The boot disk type is provider-specific and can be a single type or a list of types:

* ``boot_disk_types``: The type(s) of the boot disk to allow (defaults to all available types
  for the provider)

Finally, some boot disk types require additional configuration:

* ``boot_disk_iops``: For any boot disk type that supports it, the number of provisioned IOPS
  to request; this is an absolute value and is not scaled by the number of vCPUs or tasks
* ``boot_disk_throughput``: For any boot disk type that supports it, the number of provisioned
  throughput in MB/s to request; this is an absolute value and is not scaled by the number of
  vCPUs or tasks


.. _config_number_of_instances_options:

Options to constrain the number of instances
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Generally speaking, the system will attempt to use the maximum number of instances allowed
based on the various ``max_`` constraints, and then will verify that the ``min_``
constraints have not been violated. Note that it is quite possible to over-constrain the
system such that no number of instances meet the requirements. As with the instance type
constraints, no constraints are required, but it is recommended to specify at least some
minimal constraints so that you can maintain control over the size of your instance pool
and the resulting costs. By default, the maximum number of instances is set to 10 to avoid
excessive instance pool sizes, and the maximum price is set to $10 per hour to avoid
runaway costs, but these can be overridden by specifying different values.

Note that depending on the provider and your account setup, you may have quotas for the
creation of specific instance types, and Cloud Tasks may attempt to violate these quotas
if you do not give it sufficient constraints.

* ``min_instances``: The minimum number of instances to use (defaults to 1)
* ``max_instances``: The maximum number of instances to use (defaults to 10)
* ``min_total_cpus``: The minimum total number of vCPUs to use
* ``max_total_cpus``: The maximum total number of vCPUs to use
* ``cpus_per_task``: The number of vCPUs per task (defaults to 1); this is also used to configure
  the worker process to limit the number of tasks that can be run simultaneously
  on a single instance
* ``min_tasks_per_instance``: The minimum number of tasks per instance
* ``max_tasks_per_instance``: The maximum number of tasks per instance
* ``min_simultaneous_tasks``: The minimum number of tasks to run simultaneously
* ``max_simultaneous_tasks``: The maximum number of tasks to run simultaneously
* ``min_total_price_per_hour``: The minimum total price per hour to use
* ``max_total_price_per_hour``: The maximum total price per hour to use (defaults to 10)

.. _config_vm_options:

Options to specify the type of VM
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``use_spot``: Use spot instances instead of on-demand instances; spot instances
  are cheaper but may be terminated by the cloud provider with little notice and should only
  be used for fault-tolerant jobs

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

* ``image``: The image to use for the VM. If no image is specified, the default image for the
  provider will be used. This is most commonly the latest release of Ubuntu 24.04 LTS.

.. _config_worker_and_manage_pool_options:

Options to specify the worker and manage_pool processes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``scaling_check_interval``: The interval in seconds to check for scaling opportunities
  (defaults to 60)
* ``instance_termination_delay``: The delay in seconds to wait before terminating instances
  once the task queue is empty (defaults to 60); this should be set to a value much greater
  than ``max_runtime`` to avoid terminating instances that are still working on tasks.
* ``max_runtime``: The maximum runtime for a task in seconds (defaults to 60); this is used
  to set the retry timeout in the task queue such that any task that takes longer than this
  is assumed to have had an internal error and should be set to a value
  significantly greater than the longest runtime expected for a task
* ``retry_on_exit``: If True, tasks will be retried if the worker exits prematurely, e.g. due
  to a crash
* ``retry_on_exception``: If True, tasks will be retried if the user function raises an
  unhandled exception
* ``retry_on_timeout``: If True, tasks will be retried if they exceed the maximum runtime
  specified by ``max_runtime``

.. _config_provider_specific_options:

Provider-Specific Options
-------------------------

The available provider-specific options are:

* All providers

  * ``job_id``: The ID of the job to run; required for all queue and job-related operations
  * ``queue_name``: The name of the task queue to use, derived from job ID if not specified;
    only use this in special circumstances
  * ``region``: The region to use, required for most operations; will be derived from the
    zone if not specified
  * ``zone``: The zone to use; if not specified, all zones in the region will be used
  * ``exactly_once_queue``: If True, task messages and events are guaranteed to be delivered
    exactly once to any recipient. If False (the default), messages will be delivered at least
    once, but could be delivered multiple times. The specific implications of this flag are
    provider-specific.

* AWS

  * ``access_key``: The access key to use
  * ``secret_key``: The secret key to use

* GCP

  * ``project_id``: The ID of the project to use; required for most operations
  * ``credentials_file``: The path to a file containing the credentials to use; if not
    specified, the default credentials will be used
  * ``service_account``: The service account to use; required for worker processes
    on cloud-based instances to have access to system resources

In addition, all run options can be specified in a provider-specific section, in which
case they will override the global run options, if any.

Command Line Overrides
----------------------

You can specify or override any configuration value from the command line unless otherwise noted.
Simple replace any ``_`` character with ``-``:

.. code-block:: bash

    python -m cloud_tasks run \
      --config config.yaml \
      --task-file tasks.json \
      --provider aws \                 # Specify/override provider setting
      --min-cpu 8 \                    # Specify/override min_cpu setting
      --min-memory-per-cpu 16 \        # Specify/override min_memory_per_cpu setting
      --total-boot-disk-size 100 \     # Specify/override total_boot_disk_size setting
      --image ami-0123456789abcdef0 \  # Specify/override image setting
      --job-id my-processing-job \     # Specify/override job_id setting
      --instance-types t3- m5-         # Specify/override instance_types and
                                       # restrict to t3 and m5 instance families

.. note::
   The priority of settings is: Command Line > Provider-Specific Config > Global Run Config > System Defaults

You will be notified when overrides occur. For example:

.. code-block:: text

    run:
      min_cpu: 2
    gcp:
      min_cpu: 8

    2025-06-03 14:04:55.668 - cloud_tasks.common.config - WARNING - Overriding run.min_cpu=2 with gcp.min_cpu=8

or:

.. code-block:: text

    $ cloud_tasks manage_pool --config config.yml --min-cpu 16

    2025-06-03 14:04:33.848 - cloud_tasks.common.config - WARNING - Overloading run.min_cpu=2 with CLI=16


Examples
--------

The Simplest Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~

For GCP, the simplest configuration useable for all functions consists of a provider name,
a job ID, a project ID, a region, and a startup script.

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

Given the lack of
:ref:`configuration options to constrain the instance type <config_compute_instance_options>`,
the system will select the ``e2-highcpu-32`` instance type. This is the lowest-memory
version of GCP's most economical instance type, costing $0.02475/vCPU/hour as of this
writing. It selects the 32-vCPU version, which is the maximum number of vCPUs available in
a single instance for the ``e2`` family, because the cost of the boot disk (which is
per-instance instead of per-vCPU) is amortized over the greatest number of vCPUs. However,
the lack of
:ref:`configuration options to constain the number of instances <config_number_of_instances_options>`
means the system will create the default maximum number of instances, 10,
which will result in the creation of 320 vCPUs and a burn rate of $7.92/hour, which may be
more than required depending on the actual workload. Note that in addition to the
default maximum number of instances being 10, the default maximum total price per hour is
$10.00, which is designed to limit the user's exposure to a high burn rate without explicitly
asking for it.

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

If the startup script was present in a file, no configuration file would be needed
at all:

.. code-block:: bash

    $ cloud_tasks manage_pool \
      --provider gcp \
      --job-id my-processing-job \
      --project-id my-project-id \
      --region us-central1 \
      --startup-script-file startup.sh

Constraining the Instance Type and Containing Costs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example uses more sophisticated constraints to limit the instance types and number of
instances to use. First, we want to use slightly higher-performance processors and choose
the ``n`` series using a balanced persistent boot disk. We want to limit instance types to
those that have at least 8 but not more than 40 vCPUs; we might choose these numbers to
balance parallelism with the network and disk bandwidth available on a single instance. At
the same time, we know that our tasks are themselves parallel internally, and require 4
vCPUs per task for optimal performance. They also require memory of at least 32 GB per
task. Finally, since we have a large number of tasks to process but our task code is still
experimental, we are concerned about starting too many instances at once and thus having a
high burn rate in case something goes wrong and we want to stop the job in the middle when
we detect a problem. We set limits of 20 instances total, 100 simultaneous tasks, and a
burn rate of $15.00 per hour. Whichever of these is most constraining will determine the
total number of instances that will be started.

.. code-block:: yaml

    provider: gcp
    gcp:
      job_id: my-processing-job
      project_id: rfrench
      region: us-central1
      instance_types: ["^n2-.*", "^n3-.*", "^n4-.*"]
      min_cpu: 8
      max_cpu: 40
      cpus_per_task: 4
      min_memory_per_task: 32
      max_instances: 20
      max_simultaneous_tasks: 100
      max_total_price_per_hour: 15.00
      boot_disk_types: pd-balanced
      startup_script: |
        #!/bin/bash
        echo "Hello, world!"

In this case, the system starts by looking at all available ``n2-``, ``n3-``, and ``n4-``
instance types that meet our vCPU and memory constraints while minimizing price per vCPU.
This results in the selection of ``n4-highmem-32`` as the optimal instance type with the
lowest cost of $0.0622/vCPU/hour while supporting the most vCPUs in a single instance.
For the number of instances, the system starts with the maximum allowed, 20. However, with
a maximum of 100 simultaneous tasks, 32 vCPUs, and 4 vCPUs per task, this is reduced to 12.
Finally, at a cost of $1.99/hour for each instance, the price limit of $15.00 per hour
sets the final number of instances to 7 for a total cost of $13.93/hour.

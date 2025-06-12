GCP-Specific Documentation
==========================


Known Issues
------------

- The maximum queue visibility timeout allowed by GCP Pub/Sub is 600 seconds. This means
  that if your task takes longer than 600 seconds to complete, it will be retried even if
  it's still in process on another worker. The only current workaround is to break your
  task into smaller chunks so that none of them exceed 600 seconds.


Setup
-----

- Be sure to enable the Pub/Sub API for the project you are using by visiting the `Pub/Sub`
  page in the Google Cloud console. To verify that the API is enabled, visit
  https://console.cloud.google.com/apis/library/pubsub.googleapis.com


.. _gcp_authentication:

Authentication to GCP
---------------------

- Authentication is required to access any GCP features. If can be provided by using an
  explicit credentials file (using the ``credentials_file`` configuration option) or by
  using the Application Default Credentials (which are initialized with
  ``gcloud auth application-default login``).

- A Project ID is required to access many GCP features. It may be specified with the
  ``project_id`` configuration option. If the Project ID is not provided and Application
  Default Credentials are being used, the Project ID will be extracted from the Application
  Default Credentials.


.. _gcp_region_and_zones:

Region and Zones
----------------

- A Region is required to access many GCP features. It may be specified with the ``region``
  configuration option.

- If the ``region`` configuration option is not provided, it will be extracted from the zone,
  if provided. If the ``zone`` configuration option is also not provided, it is an error.

- If the ``zone`` configuration option is specified, operations such as listing running instances,
  creating new instances, and terminating instances will be restricted to the specified zone.
  Otherwise, all zones in the specified region will be used. For creation of compute instances,
  that means each new instance will be randomly assigned to a zone.


.. _gcp_service_account:

Service Accounts
----------------

For GCP, the permissions granted to compute instances are determined by an optional
"service account". This account can be specified with the ``service_account`` configuration
option. If not provided, the compute instances will not have any credentials and thus will
have limited to no access to GCP resources.

Here is the basic process for creating a service account using the Google Cloud
web interface:

1. Go to the `IAM & Admin` page in the Google Cloud console.
2. Click on `Service Accounts` in the left sidebar.
3. Click on `Create service account`.
4. Enter a name for the service account.
5. Note the email address of the service account. This is the value to use for the
   ``service_account`` configuration option or the ``--service-account`` command line
   option.
6. Click on `Create and continue`.
7. Grant the role "Pub/Sub Editor" (this is required for the Cloud Tasks system to work)
8. Grant other roles as needed, for example "Storage Object User" (if the tasks need to read
   and write buckets).
9. Click on `Done` to save the changes.

See the
`Google Cloud documentation <https://cloud.google.com/iam/docs/service-account-overview>`_
for information on creating and managing them.


.. _gcp_queues:

Queues
------

GCP supports two types of queues using Pub/Sub:

- Standard queues guarantee *at least once* delivery of messages. These are the default.
  When used for the task queue, it is possible that a given task will be assigned to more
  than one worker at a time. This is usually a low-probability event, but it can happen.
  It is important that your worker gracefully handle this situation.

- Exactly-once queues guarantee *exactly once* delivery of messages. In this case,
  a given task is guaranteed to be assigned to exactly one worker.

Unfortunately, exactly-once queues have fundamental difficulties that are still being
worked out in Cloud Tasks and are thus not recommended.

Standard queues have different difficulties: It is impossible to determine how many
messages are remaining in the queue. Thus it is not possible to automatically scale the
number of workers based on the number of tasks remaining. Whenever when you use a command
such as ``show_queue`` that returns the number of tasks remaining, it will return a
maximum of 10. This number if a lower bound, possibly drastically so.


.. _gcp_compute_instances:

Compute Instances
-----------------

- Your account will have quotas for the number of instances of each type that can be created.
  Cloud Tasks does not monitor these quotas and thus may attempt to create more instances than
  are allowed. If you see an error about a quota being exceeded, you can try to create fewer
  instances or send a request to GCP to increase your quota.

- Compute Engine instances are tagged with ``rmscr-<job_id>`` so that they can be identified.

- Compute Engine instance types are per-zone, and thus listing available instance types
  requires a specific zone. If a zone is not specified, the default zone for the region will
  be used; this is the first zone returned by GCP for the region. When choosing an optimal
  instance type, if the zone is not specified, it may be possible to get the available instance
  types for the default zone, and then attempt to create that instance type in a different zone
  that doesn't support it. Thus if you are planning to use a rare instance type, you should
  specify a specific zone to use.

- On the other hand, Compute Engine pricing (both on-demand and spot) is per-region, not
  per-zone. Thus it is irrelevant which zone within a region you specify when retrieving
  pricing information, and no zone needs to be specified at all. The zone returned for
  pricing formation will always end with a wildcard character such as ``us-central1-*`` to
  indicate that it applies to all zones in the region.

- Computation of pricing does not include any extra costs associated with licensed boot
  images or reductions due to negotiated discounts.


Restrictions
~~~~~~~~~~~~

- Sole-tenant nodes are not supported.

- Discuss exactly-once queue


Boot Images
~~~~~~~~~~~

The list of currently available boot images can be found by running the ``list_images``
command. When creating instances, the boot image may be specified with the ``image``
configuration option. There are three ways to specify the image:

- If no image is specified, the default image will be used. This is the most recent
  build of Ubuntu 24.04 LTS for AMD64. Note that if you are not using an AMD64 archicture,
  you will always need to specify an image.

- You can specify an image by its family name. In this case, a non-deprecated image
  from that family will be used. If there is more than one such image, it is an error.
  Example: ``--image ubuntu-2404-lts``

- You can specify an image by its full URI. This is available by using the
  ``list_images --detail`` command. This option should only be used if you truly need to
  use a specific image. Otherwise as time progresses you will end up specifying an image
  that has been deprecated.Example:
  ``--image https://www.googleapis.com/compute/v1/projects/ubuntu-os-cloud/global/images/ubuntu-2404-lts-amd64-v20240416``


.. _gcp_boot_disk_types:

Boot Disk Types and CPU Types/Performance
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are five types of disks that can be used as boot disks, which are specified by the
following abbreviations:

- Persistent Standard (pd-standard)
- Persistent Balanced (pd-balanced)
- Persistent SSD (pd-ssd)
- Persistent Extreme (pd-extreme)
- HyperDisk Balanced (hd-balanced)

Not all boot disk types are supported by all instance types. When choosing optimal
instance types, if no boot disk type is specified, all supported types will be considered
as fair game, possibly resulting in the use of the slowest (and thus cheapest) disk type.
If you do not want to use a particular type (for example you want to avoid using the slow
HDD type `Standard`), you can specify the types you are willing to use with the
``boot_disk_types`` option. When computing pricing, a separate price will be computed for
each instance type for each boot disk type it supports. Here are examples of how to specify
the boot disk types:

.. code-block:: yaml

    boot_disk_types: pd-ssd

or

.. code-block:: yaml

    boot_disk_types: [pd-standard, pd-balanced, pd-ssd]

or

.. code-block:: bash

    cloud_tasks <command> --boot-disk-types pd-ssd

or

.. code-block:: bash

    cloud_tasks <command> --boot-disk-types pd-standard pd-balanced pd-ssd

The ``pd-extreme`` disk type requires the specification of the number of provisioned IOPS
using the ``boot_disk_iops`` configuration option. If not specified, the default number of
IOPS (3,120) will be used. The ``hd-balanced`` disk type requires the specification of the
number of provisioned IOPS, and also requires the specification of the amount of
provisioned throughput in MB/s using the ``boot_disk_throughput`` configuration option. If
not specified, the default amount of throughput (170 MB/s) will be used.

Note that different instances and boot disk types have different limits on the number of IOPS
and the amount of throughput, and also the minimum and maximum disk size. These limits are
not enforced in the Cloud Tasks system and it is your responsibility to ensure that what
you specify is within the supported limits; otherwise, you will see an error when instances
are being created.

Each instance type has a different type of CPU. CPUs are specified by their manufacturer's
designation, such as "Intel Ice Lake" or "AMD Milan". The performance of the CPU is
specified by a "performance rank", which is a measure of the relative performance of the
CPU, with 1 being the slowest. Performance ranks should be taken as an approximation, as
each CPU type has its own unique performance characteristics.

The performance rank can be used to determine the optimal instance type to use. When
choosing an optimal instance type, if no CPU type is specified, all supported types will
be considered as fair game, possibly resulting in the use of the slowest (and thus
cheapest) CPU type. A specific CPU type can be specified with the ``cpu_family`` configuration
option, and minimum and maximum bounds on the performance can be placed with the ``min_cpu_rank``
and ``max_cpu_rank`` configuration options.

Below is a list of supported machine instance types and their supported boot disk types, along
with CPU family and performance rank.


.. list-table::
   :header-rows: 1

   * - Machine Type
     - St
     - Bal
     - Ex
     - SSD
     - HD
     - Processor Type
     - Perf. Rank

   * - **General Purpose**
     -
     -
     -
     -
     -
     -
     -
   * - c3
     -
     - X
     -
     - X
     - X
     - Intel Ice Lake
     - 16
   * - c3d
     -
     - X
     -
     - X
     - X
     - AMD Milan
     - 17
   * - c4
     -
     -
     -
     -
     - X
     - Intel Ice Lake
     - 16
   * - c4a
     -
     -
     -
     -
     - X
     - AMD Milan
     - 17
   * - c4d
     -
     -
     -
     -
     -
     - Intel Ice Lake
     - 16
   * - e2
     - X
     - X
     - X
     - X
     -
     - Intel Cascade Lake
     - 12
   * - f1
     - X
     - X
     - X
     - X
     -
     - Intel Cascade Lake
     - 12
   * - g1
     - X
     - X
     - X
     - X
     -
     - Intel Cascade Lake
     - 12
   * - n1
     - X
     - X
     - X
     - X
     -
     - Intel Skylake
     - 11
   * - n2
     - X
     - X
     - X
     - X
     -
     - Intel Cascade Lake
     - 12
   * - n2d
     - X
     - X
     - X
     - X
     - X
     - AMD Rome
     - 13
   * - n4
     -
     -
     -
     -
     - X
     - Intel Ice Lake
     - 16
   * - t2a
     - X
     - X
     - X
     - X
     -
     - AMD Milan
     - 17
   * - t2d
     - X
     - X
     -
     - X
     -
     - AMD Rome
     - 13

   * - **Compute Optimized**
     -
     -
     -
     -
     -
     -
     -
   * - c2
     - X
     - X
     - X
     - X
     -
     - Intel Cascade Lake
     - 12
   * - c2d
     - X
     - X
     - X
     - X
     -
     - AMD Rome
     - 13
   * - h3
     -
     - X
     -
     -
     - X
     - Intel Ice Lake
     - 16

   * - **Memory Optimized**
     -
     -
     -
     -
     -
     -
     -
   * - m1
     - X
     - X
     - X
     - X
     - X
     - Intel Skylake
     - 11
   * - m2
     - X
     - X
     - X
     - X
     - X
     - Intel Cascade Lake
     - 12
   * - m3
     - X
     - X
     - X
     - X
     - X
     - Intel Ice Lake
     - 16
   * - m4
     -
     -
     -
     -
     - X
     - Intel Ice Lake
     - 16
   * - x4
     -
     -
     -
     -
     - X
     - Intel Ice Lake
     - 16

   * - **Storage Optimized**
     -
     -
     -
     -
     -
     -
     -
   * - z3
     -
     - X
     -
     - X
     - X
     - Intel Ice Lake
     - 16

   * - **Accelerator Optimized**
     -
     -
     -
     -
     -
     -
     -
   * - a2
     - X
     - X
     - X
     - X
     -
     - Intel Cascade Lake
     - 12
   * - a3
     -
     - X
     -
     - X
     - X
     - Intel Ice Lake
     - 16
   * - a4
     -
     -
     -
     -
     - X
     - Intel Ice Lake
     - 16
   * - ct6e
     -
     -
     -
     -
     - X
     - Intel Ice Lake
     - 16
   * - g2
     - X
     - X
     -
     - X
     -
     - Intel Cascade Lake
     - 12

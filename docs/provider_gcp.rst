GCP-Specific Documentation
==========================

- How regions and zones are handled for pricing, spot pricing, compute engine allocation

- Sole-tenant nodes are not supported.

- Setting up a service account and roles required

- Check quotas for CPU type - we don't

- If "credentials_file" is not provided, the default application credentials will be
  used.
- If "project_id" is not provided, and "credentials_file" is provided, the project ID
  will be extracted from the credentials file.
- "project_id" is required if application default credentials are not used.
- If "region" is not provided, it will be extracted from the zone. If zone is also not
  provided, it is an error.
- If "zone" is not provided and is not otherwise specified, a random zone will be chosen.
- Compute Engine instances are tagged with "rmscr-<job_id>".
- There are no instance types that have non-SSD storage so the values returned from
  get_available_instance_types() will always have "boot_disk_gb" set to 0.
- Compute Engine instance types are per-zone, and if a zone is not specified the default
  zone for the region will be used. This is the first zone returned by GCP for the region.
- "service_account" is optional, but if not provided the instance will not have any
  credentials.
- Zone names can end with a wildcard ``-*`` to indicate that the instance can be started
  in any zone in the region.
- GCP pricing is per-region for both on-demand and spot pricing; wildcards are returned
  for the zone.
- GCP pricing does not include the cost of licensed boot images
- Boot disk types:
  - Standard
  - Balanced
  - SSD
  - Extreme
- Pricing will not include any negotiated discounts


.. _gcp_boot_disk_types:

Boot Disk Types
===============

Below is a list of supported machine instance types and the boot disk types that they support.
When pricing or selecting one of these instances, if you do not specify a boot disk type, all of the
supported types will be used. You can specify one or more type with the ``boot_disk_types``
configuration option or the ``--boot-disk-types`` command line option like this:

The ``pd-extreme`` requires the specification of the number of provisioned IOPS using the
``boot_disk_iops`` configuration option or the ``--boot-disk-iops`` command line option. If not
specified, the default number of IOPS (3,120) will be used. The ``hd-balanced`` disk type
requires the specification of the number of provisioned IOPS, and also requires the
specification of the amount of provisioned throughput in MB/s using the
``boot_disk_throughput`` configuration option or the ``--boot-disk-throughput`` command line
option. If not specified, the default amount of throughput (170) will be used.

Note that different instances and boot disk types have different limits on the number of IOPS
and the amount of throughput, and also the minimum and maximum disk size. These limits are
not encoded in the ``cloud_tasks`` system and it is your responsibility to ensure that what
you specify is within the supported limits. Otherwise you will see an error when instances
are being created.

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


.. list-table::
   :header-rows: 1

   * - Machine Type
     - pd-standard
     - pd-balanced
     - pd-extreme
     - pd-ssd
     - hd-balanced

   * - **General Purpose**
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
   * - c3d
     -
     - X
     -
     - X
     - X
   * - c4
     -
     -
     -
     -
     - X
   * - c4a
     -
     -
     -
     -
     - X
   * - c4d
     -
     -
     -
     -
     -
   * - e2
     - X
     - X
     - X
     - X
     -
   * - f1
     - X
     - X
     - X
     - X
     -
   * - g1
     - X
     - X
     - X
     - X
     -
   * - n1
     - X
     - X
     - X
     - X
     -
   * - n2
     - X
     - X
     - X
     - X
     -
   * - n2d
     - X
     - X
     - X
     - X
     - X
   * - n4
     -
     -
     -
     -
     - X
   * - t2a
     - X
     - X
     - X
     - X
     -
   * - t2d
     - X
     - X
     -
     - X
     -

   * - **Compute Optimized**
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
   * - c2d
     - X
     - X
     - X
     - X
     -
   * - h3
     -
     - X
     -
     -
     - X

   * - **Memory Optimized**
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
   * - m2
     - X
     - X
     - X
     - X
     - X
   * - m3
     - X
     - X
     - X
     - X
     - X
   * - m4
     -
     -
     -
     -
     - X
   * - x4
     -
     -
     -
     -
     - X

   * - **Storage Optimized**
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

   * - **Accelerator Optimized**
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
   * - a3
     -
     - X
     -
     - X
     - X
   * - a4
     -
     -
     -
     -
     - X
   * - ct6e
     -
     -
     -
     -
     - X
   * - g2
     - X
     - X
     -
     - X
     -

.. list-table::
   :header-rows: 1

   * - Machine Type
     - Processor Type
     - Performance Rank

   * - **General Purpose**
     -
     -
   * - c3
     - Intel Ice Lake
     - 16
   * - c3d
     - AMD Milan
     - 17
   * - c4
     - Intel Ice Lake
     - 16
   * - c4a
     - AMD Milan
     - 17
   * - c4d
     - Intel Ice Lake
     - 16
   * - e2
     - Intel Cascade Lake
     - 12
   * - f1
     - Intel Cascade Lake
     - 12
   * - g1
     - Intel Cascade Lake
     - 12
   * - n1
     - Intel Skylake
     - 11
   * - n2
     - Intel Cascade Lake
     - 12
   * - n2d
     - AMD Rome
     - 13
   * - n4
     - Intel Ice Lake
     - 16
   * - t2a
     - AMD Milan
     - 17
   * - t2d
     - AMD Rome
     - 13

   * - **Compute Optimized**
     -
     -
   * - c2
     - Intel Cascade Lake
     - 12
   * - c2d
     - AMD Rome
     - 13
   * - h3
     - Intel Ice Lake
     - 16

   * - **Memory Optimized**
     -
     -
   * - m1
     - Intel Skylake
     - 11
   * - m2
     - Intel Cascade Lake
     - 12
   * - m3
     - Intel Ice Lake
     - 16
   * - m4
     - Intel Ice Lake
     - 16
   * - x4
     - Intel Ice Lake
     - 16

   * - **Storage Optimized**
     -
     -
   * - z3
     - Intel Ice Lake
     - 16

   * - **Accelerator Optimized**
     -
     -
   * - a2
     - Intel Cascade Lake
     - 12
   * - a3
     - Intel Ice Lake
     - 16
   * - a4
     - Intel Ice Lake
     - 16
   * - ct6e
     - Intel Ice Lake
     - 16
   * - g2
     - Intel Cascade Lake
     - 12

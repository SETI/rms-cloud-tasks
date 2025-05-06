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


Boot Disk Types
===============

#. pd-standard
#. pd-balanced
#. pd-extreme
#. pd-ssd
#. hd-balanced

.. list-table::
   :header-rows: 1

   * - Type
     - 1
     - 2
     - 3
     - 4
     - 5
     - Processor Type
     - Ranking

   * - c4a
     -
     -
     -
     -
     - X
     - Google Axion
     - 12
   * - c4d
     -
     -
     -
     -
     -
     - AMD Turin
     - 11
   * - c4
     -
     -
     -
     -
     - X
     - Intel Emerald Rapids
     - 10
   * - m4
     -
     -
     -
     -
     - X
     - Intel Emerald Rapids
     - 10
   * - n4
     -
     -
     -
     -
     - X
     - Intel Emerald Rapids
     - 10
   * - c3d
     -
     - X
     -
     - X
     - X
     - AMD Genoa
     - 9
   * - c3
     -
     - X
     -
     - X
     - X
     - Intel Sapphire Rapids
     - 8
   * - h3
     -
     - X
     -
     -
     - X
     - Intel Sapphire Rapids
     - 8
   * - x4
     -
     -
     -
     -
     - X
     - Intel Sapphire Rapids
     - 8
   * - z3
     -
     - X
     -
     - X
     - X
     - Intel Sapphire Rapids
     - 8
   * - n2d
     - X
     - X
     - X
     - X
     - X
     - AMD Milan
     - 6
   * - c2d
     - X
     - X
     - X
     - X
     -
     - AMD Milan
     - 6
   * - t2d
     - X
     - X
     -
     - X
     -
     - AMD Milan
     - 6
   * - t2a
     - X
     - X
     - X
     - X
     -
     - Ampere Altra
     - 7
   * - m3
     - X
     - X
     - X
     - X
     - X
     - Intel Ice Lake
     - 5
   * - c2
     - X
     - X
     - X
     - X
     -
     - Intel Cascade Lake
     - 4
   * - m2
     - X
     - X
     - X
     - X
     - X
     - Intel Cascade Lake
     - 4
   * - n2
     - X
     - X
     - X
     - X
     -
     - Intel Cascade Lake
     - 4
   * - m1
     - X
     - X
     - X
     - X
     - X
     - Intel Skylake
     - 3
   * - e2
     - X
     - X
     - X
     - X
     -
     - Intel Broadwell
     - 2
   * - n1
     - X
     - X
     - X
     - X
     -
     - Intel Haswell
     - 1
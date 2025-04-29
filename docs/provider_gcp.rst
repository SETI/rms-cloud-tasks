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
- Zone names can end with a wildcard "-*" to indicate that the instance can be started
  in any zone in the region.
- GCP pricing is per-region for both on-demand and spot pricing; wildcards are returned
  for the zone.

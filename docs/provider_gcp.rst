GCP-Specific Documentation
==========================


Queue Visibility Timeout
------------------------

The maximum queue visibility timeout (ack deadline) allowed by GCP Pub/Sub is 600 seconds.
Tasks are not limited to that duration, because the worker renews the visibility timeout of
each running task roughly every half of that interval, extending it for as long as the task
keeps running (up to ``--max-runtime``). A task that takes hours is therefore not redelivered
to another worker while it is still making progress.

If a worker dies, its renewals stop and the task becomes visible again within at most 600
seconds, which is how a crashed worker's task gets picked up by someone else. The same
mechanism means a worker that is alive but unable to reach Pub/Sub - for example during a
network partition long enough for the deadline to lapse - can have its task redelivered and
executed a second time. Tasks should be written to tolerate this; see
:ref:`gcp_queues` for the delivery guarantees the queue types provide.


Setup
-----

- Be sure to enable the Pub/Sub API for the project you are using by visiting the `Pub/Sub`
  page in the Google Cloud console. To verify that the API is enabled, visit
  https://console.cloud.google.com/apis/library/pubsub.googleapis.com

- The Cloud Billing API must also be enabled, because instance prices are read from the
  Cloud Billing Catalog API when choosing an instance type. No permission has to be granted
  for it, as the pricing data it returns is public, but the API itself has to be on:
  https://console.cloud.google.com/apis/library/cloudbilling.googleapis.com

- Compute Engine must be enabled to create instances:
  https://console.cloud.google.com/apis/library/compute.googleapis.com


.. _gcp_authentication:

Authentication to GCP
---------------------

- Authentication is required to access any GCP features. If can be provided by using an
  explicit credentials file (using the ``credentials_file`` configuration option) or by
  using the Application Default Credentials (which are initialized with
  ``gcloud auth application-default login``).

- Application Default Credentials created by ``gcloud auth application-default login``
  belong to a person, not to a service, and expire; in many organizations within 16 hours
  or less. A job outlives the command that starts it, and once its credentials have expired
  it can no longer start, monitor or *terminate* its instances, which then keep running,
  and costing money, until someone shuts them down by hand. ``cloud_tasks run`` warns about
  this before it starts any instances and, when run interactively, asks whether to
  continue. For a job that will run unattended, give it a service account instead; see
  :ref:`gcp_service_account`.

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

There are two identities involved in a job, and they do different things:

- The **runner** is ``cloud_tasks`` itself, wherever you run it. It chooses an instance
  type, creates and terminates instances, and creates and uses the task and event queues.

- The **workers** are the compute instances the runner creates. They receive tasks from
  the queue, report events back to it, and do whatever the tasks themselves do, such as
  reading and writing buckets.

The two can be the same service account, but they need not be, and giving the workers only
what the tasks need is the safer arrangement: worker instances run code from your startup
script on a machine anyone with instance access can reach.

Three configuration options select these identities:

.. list-table::
   :header-rows: 1
   :widths: 25 30 45

   * - Option
     - Identity
     - How it authenticates
   * - ``credentials_file``
     - the runner
     - a service account key file on the machine running ``cloud_tasks``
   * - ``runner_service_account``
     - the runner
     - impersonation from whatever credentials are already available; no file
   * - ``worker_service_account``
     - the worker instances
     - the instance metadata server; no file, and nothing to distribute

``credentials_file`` and ``runner_service_account`` are two answers to the same question,
and only the first survives a personal login expiring. See
:ref:`gcp_runner_authentication` below.

If ``worker_service_account`` is not specified, the compute instances have no credentials
at all and can reach little or nothing in GCP, including the event queue.


Creating a service account
~~~~~~~~~~~~~~~~~~~~~~~~~~

With the ``gcloud`` command line, where ``<PROJECT_ID>`` is your project:

.. code-block:: bash

    gcloud iam service-accounts create cloud-tasks-runner \
        --project=<PROJECT_ID> --display-name="cloud_tasks runner"
    gcloud iam service-accounts create cloud-tasks-worker \
        --project=<PROJECT_ID> --display-name="cloud_tasks worker"

That gives two accounts named
``cloud-tasks-runner@<PROJECT_ID>.iam.gserviceaccount.com`` and
``cloud-tasks-worker@<PROJECT_ID>.iam.gserviceaccount.com``. The email address is what the
configuration options take.

The same thing through the Google Cloud web interface:

1. Go to the `IAM & Admin` page in the Google Cloud console.
2. Click on `Service Accounts` in the left sidebar.
3. Click on `Create service account`.
4. Enter a name for the service account.
5. Note the email address of the service account. This is the value to use for the
   ``runner_service_account`` or ``worker_service_account`` configuration option, or for
   the ``--runner-service-account`` or ``--worker-service-account`` command line option.
6. Click on `Create and continue`.
7. Grant the roles described below.
8. Click on `Done` to save the changes.

See the
`Google Cloud documentation <https://cloud.google.com/iam/docs/service-account-overview>`_
for information on creating and managing service accounts.


What the runner has to be allowed to do
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Role
     - Why
   * - ``roles/compute.instanceAdmin.v1``
     - create, list and terminate instances, and read machine types and images
   * - ``roles/iam.serviceAccountUser``
     - attach the worker service account to the instances it creates; granted **on the
       worker service account**, not on the project
   * - ``roles/pubsub.editor``
     - create and use the task and event queues
   * - ``roles/monitoring.viewer``
     - read the task queue depth

Attaching a service account to a new instance is a permission in its own right
(``iam.serviceAccounts.actAs``), and ``roles/compute.instanceAdmin.v1`` does not include
it. It is easy to miss, because the basic `Owner` and `Editor` roles do include it, so a
person who has either of those never notices; a service account created for the runner
alone does, and instance creation fails with a permission error naming ``actAs``.

Reading instance prices needs no role of any kind: the Cloud Billing Catalog API returns
public data, and only has to be enabled on the project as described under `Setup`_.

.. code-block:: bash

    RUNNER=cloud-tasks-runner@<PROJECT_ID>.iam.gserviceaccount.com
    WORKER=cloud-tasks-worker@<PROJECT_ID>.iam.gserviceaccount.com

    for ROLE in roles/compute.instanceAdmin.v1 roles/pubsub.editor roles/monitoring.viewer
    do
        gcloud projects add-iam-policy-binding <PROJECT_ID> \
            --member="serviceAccount:$RUNNER" --role="$ROLE"
    done

    gcloud iam service-accounts add-iam-policy-binding "$WORKER" \
        --project=<PROJECT_ID> --member="serviceAccount:$RUNNER" \
        --role=roles/iam.serviceAccountUser

If the runner is not running as a service account at all, these are the roles the person
running it needs instead.


What the workers have to be allowed to do
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The worker service account needs ``roles/pubsub.editor``, which is what lets a worker
receive tasks and report events, plus whatever the tasks themselves reach - for example
``roles/storage.objectUser`` on a bucket the tasks read and write.

.. code-block:: bash

    gcloud projects add-iam-policy-binding <PROJECT_ID> \
        --member="serviceAccount:$WORKER" --role=roles/pubsub.editor


.. _gcp_runner_authentication:

How the runner authenticates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

There are three ways to run ``cloud_tasks`` as a service account, and one way to run it as
yourself.

**A key file.** Create a key for the runner account and point the configuration at it:

.. code-block:: bash

    gcloud iam service-accounts keys create /path/to/runner-key.json \
        --iam-account="$RUNNER" --project=<PROJECT_ID>
    chmod 600 /path/to/runner-key.json

.. code-block:: yaml

    gcp:
      project_id: <PROJECT_ID>
      credentials_file: /path/to/runner-key.json
      worker_service_account: cloud-tasks-worker@<PROJECT_ID>.iam.gserviceaccount.com

That file *is* the credential: anyone who has it is that service account until the key is
deleted. Keep it outside your repository, readable only by you, and never commit it. This
is the only option that keeps working when the person who started the job is not logged in
any more, so it is the one to use for a long unattended run.

Many organizations forbid service account keys through the
``iam.disableServiceAccountKeyCreation`` policy, in which case ``keys create`` fails and
one of the other options is needed.

**Impersonation.** Name the account instead of holding a key for it:

.. code-block:: bash

    gcloud iam service-accounts add-iam-policy-binding "$RUNNER" \
        --project=<PROJECT_ID> --member="user:<YOUR_LOGIN>" \
        --role=roles/iam.serviceAccountTokenCreator

.. code-block:: yaml

    gcp:
      project_id: <PROJECT_ID>
      runner_service_account: cloud-tasks-runner@<PROJECT_ID>.iam.gserviceaccount.com
      worker_service_account: cloud-tasks-worker@<PROJECT_ID>.iam.gserviceaccount.com

Every call ``cloud_tasks`` makes is then made as the runner account, so what a run may do
is decided by that account rather than by whoever is logged in. You still authenticate
first with ``gcloud auth application-default login``.

Impersonation does **not** make a personal login last longer. The impersonated token is
refreshed with the credentials underneath it, so a login that expires takes the
impersonation with it, and ``cloud_tasks run`` still warns about it.

**A machine that is already the service account.** Run ``cloud_tasks`` from a Compute
Engine instance created with the runner service account attached to it. The metadata server
issues tokens for as long as the instance exists: no key to protect, no expiry, and nothing
to configure beyond ``project_id`` and ``worker_service_account``.

**As yourself.** With none of these set, ``cloud_tasks`` runs on your own Application
Default Credentials and needs the roles listed above granted to you. This is convenient for
short interactive runs and is the case the expiry warning in :ref:`gcp_authentication` is
about.


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
   * - n4a
     -
     - X
     -
     -
     -
     - Google Axion
     - 27
   * - n4d
     -
     - X
     -
     -
     -
     - AMD Turin
     - 26
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

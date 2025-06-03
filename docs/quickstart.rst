Quick Start Guide
=================

Below is a short introduction to using Cloud Tasks, providing the minimum steps required to
get started. Please see the rest of this user's guide for more detailed information.

Step 1: Install the Cloud Tasks CLI and Python Module
-----------------------------------------------------

Activate your Python virtual environment, as appropriate, then:

.. code-block:: bash

    pip install rms-cloud-tasks


Step 2: Modify Your Code to be a Worker
---------------------------------------

#. Modify your code to have a single function entry point; we will call this ``process_task`` here.
   The function takes three arguments:

   - ``task_id: str``: The ID of the task as provided in the tasks file (described below).
   - ``task_data: Dict[str, Any]``: The data for the task as provided in the tasks file (described
     below). This should be the only data your function needs to process the task; it should not
     use command line arguments.
   - ``worker_data: cloud_tasks.worker.WorkerData``: The WorkerData object. This provides access to
     configuration options that were set by
     :ref:`environment variables or command line arguments <worker_environment_variables>` as well
     as real-time information about the worker's and computer's status, such as whether the
     :ref:`termination of a spot instance <worker_spot_instances>` is imminent. See
     :ref:`worker_api` for more information about the Worker object.

   The function must return a tuple of two elements:

   - The first is bool indicating whether the task was processed successfully. This value is
     used to determine if the task should be retried (perhaps there was an I/O error or the process
     ran out of memory). Return ``True`` if the task was processed successfully and should be
     permanently removed from the queue; return ``False`` if the task should be retried. Note that
     if the task failed in a deterministic way (e.g. the task data was malformed), you should
     return ``True`` so that the task is not retried; otherwise, the task will be retried
     indefinitely with no hope of success.

   - The second is the result of the task. This can be any Python object that can be converted to
     JSON. A return value of ``None`` is converted to an empty dictionary. This result is returned
     in the results queue so that it can be logged.

#. Modify your code to save its results to cloud-based storage, if needed. For example, if running
   on a local workstation, you might save the results to a file in a local directory, but if
   running on a cloud compute instance, you might save the results to a cloud-based storage bucket.
   One particularly easy way to handle this is to use the
   `FileCache package <https://rms-filecache.readthedocs.io/>`, which provides a
   provider-independent API for reading and writing files from/to the local disk or cloud-based
   storage.

#. Modify your code's ``__main__`` block to use the ``cloud_tasks`` worker
   support:

    .. code-block:: python

        import asyncio
        import sys
        from cloud_tasks.worker import Worker

        def process_task(task_id: str, task_data: Dict[str, Any], worker_data: WorkerData) -> Tuple[bool, Any]:
            """
            Process a task.

            Args:
                task_id: The unique ID of the task.
                task_data: The data required to process the task.
                worker_data: WorkerData object (useful for retrieving information about the
                    local environment and polling for shutdown notifications)

            Returns:
                Tuple of (success, result)
            """
            [... your code ...]

        async def main():
            # These command line arguments are used to override environment variables when
            # specifying the behavior of the worker process manager.
            worker = Worker(process_task, args=sys.argv[1:])
            await worker.start()

        if __name__ == "__main__":
            asyncio.run(main())


Step 3: Create a Tasks File
---------------------------

The data for your tasks must be provided in a JSON (``.json``) or YAML (``.yml`` or
``.yaml``) file with the following format:

YAML:

.. code-block:: yaml

    - task_id: task-1
      data:
        key1: value1
        key2: value2

JSON:

.. code-block:: json

    [
      {
        "task_id": "task-1",
        "data": {
          "key1": "value1",
          "key2": "value2"
        }
      }
    ]

Both ``task_id`` and ``data`` are required keys. ``task_id`` must be a string that is unique
within all tasks that will be processed at the same time. ``data`` must be a dictionary containing
zero or more key-value pairs. The values can be as complicated as necessary but must be able to
be represented in JSON/YAML format.


Interlude - Running Tasks Locally
---------------------------------

At this point you have done all of the preparation needed to run the tasks locally on your
workstation. This could be useful for debugging your initial implementation or, if you
have access to a high-end workstation with enough parallelism you may always want to run
your code locally and not take advantage of a cloud provider's (costly) resources.

To run tasks locally, set up your environment as needed (install Python, create and activate a
virtual environment, install the dependencies and the ``rms-cloud-tasks`` package, etc.). Then
execute your worker code from the command line as follows:

.. code-block:: bash

    python3 my_worker.py --tasks my_tasks.json

This will run your ``process_task`` function once for each task. To increase the parallelism, you
can specify the number of simultaneous tasks to run:

.. code-block:: bash

    python3 my_worker.py --tasks my_tasks.json --num-simultaneous-tasks 10


Step 4: Create a Startup Script
-------------------------------

The startup script will be run as root on each cloud compute instance that is started to
process tasks (it will not be run on a local workstation). At a minimum, the startup
script should install your project and its dependencies and then run your worker code. It
may also do more complicated operations such as setting up authentication, attaching
additional disks or GPUs, copying static data files to the local disk, etc., as well as
define environment variables that will be accessible to your task code. Here is an
example:

.. code-block:: bash

    apt-get update -y
    apt-get install -y python3 python3-pip python3-venv git
    cd
    git clone https://github.com/MY-ORG/MY-REPO.git
    cd MY-REPO
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    export MY_WORKER_DEST_BUCKET=gs://my-bucket/results
    python3 my_worker.py


Step 5: Create a Configuration File
-----------------------------------

The :ref:`configuration file <config>` will be used to configure the ``cloud_tasks`` commands.
Almost everything in the configuration file could also be specified as a command line option to the
``cloud_tasks`` commands, but consolating all of the configuration into a single file makes it much
simpler to run commands going forward.

At a minimum you will need to specify:

- ``provider``: The cloud provider to use (``aws`` or ``gcp``).
- ``job_id``: A unique string that identifies the job.
- ``startup_script`` or ``startup_script_file``: The startup script to run on the worker.

You will also want to set some constraints on the
:ref:`number of instances <config_number_of_instances_options>` that can be started and
what :ref:`compute instance types <config_compute_instance_options>` you want to use. You may
also need to specify other options depending on the cloud provider. See the
:ref:`configuration file <config>` for more information.

Here is an example:

.. code-block:: yaml

    provider: gcp
    gcp:
      job_id: my-processing-job
      startup_script_file: startup_script.sh
      max-instances: 5
      max-cpu: 8
      min-memory-per-cpu: 4  # GB
      max-total-price-per-hour: 1.00  # USD/hour
      instance-types: "n2-"


Step 6: Load the Task Queue and Run the Worker
----------------------------------------------

The ``cloud_tasks run`` command will load the task queue and then start the compute instance
pool manager.

.. code-block:: bash

    cloud_tasks run --config myconfig.yml --tasks my_tasks.json

This will perform the following steps:

#. Create the task queue.

#. Load the tasks from ``my_tasks.json`` into the task queue.

#. Based on the constraints given in the configuration file, choose the optimal compute instance
   type.

#. Based on the constraints given in the configuration file, choose the optimal number of
   compute instances.

#. Create the chosen number of compute instances. Each will run the startup script.

#. Monitor the compute instances and replace them if they fail or are terminated.

#. Monitor the task queue and terminate the compute instances once it is empty.

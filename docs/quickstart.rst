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
     below). This should be the only data your function needs to process the task. Your
     function should not use command line arguments.
   - ``worker: cloud_tasks.worker.Worker``: The Worker object. This provides access to
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

   - The second is the result of the task. This can be any serializable Python object,
     including ``None``. This result is returned in the results queue so that it can be
     logged.


#. Modify your code's ``__main__`` block to use the ``cloud_tasks`` worker
   support:

    .. code-block:: python

        import asyncio
        import sys
        from cloud_tasks.worker import Worker

        def process_task(task_id: str, task_data: Dict[str, Any], worker: Worker) -> Tuple[bool, Any]:
            """
            Process a task.

            Args:
                task_id: The ID of the task.
                task_data: The data of the task.
                worker: The worker object.

            Returns:
                Tuple of (success, result)
            """
            [... your code ...]

        async def main():
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


Step 4: Create a Startup Script
-------------------------------

The startup script will be run as root on each cloud compute instance that is started to
process tasks. At a minimum, the startup script should install your project and its
dependencies and then run your worker code. It may also do more complicated operations
such as setting up authentication, attaching additional disks or GPUs, copying static data
files to the local disk, etc. It may also define environment variables that will be
accessible to your task code. Here is an example:

.. code-block:: bash

    apt-get update -y
    apt-get install -y python3 python3-pip python3-venv git
    cd
    git clone https://github.com/MY-ORG/MY-REPO.git
    cd MY-REPO
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    python3 my_worker.py


Step 5: Create a Configuration File
-----------------------------------

The :ref:`configuration file <config>` will be used to configure the ``cloud_tasks`` commnand.
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

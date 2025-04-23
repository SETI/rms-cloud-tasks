Parallel Addition Example
=========================

A simple example is included in the ``rms-cloud-tasks`` repo. The task accepts two integers, adds them
together, and stores the result in a cloud bucket. It can then delay a programmable amount of time
to simulate a task that takes more time and emphasize the need for parallelism. The example includes
a file describing 10,000 tasks. If the delay is set to 1 second, this means the complete set of
tasks will require 10,000 CPU-seconds, or about 2.8 hours on a single CPU. Running with 100-fold
parallelism will reduce the time to around two minutes.

Specifying Tasks
----------------

The task queue is stored in whatever queueing system is native to the cloud provider being used.
Tasks are specified using a JSON file consisting of a list of dictionaries with the format:

.. code-block:: json

    [
      {
        "id": "task-name-1",
        "data": {
          "some_arg1": "value",
          "some_arg2": "value"
        }
      }
    ]

For example, the tasks for the addition example look like:

.. code-block:: json

    [
      {
        "id": "addition-task-000001",
        "data": {
          "num1": -84808,
          "num2": -71224
        }
      },
      {
        "id": "addition-task-000002",
        "data": {
          "num1": 511,
          "num2": -44483
        }
      }
    ]

To load the tasks into the queue, you run the ``cloud_tasks`` command line program with, at
a minimum, the name of the cloud provider and a job ID. These can also be specified in a
configuration file. For Google Cloud you also need to specify the project ID. For our sample
addition task, we will get the job ID from a configuration file and specify the provider
and project ID on the command line, since these are user-specific. The configuration file
and tasks list are available in the ``rms-cloud-tasks`` repo:

.. code-block:: bash

    git clone https://github.com/SETI/rms-cloud-tasks
    cd rms-cloud-tasks

Here is the command that loads the task queue:

.. code-block:: bash

    cloud_tasks load_queue --config examples/parallel_addition/config.yml --provider gcp --project-id my-project --tasks examples/parallel_addition/addition_tasks.json

This will create the queue, if it doesn't already exist, read the tasks from the given JSON file,
and place them in the queue. If the queue already exists, the tasks will be added to those already
there.

Running Tasks
-------------

Running tasks consists of:

- Choosing an optimal instance type based on given constraints
- Creating a specified number of instances; each instance will run a specified startup script
- Monitoring the instances to make sure they continue to run, and starting new instances as necessary
- Terminating the instances when the job is complete

These steps are performed automatically.

Here is an example command that will find the cheapest compute instance in the specified region with
exactly 8 CPUs and at least 2 GB memory per CPU and create 5 of them.

.. code-block:: bash

    cloud_tasks manage_pool --config examples/parallel_addition/config.yml --provider gcp --project-id my-project --job-id addition --min-cpu 8 --max-cpu 8 --min-memory-per-cpu 2 --max-instances 5

.. note::
   - manage_pool uses info logging which is turned off by default
   - Termination time delay needs to be longer than 120 seconds

Principles for Greatest Success (and Least Frustration)
=======================================================

- Test your worker code in a local environment before attempting to run it on the cloud.
  This can be done by running the worker code directly with ``python my_worker.py`` using
  :ref:`command line options <worker_environment_variables>` to specify the needed parameters,
  including the use of a local tasks file:

  .. code-block:: bash

      python my_worker.py --tasks my_tasks.json

- When first testing in the cloud, use a single, cheap instance that runs only a single
  task a time. Specifying a small number of CPUs will automatically choose the cheapest
  option:

  .. code-block:: bash

      cloud_tasks run --tasks my_tasks.json --max-cpu 1 --max-instances 1

- When developing the startup script, using the console logging system for the given provider
  to watch the commands being executed.

Principles for Greatest Success (and Least Frustration)
=======================================================

- Use these principles to ensure your worker code will function as expected, is
  observable, and can be debugged:

  - Keep the function stateless; there should be no communication between tasks.
  - Handle all exceptions at the top level; if the program has to exit, be sure
    to return the appropriate status.
  - Return informative result messages to ease observation and debugging.
  - Clean up temporary files.
  - Release any system resources that will not automatically be released on process
    termination.
  - Log errors with sufficient context, such as complete tracebacks.

- Test your worker code in a local environment before attempting to run it on the cloud.
  This can be done by running the worker code directly with ``python <my_worker.py>``
  using :ref:`command line options <worker_environment_variables>` to specify the needed
  parameters, including the use of a local task file:

  .. code-block:: bash

      python my_worker.py --task-file my_tasks.json

  You can also simulate a spot termination notice and subsequent forced shutdown of the
  compute instance.

- When first testing in the cloud, use a single, cheap instance that runs only a single
  task a time. Specifying a small number of CPUs will automatically choose the cheapest
  option:

  .. code-block:: bash

      cloud_tasks run --task-file my_tasks.json --max-cpu 1 --max-instances 1

- When developing the startup script, use the console logging system for the given provider
  to watch the commands being executed to make sure the script is executing as expected.

- If you are copying data to the instance to be shared amoung all tasks, either do it during
  the startup script or allow your tasks to do it on-demand in a multi-processor-safe manner.

- If you want to preload large amounts of data, you can instead create custom a boot image
  that already has the data loaded on the boot disk.

- When writing results to a file, be sure to separate them in a way that makes them
  specific to the current task. The
  `FileCache package <https://rms-filecache.readthedocs.io/>` is a good way to do this,
  using the unique Task ID as the name of the cache.

- Always specify one or more boot disk types that you are willing to use. If you don't, the
  cheapest type will be chosen, which may be a slow HDD.

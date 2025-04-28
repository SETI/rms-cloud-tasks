Welcome to the documentation for Cloud Tasks
============================================

.. include:: ../README.md
   :parser: myst_parser.sphinx_
   :start-after: forks/SETI/rms-cloud-tasks)

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   quickstart
   config
   cli
   providers
   examples
   worker
   worker_api

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

Docs todo:
- Doesn't support sole tenant nodes
- Help for main command and sub commands
- GCP
  - Provider-specific doc pages. See comments in files.
  - How regions and zones are handled for pricing, spot pricing, compute engine allocation
  - Setting up a service account and roles required
  - Check quotes for CPU type - we don't

[![GitHub release; latest by date](https://img.shields.io/github/v/release/SETI/rms-cloud-tasks)](https://github.com/SETI/rms-cloud-tasks/releases)
[![GitHub Release Date](https://img.shields.io/github/release-date/SETI/rms-cloud-tasks)](https://github.com/SETI/rms-cloud-tasks/releases)
[![Test Status](https://img.shields.io/github/actions/workflow/status/SETI/rms-cloud-tasks/run-tests.yml?branch=main)](https://github.com/SETI/rms-cloud-tasks/actions)
[![Documentation Status](https://readthedocs.org/projects/rms-cloud-tasks/badge/?version=latest)](https://rms-cloud-tasks.readthedocs.io/en/latest/?badge=latest)
[![Code coverage](https://img.shields.io/codecov/c/github/SETI/rms-cloud-tasks/main?logo=codecov)](https://codecov.io/gh/SETI/rms-cloud-tasks)
<br />
[![PyPI - Version](https://img.shields.io/pypi/v/rms-cloud-tasks)](https://pypi.org/project/rms-cloud-tasks)
[![PyPI - Format](https://img.shields.io/pypi/format/rms-cloud-tasks)](https://pypi.org/project/rms-cloud-tasks)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/rms-cloud-tasks)](https://pypi.org/project/rms-cloud-tasks)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/rms-cloud-tasks)](https://pypi.org/project/rms-cloud-tasks)
<br />
[![GitHub commits since latest release](https://img.shields.io/github/commits-since/SETI/rms-cloud-tasks/latest)](https://github.com/SETI/rms-cloud-tasks/commits/main/)
[![GitHub commit activity](https://img.shields.io/github/commit-activity/m/SETI/rms-cloud-tasks)](https://github.com/SETI/rms-cloud-tasks/commits/main/)
[![GitHub last commit](https://img.shields.io/github/last-commit/SETI/rms-cloud-tasks)](https://github.com/SETI/rms-cloud-tasks/commits/main/)
<br />
[![Number of GitHub open issues](https://img.shields.io/github/issues-raw/SETI/rms-cloud-tasks)](https://github.com/SETI/rms-cloud-tasks/issues)
[![Number of GitHub closed issues](https://img.shields.io/github/issues-closed-raw/SETI/rms-cloud-tasks)](https://github.com/SETI/rms-cloud-tasks/issues)
[![Number of GitHub open pull requests](https://img.shields.io/github/issues-pr-raw/SETI/rms-cloud-tasks)](https://github.com/SETI/rms-cloud-tasks/pulls)
[![Number of GitHub closed pull requests](https://img.shields.io/github/issues-pr-closed-raw/SETI/rms-cloud-tasks)](https://github.com/SETI/rms-cloud-tasks/pulls)
<br />
![GitHub License](https://img.shields.io/github/license/SETI/rms-cloud-tasks)
[![Number of GitHub stars](https://img.shields.io/github/stars/SETI/rms-cloud-tasks)](https://github.com/SETI/rms-cloud-tasks/stargazers)
![GitHub forks](https://img.shields.io/github/forks/SETI/rms-cloud-tasks)

# Introduction

Cloud Tasks (contained in the `rms-cloud-tasks` package) is a framework for running
independent tasks on cloud providers with automatic compute instance and task queue
management. It is specifically designed for running the same code multiple times in a
batch environment to process a series of different inputs. For example, the program could
be an image processing program that takes the image filename as an argument, downloads the
image from the cloud, performs some manipulations, and writes the result to a cloud-based
location. It is very important that the tasks are completely independent; no communication
between them is supported. Also, the processing happens entirely in a batch mode: a
certain number of compute instances are created, they all process tasks in parallel, and
then the compute instances are destroyed.

`rms-cloud-tasks` is a product of the [PDS Ring-Moon Systems Node](https://pds-rings.seti.org).

# Features

Cloud Tasks is extremely easy to use with a simple command line interface and
straightforward configuration file. It supports AWS and GCP compute instances and queues
along with the ability to run jobs on a local workstation, all using a
provider-independent API. Although each cloud provider has implemented similar
functionality as part of their offering (e.g. GCP's Cloud Batch), Cloud Tasks is unique in
that it unifies all supported providers into a single, simple, universal system that does
not require learning the often-complicated details of the official full-featured services.

Cloud Tasks consists of four primary components:

- **A Python module to make parallel execution simple**
  - Allows conversion of an existing Python program to a parallel task with only a few lines
    of code
  - Supports both cloud compute instance and local machine environments
  - Executes each task in its own process for complete isolation
  - Reads task information from a cloud-based task queue or directly from a local file
  - Monitors the state of spot instances to notify tasks of upcoming preemption
- **A command line interface to manage the task queue system, that allows**
  - Loading of tasks from a JSON or YAML file
  - Checking the status of a queue
  - Purging a queue of remaining tasks
  - Deleting a queue entirely
- **A command line interface to query the cloud about available resources, given certain
  constraints**
  - Types of compute instances available, including price (both demand and spot instances)
  - VM boot images available
  - Regions and zones
- **A command line interface to manage a pool of compute instances optimized for price,
  given certain constraints**
  - Automatically finds the optimal compute instance type given pricing and other constraints
  - Automatically determines the number of simultaneous instances to use
  - Creates new instances and runs a specified startup script to execute the task manager
  - Monitors instances for failure or preemption and creates new instances as needed to keep
    the compute pool full
  - Detects when all jobs are complete and terminates the instances

# Installation

`cloud_tasks` consists of a command line interface (called `cloud_tasks`) and a Python
module (also called `cloud_tasks`). They are available via the `rms-cloud-tasks` package
on PyPI and can be installed with:

```sh
pip install rms-cloud-tasks
```

Note that this will install `cloud_tasks` into your current system Python, or into your
currently activated virtual environment (venv), if any.

If you already have the `rms-cloud-tasks` package installed but wish to upgrade to a
more recent version, you can use:

```sh
pip install --upgrade rms-cloud-tasks
```

You may also install `cloud_tasks` using `pipx`, which will isolate the installation from
your system Python without requiring the creation of a virtual environment. To install
`pipx`, please see the [installation
instructions](https://pipx.pypa.io/stable/installation/). Once `pipx` is available, you
may install `cloud_tasks` with:

```sh
pipx install rms-cloud-tasks
```

If you already have the `rms-cloud-tasks` package installed with `pipx`, you may
upgrade to a more recent version with:

```sh
pipx upgrade rms-cloud-tasks
```

Using `pipx` is only useful if you want to use the command line interface and not access
the Python module; however, it does not require you to worry about the Python version,
setting up a virtual environment, etc.

# Basic Examples

The `cloud_tasks` command line program supports many useful commands that control the task
queue, compute instance pool, and retrieve general information about the cloud in a
provider-indepent manner. A few examples are given below.

To get a list of available commands:

```bash
cloud_tasks --help
```

To get help on a particular command:

```bash
cloud_tasks load_queue --help
```

To list all ARM64-based compute instance types that have 2 to 4 vCPUs and at most 4 GB
memory per vCPU.

```bash
cloud_tasks list_instance_types \
  --provider gcp --region us-central1 \
  --min-cpu 2 --max-cpu 4 --arch ARM64 --max-memory-per-cpu 4
```

To load a JSON file containing task descriptions into the task queue:

```bash
cloud_tasks load_queue \
  --provider gcp --region us-central1 --project-id my-project \
  --job-id my-job --task-file mytasks.json
```

To start automatic creation and management of a compute instance pool:

```bash
cloud_tasks manage_pool --provider gcp --config myconfig.yaml
```

# Contributing

Information on contributing to this package can be found in the
[Contributing Guide](https://github.com/SETI/rms-cloud-tasks/blob/main/CONTRIBUTING.md).

# Links

- [Documentation](https://rms-cloud-tasks.readthedocs.io)
- [Repository](https://github.com/SETI/rms-cloud-tasks)
- [Issue tracker](https://github.com/SETI/rms-cloud-tasks/issues)
- [PyPi](https://pypi.org/project/rms-cloud-tasks)

# Licensing

This code is licensed under the [Apache License v2.0](https://github.com/SETI/rms-cloud-tasks/blob/main/LICENSE).

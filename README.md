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
independent tasks on cloud providers with automatic compute instance management. It is
specifically designed for running the same code multiple times in a batch environment to
process a series of different inputs. For example, this could be an image processing
program that takes the image filename as an argument, downloads the image from the cloud,
performs some manipulations, and writes the result to a cloud-based location. It is very
important that the tasks are completely independent; no communication between them is
supported. Also, the processing happens entirely in a batch mode: a certain number of
compute instances are created, they all process tasks in parallel, and then the compute
instances are destroyed.

`rms-cloud-tasks` is a product of the [PDS Ring-Moon Systems Node](https://pds-rings.seti.org).

# Features

- Extremely easy to use with a simple command line interface and straightforward
  configuration file
- Allows conversion of an existing Python program to a worker task with only a few lines
  of code
- Runs tasks on AWS, GCP, or Azure compute instances, or even a local workstation, using a
  provider-independent API (NOTE: Azure support is currently not finished)
- Enables the use of spot/preemptible instances to reduce costs, with graceful shutdown of
  task workers
- Automatically chooses the optimal instance type based on a wide-ranging set of
  provider-independent constraints, including cost, number of vCPUs, and amount of memory
- Uses cloud-based queueing to load and consume task

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

Using `pipx` is particularly useful if you only want to use the command line interface and
not access the Python module, as it does not require you to worry about the Python
version, setting up a virtual environment, etc.

# Basic Examples

The `cloud_tasks` command line program supports many useful commands that control the task
queue, compute instance pool, and retrieve general information about the cloud in a
provider-indepent manner.

To get a list of available commands:

```bash
cloud_tasks --help
```

To get help on a particular command:

```bash
cloud_tasks load_queue --help
```

To list all ARM64-based compute instance types that have 2 to 4 vCPUs at at most 4 GB
memory per vCPU.

```bash
cloud_tasks list_instance_types \
  --provider gcp --region us-central1 \
  --min-cpu 2 --max-cpu 4 --arch ARM64 --max-memory-per-cpu 4
```

To load a JSON file containing task descriptions into the task queue:

```bash
cloud_tasks load_queue --provider gcp --region us-central1 --project-id my-project \
  --job-id my-job --tasks mytasks.json
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

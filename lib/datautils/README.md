# owid-datautils

![version](https://img.shields.io/badge/version-0.5.3-blue)
![version](https://img.shields.io/badge/python-3.8|3.9|3.10-blue.svg?&logo=python&logoColor=yellow)
[![Build status](https://badge.buildkite.com/caba621fb64f2c7dcc692a474e68f4ead21e6ba6ee151fe3b6.svg)](https://buildkite.com/our-world-in-data/owid-datautils-unit-tests)
[![Documentation Status](https://readthedocs.org/projects/owid-datautils/badge/?version=latest)](https://docs.owid.io/projects/owid-datautils/en/latest/?badge=latest)

**owid-datautils** is a library to support the work of the Data Team at Our World in Data.

## Install

Currently no release has been published. You can install the version under development directly from GitHub:

```
pip install git+https://github.com/owid/owid-datautils-py
```

## Development

### Pre-requisites

You need Python 3.8+, `poetry` and `make` installed.

```
# Install poetry
pip install poetry
```

### Install in development mode

```
make .venv
```

### Test the code

Run:

```
# run all unit tests and CI checks
make test
```

### Other useful commands

#### Report

```
make report
```

Generate coverage and linting reportings. It is equivalent to running first `make report-coverage` and then
`make report-linting`. It also launches a local server so you can access the reports via localhost:8000 URL.

The generated reports are saved as `./reports/coverage` and `./reports/linting` (both HTML directories).

##### Coverage

```
make report-coverage
```

This will print how much of the source code is covered by the implemented tests. Additionally, it generates an HTML
directory (`.reports/coverage`), which provides a frendly view of the source code coverage.

##### Linting

```
make report-linting
```

Check if the source code passes all flake8 styling tests. Additionally, it generages an HTML directory
(`.reports/linting`), which provides a friendly view of the style issues (if any).

Flake8 configuration can be tweaked in [.flake8](.flake8) file.

#### Versioning

```
make bump [part]
```

Upgrade version in all files where it appears. `[part]` can be `patch`, `minor` ad `major`.

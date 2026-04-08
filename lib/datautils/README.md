# owid-datautils

![version](https://img.shields.io/badge/version-0.6.0-blue)
![version](https://img.shields.io/badge/python-3.10|3.11|3.12|3.13-blue.svg?&logo=python&logoColor=yellow)
[![Build status](https://badge.buildkite.com/caba621fb64f2c7dcc692a474e68f4ead21e6ba6ee151fe3b6.svg)](https://buildkite.com/our-world-in-data/owid-datautils-unit-tests)
[![Documentation Status](https://readthedocs.org/projects/owid-datautils/badge/?version=latest)](https://docs.owid.io/projects/owid-datautils/en/latest/?badge=latest)

**owid-datautils** is a library to support the work of the Data Team at Our World in Data.

## Install

```
pip install owid-datautils
```

Or install the latest development version directly from GitHub:

```
pip install git+https://github.com/owid/etl.git#subdirectory=lib/datautils
```

## Development

### Pre-requisites

You need Python 3.10+, `UV` and `make` installed.

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

#### Code Quality

```
make check
```

Format, lint, and typecheck changed files from master branch using `ruff` and `pyright`.

```
make format
```

Format code using `ruff`.

```
make lint
```

Lint code using `ruff`.

```
make check-typing
```

Run type checking using `pyright`.

#### Coverage

```
make coverage
```

Run unit tests with coverage reporting.

#### Versioning

```
make bump [part]
```

Upgrade version in all files where it appears. `[part]` can be `patch`, `minor` ad `major`.

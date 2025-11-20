---
icon: simple/python
---

# Python version update

Python releases [:octicons-link-external-16: new major versions annually](https://devguide.python.org/versions/). Our current recommendation is to add support for the new version to the project short after this happens.

New versions provide new features, but are also important for security and bug fixes. Read below to learn how to upgrade the Python version used in the ETL pipeline.

## Add support for a new Python version
- **Python versions in `lib/`**
    - Update the `requires-python` field in `pyproject.toml` of all dependencies under `lib/`: `catalog`, `datautils`, and `repack`. Refer to [:fontawesome-brands-github: this PR](https://github.com/owid/etl/pull/4448/files#diff-906a42619ca77c0da9d85d9fe334e1b69bd846d5f56632501907ae664cad9638) for reference.
    - Go back and forth with `make test` commands to verify that all dependencies are compatible with the new version. Otherwise, edit these in the `pyproject.toml` files. You may need to change some parts of the code if there are breaking changes.
    - Update the library version (field `version` in `pyproject.toml`).
    - Note: Make sure that all python versions are of the same range.
- **Python version in project**
    - Update `requires-python` in the root `pyproject.toml`.
    - Test your changes with `make test`, and make sure that all dependencies are compatible with the new version. You may need to change some parts of the code if there are breaking changes. For extra-check, remove `.venv` and rebuild it with `make .venv`.
- **Documentation files**
    - Update the versions shown in the README badge.
    - Review the "getting started" section on the [environment](../getting-started/working-environment.md) page to ensure it reflects the new version.
    - Revisit this file as well!
- **Final check**
    - Remove environemnt (`rm -fr .venv`) and create a new one with the version to be checked (`PYTHON_VERSION=3.xx.x make .venv`).
    - Run all tests with `make test-all`.

### Reflect this changes also in `ops` repository
In the ops repository we manage all the Buildkite pipelines. In there, we need to specify the python versions of the servers. Therefore, if there is a change in the python versions that the project supports, we need to reflect this in the `ops` repository.

You will need to change various bits in ops. As reference you have this [:fontawesome-brands-github: PR](https://github.com/owid/ops/pull/303/files). HOWEVER, please have a look as well at the [:fontawesome-brands-github: .buildkite](https://github.com/owid/ops/tree/main/.buildkite) folder, where we store all buildkite jobs and sometimes define python versions.

!!! warning "Check the default python version in `ops`"

    It is very important to check the `ops` repository for the default project version. This one is used in the servers, and should be one of the supported ones!

## Upgrade your local Python version
1. Remove your virtual environment:
   ```
   rm -rf .venv
   ```
2. Rebuild the environment with the new Python version (replace xx.x with the desired version):
   ```
   PYTHON_VERSION=3.xx.x make .venv
   ```

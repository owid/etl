---
tags:
    - 👷 Staff
---
We have created a python library to enable easy access to our large data catalog. It also assists our work in ETL, as it contains various methods and objects essential to the data wrangling procceses.


Currently, this library lives in the `etl` repository ([find it here](https://github.com/owid/etl/blob/master/lib/catalog)).

### Installation
Simply install it from PyPI:

```shell
pip install owid-catalog
```

### Update release
After working on your changes in the library, publishing to PyPI is automated:

1. **Bump the version** in [`lib/catalog/pyproject.toml`](https://github.com/owid/etl/blob/master/lib/catalog/pyproject.toml)
2. **Update the changelog** in [`lib/catalog/README.md`](https://github.com/owid/etl/blob/master/lib/catalog/README.md?plain=1#L215)
3. **Commit and push to `master`** - the package will be automatically published to PyPI via [GitHub Actions](https://github.com/owid/etl/actions/workflows/publish-owid-catalog.yml)

The workflow triggers automatically when `lib/catalog/pyproject.toml` changes on the master branch. It includes a safety check to ensure the version was actually bumped before publishing.

**Manual trigger:** You can still manually trigger the workflow by clicking `Run Workflow` in [GitHub Actions](https://github.com/owid/etl/actions/workflows/publish-owid-catalog.yml) if needed.

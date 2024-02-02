We have created a python library to enable easy access to our large data catalog. It also assists our work in ETL, as it contains various methods and objects essential to the data wrangling procceses.


Currently, this library lives in the `etl` repository ([find it here](https://github.com/owid/etl/blob/master/lib/catalog)).

### Installation
Simply install it from PyPI:

```shell
pip install owid-catalog
```

### Update release
After working on your changes in the library, you need to follow these steps to update the release (manual work):


- Bump [its version](https://github.com/owid/etl/blob/master/lib/catalog/pyproject.toml).
- Update the [changelog](https://github.com/owid/etl/blob/master/lib/catalog/README.md?plain=1#L215).
- Run the workflow: click on `Run Workflow` in [Github Action](https://github.com/owid/etl/actions/workflows/publish-owid-catalog.yml).

# Processing log
!!! warning "In progress."

    The processing log is an experimental feature that is not yet fully tested.


This log captures every pandas operation in its metadata, allowing users to track the processing history of a dataset. It is particularly useful for visualizing the data pipeline and aiding in debugging processes.

To enable the processing log, set the environment variable `PROCESSING_LOG` to `1`. For example:

```
PROCESSING_LOG=1 etl meadow/dummy/2020-01-01/dummy --force
```

To visualize the processing log in a browser, use the following code (from notebook):

```python
ds = Dataset(DATA_DIR / "meadow/dummy/2020-01-01/dummy")
tab = ds['dummy']
tab['dummy_variable'].metadata.processing_log.display()
```

Ensure that the `PROCESSING_LOG` environment variable is unset or set to "0" when displaying the log. Otherwise, the diagram will only show a single "load" operation. Therefore, it is not advisable to set this variable in the `.env` file.

## Custom processing log entry

Sometimes you have a function that is so complex that its visualisation doesn't look good. You can wrap the function with the decorator `@pl.wrap` to squeeze the function into a single log entry. For example:

```python

from owid.catalog import processing_log as pl

@pl.wrap("complex_processing")
def func(...) -> Table:
    ...
    return tab
```

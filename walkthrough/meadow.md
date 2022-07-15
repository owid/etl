# Walkthrough - Meadow

Here's a summary of this walkthrough, you don't have to manually execute anything, all of it will be done automatically after submitting a form below


1. **Create a new `meadow` step file** (e.g. `etl/etl/steps/data/meadow/example_institution/YYYY-MM-DD/example_dataset.py`). The step must contain a `run(dest_dir)` function that loads data from the `walden` bucket in S3 and creates a dataset (a `catalog.Dataset` object) with one or more tables (`catalog.Table` objects) containing the raw data.

    Keep in mind that both the dataset and its table(s) should contain metadata. Additionally, all of the column names must be snake case before uploading to `meadow`. There is a function in the `owid.catalog.utils` module that will do this for you: `tb = underscore_table(Table(full_df))`.

2. **Add the new meadow step to the dag**, including its dependencies.

3. **Run `make test` in `etl`** and  ensure the step runs well. To run the step: `etl data://meadow/example_institution/YYYY-MM-DD/example_dataset`

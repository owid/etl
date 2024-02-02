Here's a summary, you don't have to manually execute anything, all of it will be done automatically after submitting a form below


1. **Create a new `meadow` step file** (e.g. `etl/etl/steps/data/meadow/example_institution/YYYY-MM-DD/example_dataset.py`). The step must contain a `run(dest_dir)` function that loads snapshot and creates a dataset (a `catalog.Dataset` object) with one or more tables (`catalog.Table` objects) containing the raw data.

    Keep in mind that both the dataset and its table(s) should contain metadata. Additionally, all of the column names must be snake case before uploading to `meadow`. There is a function in the `owid.catalog.utils` module that will do this for you: `tb = underscore_table(Table(full_df))`.

2. **Add the new meadow step to the dag**, including its dependencies.

3. **Run `make test` in `etl`** and  ensure the step runs well. To run the step: `etl data://meadow/example_institution/YYYY-MM-DD/example_dataset`

`meadow` is for _syntactic harmonisation_, meaning getting something into the right format. The idea is that instead of sitting on the dataset that you're working on for a long time without sharing it, you could spend a few hours and get it into a form where others can already reuse it. Out of this you get more, smaller pull-requests. It's also important to have this form for debugging, so that we can compare changes we make later to the "original data".

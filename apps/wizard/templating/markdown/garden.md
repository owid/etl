Here's a summary of this wizard, you don't have to manually execute anything, all of it will be done automatically after submitting a form below

1. **Create a new `garden` step** (e.g. `etl/etl/steps/data/garden/example_institution/YYYY-MM-DD/example_dataset.py`). The step must contain a `run(dest_dir)` function that loads data from the last `meadow` step, processes the data and creates a dataset with one or more tables and the necessary metadata.

    Country names must be harmonized (for which the `harmonize` tool of `etl` can be used).

    Add plenty of assertions and sanity checks to the step (if possible, compare the data with its previous version and check for abrupt changes).

2. **Add the new garden step to the dag**, including its dependencies.

3. **Run `make test` in `etl`** and  ensure the step runs well.

`garden` is for _semantic harmonisation_, meaning the field names and field values are harmonised whenever possible. So all datasets in garden should play well together, you should be able to join across datasets easily and without accidents. At the same time, this is the place where we want to add metadata standards.

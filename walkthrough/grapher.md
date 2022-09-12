# Walkthrough - Grapher

**Grapher step can be only executed by OWID staff**

Here's a summary of this walkthrough, you don't have to manually execute anything, all of it will be done automatically after submitting a form below

1. **Create a new grapher step** (e.g. `etl/etl/steps/grapher/example_institution/YYYY-MM-DD/example_dataset.py`). The step must contain a `get_grapher_dataset()` function and a `get_grapher_tables()` function. To test the step, you can run it on the grapher `staging` database, or using
[a local grapher](https://github.com/owid/owid-grapher/blob/master/docs/docker-compose-mysql.md).

Note that pushing to a grapher DB is **not yet automated** and must be done manually.

## Grapher step

_TODO: rewrite to reflect the new grapher step_

A step to load a dataset into the grapher mysql database. Similar to `data` steps the path is interpreted as the path to a python script. The job of this script is to make the input dataset fit the constrained grapher data model where we only have the exact dimensions of year and entity id. The latter is the numeric id of the entity (usually the country) and the former can also be the number of days since a reference date.

The python script that does this re-fitting of the data for the grapher datamodel only has to reformat the data and ensure the metadata exists. Actually interfacing with the database is taken care of by the ETL library.

The script you supply has to have two functions, the first of which is `get_grapher_dataset` which should return a `owid.catalog.Dataset` instance. The `.metadata.namespace` field will be used to decide what namespace to upsert this data to (or whether to create this namespace if it does not exist) and the `.metadata.short_name` field will be used to decide what dataset to upsert to (again, this will be created if no dataset with this name in this namespace exists, otherwise the existing dataset will be upserted to). `.metadata.sources` will be used to upsert the entries for the sources table.

The other required function is `get_grapher_tables`. It is passed the Dataset instance created by the `get_grapher_dataset` function and should return an iterable of `owid.catalog.Table` instances that should be upserted as an entry in the variables table and then the data points in the data_values table. Every table that is yielded by this function has to adhere to the limited grapher data schema. This means that if you have e.g. a variable with dimensions `year`, `country` and `sex` with the latter having values [male, female, both] then to fit this into grapher we have to created 3 separate variables, one for every sex value. I.e. that this function would have to yield 3 Table instances with year, entityId and value.

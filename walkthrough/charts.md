# Walkthrough - Charts

## Charts step

**Charts step can be only executed by OWID staff**

After the new dataset has correctly been upserted into the database, this step aims at helping update the affected charts.

- It creates chart revisions for all the public charts that use variables from the _old dataset_. These charts should now start using variables from the _new dataset_ instead. You will be asked to map variables from the _old dataset_ to ones from the _new dataset_.
- Once the chart revisions are created, these are submitted to the database and will be available on the _Approval tool_.

Note that this step is equivalent to running `etl-match-variables` and `etl-chart-suggester` commands in terminal. Call them in terminal with option `--help` for more details.

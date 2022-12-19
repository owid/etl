# Walkthrough - Charts

## Charts step

**Charts step can be only executed by OWID staff**

Charts step is a step after the new dataset has correctly been upserted into the database.

- It creates chart revisions for all those charts that use variables from the _old dataset_. The idea, is that these charts should now start using variables from the _new dataset_. You will be asked to map variables from the _old dataset_ to ones from the _new dataset_.
- Once these revisions are created, they are submitted to the database and will be available on the _Aproval tool_.

Note that this step is equivalent to running `etl-match-variables` and `etl-chart-suggester` commands in terminal.

After the new dataset has been correctly upserted into the database, we need to update the affected charts. This step helps with that. These are the steps (this is all automated):

- The user is asked to choose the _old dataset_ and the _new dataset_.
- The user has to establish a mapping between variables in the _old dataset_ and in the _new dataset_. This mapping tells Grapher how to "replace" old variables with new ones.
- The tool creates chart revisions for all the public charts using variables in the _old dataset_ that have been mapped to variables in the _new dataset_.
- Once the chart revisions are created, you can review these and submit them to the database so that they become available on the _Approval tool_.

Note that this step is equivalent to running `etl-match-variables` and `etl-chart-suggester` commands in terminal. Call them in terminal with option `--help` for more details.

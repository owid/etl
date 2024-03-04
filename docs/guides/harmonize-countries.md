We provide a powerful tool to help standardise the country names of any datasets. Harmonizing these names is crucial to our work, so that our visualisation tools can correctly display the data.

Harmonizing country names is done after the Meadow step and before the Garden step. The output of this process is a JSON dictionary, mapping the source's country names to our standard names. For instance:

```json
// some_step.countries.json
{
    "United States of America": "United States",
    "Congo, Democratic Republic of": "Democratic Republic of Congo",
    "DPRK": "North Korea"
}
```

Our list with the standard names can be found [here](https://github.com/owid/etl/blob/master/etl/steps/data/garden/regions/2023-01-01/regions.yml) (look for `name` property).


The harmonization can be performed with our [CLI](../etl-cli) (i.e. `etl harmonize`) or using [the Wizard](../wizard) (i.e. `etlwiz`).


Our harmonization tool relies on [rapidfuzz](https://github.com/rapidfuzz/RapidFuzz).


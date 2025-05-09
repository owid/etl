We use URIs throughout all the ETL to identify files and datasets. The format of a URI varies depending on the step we are dealing with, but in general they follow the following convention:

```
<prefix>://<path>
```

!!! info "[Learn more about all the ETL steps](../../workflow/)."

## Prefix

Most of the time, the prefix will either be `snapshot` or `data`. The former is used for snapshots of upstream dat files, and the latter for ETL datasets (with different levels of curation).

| Prefix     | Description                                                                                                                                                                                                                              |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `snapshot` | Used for [`snapshot`](../../workflow/#snapshot) steps.                                                                                                                                                                                   |
| `data`     | Used for [`meadow`](../../workflow/#meadow), [`garden`](../../workflow/#garden), [`grapher`](../../workflow/#grapher) and most of the ETL steps where we operate with curated [Datasets](../common-format/#datasets-owidcatalogdataset). |
| `backport` | Used to import datasets from the OWID database that are not present in the ETL.                                                                                                                                                          |

## Path

The format of the path is different depending on the prefix.

### Path for `snapshot://`

```
snapshot://<namespace>/<version>/<filename>.<extension>
```

where

| Prefix      | Description                                                                                                                          |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `namespace` | Used to group files from similar topics or sources. Namespace are typically source names (e.g. `un`) or topic names (e.g. `health`). |
| `version`   | Version of the file. Typically, we use the date the file was downloaded in the format `YYYY-mm-dd`.                                  |
| `filename`  | Name of the downloaded file.                                                                                                         |
| `extension` | Extension of the file.                                                                                                               |

!!! example

    ```
    snapshot://ember/2023-02-20/yearly_electricity.csv
    ```

### Path for `data://`

```
data://<channel>/<namespace>/<version>/<dataset-name>
```

where

| Prefix         | Description                                                                                                                                                                                                                        |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `channel`      | Denotes the curation level of the dataset. Possible values include [`meadow`](../../workflow#meadow), [`garden`](../../workflow#garden), [`grapher`](../../workflow#grapher), [`explorers`](../../workflow/other-steps#explorers). |
| `namespace`    | Used to group datasets from similar topics or sources. Namespace are typically source names (e.g. `un`) or topic names (e.g. `health`).                                                                                            |
| `version`      | Version of the file. Typically, we use the date the file was downloaded in the format `YYYY-mm-dd`.                                                                                                                                |
| `dataset-name` | Short name of the curated dataset (e.g. `un_wpp`).                                                                                                                                                                                 |

!!! example "Examples"

    - **Meadow**: `data://meadow/nasa/2023-03-06/ozone_hole_area`
    - **Garden**: `data://garden/nasa/2023-03-06/ozone_hole_area`
    - **Grapher**: `data://grapher/nasa/2023-03-06/ozone_hole_area`
    - **Explorers**: `data://explorers/faostat/2023-02-22/food_explorer`

### Path for `export://`
Export steps are defined in `etl/steps/export` directory and have similar structure to regular steps. Their URI begins with the prefix `export://` and use the following format:

```
export://<channel>/<namespace>/<version>/<filename>
```

where channel is typically one of the following:

- `multidim`: For multidimensional indicators.
- `explorers`: For explorers.
- `github`: For exports to GitHub.

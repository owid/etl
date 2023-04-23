We use URIs throughout all the ETL to identify files and datasets. The format of a URI varies depending on the step we are dealing with, but in general they follow the following convention:

```
<prefix>://<path>
```

!!! info "[Learn more about all the ETL steps](../../workflow/)."


## Prefix
Most of the time, the prefix will either be `snapshot` or `data`. The former is used for snapshots of upstream dat files, and the latter for ETL datasets (with different levels of curation).

| Prefix      | Description                          |
| ----------- | ------------------------------------ |
| `snapshot`       | Used for [`snapshot`](../workflow/snapshot.md) steps. |
| `data`       | Used for [`meadow`](../workflow/meadow.md), [`garden`](../workflow/garden.md), [`grapher`](../workflow/grapher.md) and most of the ETL steps where we operate with curated [Datasets](../common-format/#datasets-owidcatalogdataset).|
| `walden`    | :warning: Deprecated. Used before the introduction of `snapshot`. |
| `backport`    | Used to import datasets from the OWID database that are not present in the ETL. |

## Path
The format of the path is different depending on the prefix.

### Path for `snapshot://`

```
snapshot://<namespace>/<version>/<filename>.<extension>
```

where

| Prefix      | Description                          |
| ----------- | ------------------------------------ |
| `namespace`       | Used to group files from similar topics or sources. Namespace are typically source names (e.g. `un`) or topic names (e.g. `health`).|
| `version`    | Version of the file. Typically, we use the date the file was downloaded in the format `YYYY-mm-dd`. |
| `filename`    | Name of the downloaded file. |
| `extension`    | Extension of the file. |


!!! example
    ```
    snapshot://ember/2023-02-20/yearly_electricity.csv
    ```
### Path for `data://`

```
data://<channel>/<namespace>/<version>/<dataset-name>
```

where

| Prefix      | Description                          |
| ----------- | ------------------------------------ |
| `channel`       | Denotes the curation level of the dataset. Possible values include [`meadow`](../workflow/meadow.md), [`garden`](../workflow/garden.md), [`grapher`](../workflow/grapher.md), [`explorers`](../workflow/other-steps/explorers.md). |
| `namespace`       | Used to group datasets from similar topics or sources. Namespace are typically source names (e.g. `un`) or topic names (e.g. `health`).|
| `version`    | Version of the file. Typically, we use the date the file was downloaded in the format `YYYY-mm-dd`. |
| `dataset-name`    | Short name of the curated dataset (e.g. `un_wpp`). |

!!! example "Examples"
    - **Meadow**: `data://meadow/nasa/2023-03-06/ozone_hole_area`
    - **Garden**: `data://garden/nasa/2023-03-06/ozone_hole_area`
    - **Grapher**: `data://grapher/nasa/2023-03-06/ozone_hole_area`
    - **Explorers**: `data://explorers/faostat/2023-02-22/food_explorer`

### Path for `walden://`

!!! warning "`walden` steps are no longer used. Use `snapshot` instead."


```
walden://<namespace>/<version>/<dataset-name>
```

where

| Prefix      | Description                          |
| ----------- | ------------------------------------ |
| `namespace`       | Used to group files from similar topics or sources. Namespace are typically source names (e.g. `un`) or topic names (e.g. `health`).|
| `version`    | Version of the file. Typically, we use the date the file was downloaded in the format `YYYY-mm-dd`. |
| `dataset-name`    | Short name of the curated dataset (e.g. `un_wpp`). |

!!! example
    ```
    walden://irena/2022-10-07/renewable_electricity_capacity_and_generation
    ```

### Path for `backport://`

```
backport://backport/owid/latest/<dataset-name>
```

where

| Prefix      | Description                          |
| ----------- | ------------------------------------ |
| `dataset-name`    | Name of the dataset. It follows the format `dataset_<dataset-id>_<dataset-name>`, where `dataset-id` corresponds to the dataset Grapher id and `dataset-name` is the name of the dataset in Grapher (with lower case, and all symbols replaced by underscores). |

!!! example
    ```
    backport://backport/owid/latest/dataset_5676_global_health_observatory__world_health_organization__2022_08
    ```
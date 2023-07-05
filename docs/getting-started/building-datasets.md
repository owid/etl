!!! info "You will learn more about the structure and design of the ETL in the next [section](../../architecture/)."

Every step in the dag has a URI. URIs allow us to uniquely identify any step (or node) throughout the whole ETL. This allows us to reference datasets (and use them) when building a new dataset.

For example, the Human Development Reports (by the UNDP):

*[URI]: Uniform Resource Identifier
*[UNDP]: United Nations Development Programme

```
data://garden/un/2022-11-29/undp_hdr
```

!!! info "See also"

    [How are URIs built? :octicons-arrow-right-24:](../architecture/design/uri.md)

## Dry-runs

See what steps would be executed to build the `undp_hdr` dataset by running:

```bash
$ poetry run etl --dry-run data://garden/un/2022-11-29/undp_hdr
Detecting which steps need rebuilding...
OK (0s)
Running 4 steps:
1. snapshot://un/2022-11-29/undp_hdr.csv...
2. snapshot://un/2022-11-29/undp_hdr.xlsx...
3. data://meadow/un/2022-11-29/undp_hdr...
4. data://garden/un/2022-11-29/undp_hdr...
```

The first two listed steps are `snapshot://` steps, which when executed will download upstream snapshots of the dataset to the `data/snapshots/` folder. The last two steps are `data://` steps, which will generate local datasets in the `data/` folder.

??? note "`meadow` and `garden` channels"

    In the above example, you can indetify different channels in the URIs: `meadow` and `garden`, followed by the same string `un/2022-11-29/undp_hdr`. These represent different levels
    of curation of a dataset (in this example, the UNDP HDR version 2022-11-29 dataset).

    `garden` datasets are good to go, whereas `meadow` datasets have not been curated enough to be used in production environments. We will explore these nuances later on.

Note that you can skip the full path of the step, in which case it will do a regex match against all available steps:

```bash
$ poetry run etl --dry-run undp_hdr
Detecting which steps need rebuilding...
OK (0s)
Running 4 steps:
1. snapshot://un/2022-11-29/undp_hdr.csv...
2. snapshot://un/2022-11-29/undp_hdr.xlsx...
3. data://meadow/un/2022-11-29/undp_hdr...
4. data://garden/un/2022-11-29/undp_hdr...
5. data://grapher/un/2022-11-29/undp_hdr...
```

Note that here there is an extra dataset listed, with prefix `data://grapher/`, as it matches the query (its URI contains the query text "undp_hdr").

## Generate the dataset
Now let's build the dataset, by removing the `--dry-run` option:

```bash
$ poetry run etl data://garden/un/2022-11-29/undp_hdr
Detecting which steps need rebuilding...
OK (0s)
Running 4 steps:
1. snapshot://un/2022-11-29/undp_hdr.csv...
OK (5s)

2. snapshot://un/2022-11-29/undp_hdr.xlsx...
OK (5s)

3. data://meadow/un/2022-11-29/undp_hdr...
2023-04-19 22:28.41 [info     ] undp_hdr.start
2023-04-19 22:28.44 [info     ] undp_hdr.end
OK (5s)

4. data://garden/un/2022-11-29/undp_hdr...
2023-04-19 22:28.46 [info     ] undp_hdr.start
2023-04-19 22:28.47 [info     ] undp_hdr.harmonize_countries
2023-04-19 22:28.47 [info     ] undp_hdr.format_df
2023-04-19 22:28.47 [info     ] undp_hdr.dtypes
2023-04-19 22:28.47 [info     ] undp_hdr.sanity_check
2023-04-19 22:28.47 [info     ] undp_hdr.creating_table
2023-04-19 22:28.47 [info     ] undp_hdr.end
OK (3s)
```

Let's confirm that the dataset was built locally:

```bash
$ ls data/garden/un/2022-11-29/undp_hdr/
undp_hdr.feather
undp_hdr.meta.json
undp_hdr.parquet
index.json
```

Several files got built for the dataset: `index.json` gives metadata about the whole dataset, and the remaining three files all represent a single data table, which is saved in both Feather and Parquet formats.

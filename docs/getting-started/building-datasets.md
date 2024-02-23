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
$ poetry run etl run --dry-run data://garden/un/2022-11-29/undp_hdr
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
$ poetry run etl run --dry-run undp_hdr
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
$ poetry run etl run data://garden/un/2022-11-29/undp_hdr
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

??? note "Parallel execution"

    There's a flag `etl ... --workers 4` you can use to run the ETL in parallel. This is useful when rebuilding large part of ETL (e.g. after updating regions).

## Reading a dataset
Now that our `data/` folder has a table built, we can try reading it. We recommend using our library `owid-catalog`.

You can load the complete dataset using the `owid.catalog.Dataset` object:


```pycon
>>> from owid.catalog import Dataset
>>> ds = Dataset('data/garden/un/2022-11-29/undp_hdr')
```

We can access the metadata of the dataset by using `Dataset.metadata`:

```pycon
>>> ds.metadata
Dataset(path='data/garden/un/2022-11-29/undp_hdr/', metadata=DatasetMeta(namespace='un', short_name='undp_hdr', title='Human Development Report - UNDP (2021-22)', description="The 2021/2022 Human Development Report is the latest in the series of global Human Development Reports published by the United Nations Development Programme (UNDP) since 1990 as independent and analytically and empirically grounded discussions of major development issues, trends and policies.\n\nAdditional resources related to the 2021/2022 Human Development Report can be found online at http://hdr.undp.org. Resources on the website include digital versions and translations of the Report and the overview in more than 10 languages, an interactive web version of the Report, a set of background papers and think pieces commissioned for the Report, interactive data visualizations and databases of human development indicators, full explanations of the sources and methodologies used in the Report's composite indices, country insights and other background materials, and previous global, regional and national Human Development Reports. Corrections and addenda are also available online.\n\nTechnical notes may be found at https://hdr.undp.org/sites/default/files/2021-22_HDR/hdr2021-22_technical_notes.pdf.\n", sources=[Source(name='UNDP, Human Development Report (2021-22)', description='The 2021/2022 Human Development Report is the latest in the series of global Human Development Reports published by the United Nations Development Programme (UNDP) since 1990 as independent and analytically and empirically grounded discussions of major development issues, trends and policies.\n\nAdditional resources related to the 2021/2022 Human Development Report can be found online at http://hdr.undp.org. Resources on the website include digital versions and translations of the Report and the overview in more than 10 languages, an interactive web version of the Report, a set of background papers and think pieces commissioned for the Report, interactive data visualizations and databases of human development indicators, full explanations of the sources and methodologies used in the Report’s composite indices, country insights and other background materials, and previous global, regional and national Human Development Reports. Corrections and addenda are also available online.\n\nTechnical notes (region definitions, reports, etc.) can be found at https://hdr.undp.org/sites/default/files/2021-22_HDR/hdr2021-22_technical_notes.pdf.\n', url='https://hdr.undp.org/', source_data_url='https://hdr.undp.org/sites/default/files/2021-22_HDR/HDR21-22_Composite_indices_complete_time_series.csv', owid_data_url=None, date_accessed='2022-11-29', publication_date='2022-09-08', publication_year=2022, published_by=None)], licenses=[License(name='CC BY 3.0 IGO', url='https://hdr.undp.org/copyright-and-terms-use')], is_public=True, additional_info=None, version='2022-11-29', source_checksum='b806f3297dfa67e996487b1c3602c94f'))
```

To load the data as a table run:

```pycon
>>> tb = ds["undp_hdr"]
>>> tb.head()
                         abr  co2_prod  coef_ineq  diff_hdi_phdi  ...  pr_m  rankdiff_hdi_phdi      se_f      se_m
country     year                                                  ...
Afghanistan 1990  142.960007  0.209727        NaN       1.098901  ...   NaN               <NA>  0.700485  5.419458
            1991  147.524994  0.182525        NaN       1.075269  ...   NaN               <NA>  0.772361  5.583395
            1992  147.520996  0.095233        NaN       1.045296  ...   NaN               <NA>  0.844236  5.747332
            1993  147.895996  0.084285        NaN       1.010101  ...   NaN               <NA>  0.916112  5.911269
            1994  155.669006  0.075054        NaN       1.027397  ...   NaN               <NA>  0.987988  6.075205

[5 rows x 39 columns]
```

We can see that this dataset provides several indicators, reported by country and year, which are the primary key for this table.


The object `tb` is an instance of `owid.catalog.Table`, which is a wrapper around the well stablished `pandas.DataFrame` class. Using these custom
objects (from our library `owid-catalog`) allow us to enrich the data with metadata.

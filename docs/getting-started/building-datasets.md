!!! info "You will learn more about the structure and design of the ETL in the next [section](../../architecture/)."

The ETL is the way we ingest, process and publish data at Our World in Data (OWID). The ETL contains a set of recipes (or steps) to build datasets, which are then made available from [OWID's catalog](../api/). A step is a python script and has a URI. URIs allow us to uniquely identify any step (or node) throughout the whole ETL. This allows us to reference datasets (and use them) when building a new one. For example, the Cherry Blossom dataset (by Yasuyuki Aono):

*[URI]: Uniform Resource Identifier

```
data://garden/biodiversity/2024-01-25/cherry_blossom
```

!!! info "See also"

    [How are URIs built? :octicons-arrow-right-24:](../architecture/design/uri.md)

## Build a dataset
### Preview dataset build
You can build any dataset in ETL using our [ETL cli]((../../guides/etl-cli)): `etl run`. This will execute all the steps required to build a dataset.

Before actually building the dataset, it is recommended to preview the steps that would be executed to build it by using the `--dry-run` flag.

Let's preview the build for Cherry Blossom dataset:

```
$ etl run --dry-run data://garden/biodiversity/2024-01-25/cherry_blossom
--- Detecting which steps need rebuilding...
OK (0.0s)
--- Running 3 steps:
1. snapshot://biodiversity/2024-01-25/cherry_blossom.xls
2. data://meadow/biodiversity/2024-01-25/cherry_blossom
3. data://garden/biodiversity/2024-01-25/cherry_blossom
```

The first step is a `snapshot://` steps, which when executed will download upstream snapshot of the dataset to the `data/snapshots/` folder. The remaining steps are `data://` steps, which will generate local datasets in the `data/` folder.

!!! note "`meadow` and `garden` channels"

    In the above example, you can indetify two different channels in the URIs: `meadow` and `garden`, followed by the same string `biodiversity/2024-01-25/cherry_blossom`. These represent different levels of curation of a dataset (in this example, for the Cherry Blossom dataset version 2024-01-25 dataset).

    `garden` datasets are user-ready, whereas `meadow` datasets have not been curated enough to be used in production environments. We will explore these nuances later on.

    [Learn more about the ETL steps :octicons-arrow-right-24:](../../architecture/workflow)

<!-- `grapher` datasets are very similar to `garden` ones, but with some adjustments to make them suitable for our Database, which powers all our site.  -->
Note that you can skip the full path of the step, in which case it will do a regex match against all available steps:

```
$ etl run --dry-run cherry_blossom
--- Detecting which steps need rebuilding...
OK (0.0s)
--- Running 4 steps:
1. snapshot://biodiversity/2024-01-25/cherry_blossom.xls
2. data://meadow/biodiversity/2024-01-25/cherry_blossom
3. data://garden/biodiversity/2024-01-25/cherry_blossom
4. data://grapher/biodiversity/2024-01-25/cherry_blossom
```

Note that here there is an extra dataset listed, with prefix `data://grapher/`, as it matches the query (its URI contains the query text "cherry_blossom").

!!! note "`grapher` channel"

    The additional step listed with prefix `data://grapher` refers to yet another dataset curation level. It is very similar to `garden` datasets, but slightly adapted for our database requirements.

    [Learn more about the ETL steps :octicons-arrow-right-24:](../../architecture/workflow)


### Build the dataset
You can build a dataset by using the `etl run` command. This uses our [ETL CLI](../../guides/etl-cli) tool.

```
$ etl run cherry_blossom
--- Detecting which steps need rebuilding...
OK (0.0s)
--- Running 4 steps:
--- 1. snapshot://biodiversity/2024-01-25/cherry_blossom.xls...
DOWNLOADED          https://snapshots.owid.io/fc/459738bd4a0a73c716755d598c6678 -> /home/lucas/repos/etl/data/snapshots/biodiversity/2024-01-25/cherry_blossom.xls
OK (0.8s)

--- 2. data://meadow/biodiversity/2024-01-25/cherry_blossom...
2024-02-26 13:55:17 [info     ] cherry_blossom.start
2024-02-26 13:55:19 [info     ] cherry_blossom.end
OK (3.7s)

--- 3. data://garden/biodiversity/2024-01-25/cherry_blossom...
2024-02-26 13:55:21 [info     ] cherry_blossom.start
2024-02-26 13:55:22 [info     ] cherry_blossom.end
OK (3.7s)

--- 4. data://grapher/biodiversity/2024-01-25/cherry_blossom...
OK (3.6s)
```

Let's confirm that the dataset was built locally:

```
$ ls -l data/garden/biodiversity/2024-01-25/cherry_blossom
cherry_blossom.feather
cherry_blossom.meta.json
index.json
```

Several files were built for the dataset: `index.json` gives metadata about the whole dataset, and the remaining three files all represent a single data table, which is saved in both Feather and Parquet formats.

??? note "Parallel execution"

    There's a flag `etl ... --workers 4` you can use to run the ETL in parallel. This is useful when rebuilding large part of ETL (e.g. after updating regions).

### Add the dataset to Grapher
Datasets are created and stored in your local environment `data/`. For the previous example, we created a grapher dataset, which is saved in `data/garden/biodiversity/2024-01-25/cherry_blossom/` directory. Now, if we want to create charts with it, we need to push it to the Grapher database. This can be achieved by repeating the previous command with the `--grapher` flag:

```
$ etl run cherry_blossom --grapher
--- Detecting which steps need rebuilding...
OK (0.9s)
--- Running 2 steps:
--- 1. grapher://grapher/biodiversity/2024-01-25/cherry_blossom...
2024-02-26 16:03:38 [info     ] upsert_dataset.verify_namespace namespace=biodiversity
2024-02-26 16:03:38 [info     ] upsert_dataset.upsert_dataset.start short_name=cherry_blossom
2024-02-26 16:03:38 [info     ] upsert_dataset.upsert_dataset.end id=6350 short_name=cherry_blossom
2024-02-26 16:03:41 [info     ] upsert_table.uploaded_to_s3    size=835 variable_id=819375
2024-02-26 16:03:41 [info     ] upsert_table.uploaded_to_s3    size=831 variable_id=819376
OK (4.5s)

--- 2. grapher://grapher/biodiversity/2023-01-11/cherry_blossom...
2024-02-26 16:03:42 [info     ] upsert_dataset.verify_namespace namespace=biodiversity
2024-02-26 16:03:43 [info     ] upsert_dataset.upsert_dataset.start short_name=cherry_blossom
2024-02-26 16:03:43 [info     ] upsert_dataset.upsert_dataset.end id=5864 short_name=cherry_blossom
2024-02-26 16:03:45 [info     ] upsert_table.uploaded_to_s3    size=833 variable_id=540251
2024-02-26 16:03:45 [info     ] upsert_table.uploaded_to_s3    size=1087 variable_id=540252
OK (3.9s)
```

Note that in this example, also the Cherry Blossom 2023-01-11 dataset was pushed to Grapher (it matches the query "cherry_blossom"). The dataset is now available in the Grapher database, and can be used to create charts.


### Run ETL in a specific environment
ETL steps are executed locally, but when pushing them to the database we can choose which database to use. This can be controlled by environment variables, which are set in an `.env`-like file. For instance, we provide [an example file](https://github.com/owid/etl/blob/master/.env.example), which should be adapted to point to the desired environment. Then, you can run any step like:

```
ENV_FILE=.env.example etl run cherry_blossom --grapher
```


## Reading a dataset

Now that our `data/` folder has a table built, we can try reading it. We recommend using our library `owid-catalog`. You can load the complete dataset using the `owid.catalog.Dataset` object:

```pycon
>>> from owid.catalog import Dataset
>>> ds = Dataset('data/garden/biodiversity/2024-01-25/cherry_blossom')
```

We can access the metadata of the dataset by using `Dataset.metadata`:

```pycon
>>> ds.metadata
DatasetMeta(channel='garden', namespace='biodiversity', short_name='cherry_blossom', title='Cherry Blossom Full Bloom Dates in Kyoto, Japan', description='A historical time series of peak cherry blossom bloom data from Kyoto, Japan. The timing of peak cherry blossom is linked to weather and climate factors. Warmer weather, due to climate change, is pulling peak cherry blossom earlier in the year.\n\nYasuyuki Aono compiled this data based on multiple sources:\n\n      - Taguchi,T. (1939) Climatic change in historical time in Japan J. Mar. Meteorol. Soc. 19 217–27\n\n      - Sekiguchi,T. (1969) The historical dates of Japanese cherry festival since the 8th century and her climatic changes Tokyo Geog. Pap. 13 175–90\n\n      - Aono,Y. and Omoto,Y. (1994) Estimation of temperature at Kyoto since 11th century using flowering data of cherry tree in old documents J. Agric. Meteorol. 49 263–72\n\n      - Aono,Y. and Kazui,K. (2008) Phenological data series of cherry tree flowering in Kyoto, Japan, and its application to reconstruction of springtime temperatures since the 9th century Int. J. Climatol. 28 905–14\n\n      - Aono,Y. and Saito,S. (2010) Clarifying springtime temperature reconstructions of the medieval period by gap-filling the cherry blossom phenological data series at Kyoto, Japan Int. J. Biometeorol. 54 211–9\n\n      Data for 812-2021 is available here - http://atmenv.envi.osakafu-u.ac.jp/aono/kyophenotemp4/\n\n      Data for 2022 & 2023 full blossom is gathered from personal communication with Yasuyuki Aono.', sources=[], licenses=[], is_public=True, additional_info=None, version='2024-01-25', update_period_days=365, non_redistributable=False, source_checksum='ad4620b0c46185628aef198598870c6c')
```

### Exploring dataset's tables
This dataset may contain some tables:

```pycon
>>> ds.table_names
['cherry_blossom']
```

which you can then explore further:

```pycon
>>> tb = ds["cherry_blossom"]
>>> tb
              full_flowering_date  average_20_years
country year
Japan   812                    92               NaN
        815                   105               NaN
        831                    96               NaN
        851                   108               NaN
        853                   104        101.000000
...                           ...               ...
        2019                   95         95.699997
        2020                   92         95.300003
        2021                   85         94.750000
        2022                   91         94.750000
        2023                   84         94.050003

[835 rows x 2 columns]
```

We can see that this dataset provides two indicators (`full_flowering_date` and `average_20_years`), reported by country and year, which are the primary key for this table. Note that this particular dataset, only contains data for Japan. The object `tb` is an instance of `owid.catalog.Table`, which is a wrapper around the well-established `pandas.DataFrame` class. Using these custom objects (from our library `owid-catalog`) allows us to enrich the data with metadata.

You can explore the indicators

### Exploring dataset's indicators
Now that we have a table, we can explore its indicators further by printing their corresponding metadata

```pycon
>>> tb["full_flowering_date"].metadata
VariableMeta(title='Day of the year with peak cherry blossom', description=None, description_short='The day of the year with the peak cherry blossom of the Prunus jamasakura species of cherry tree in Kyoto, Japan.', description_from_producer=None, description_key=[], origins=[Origin(producer='Yasuyuki Aono', title='Cherry Blossom Full Bloom Dates in Kyoto, Japan', description='A historical time series of peak cherry blossom bloom data from Kyoto, Japan. The timing of peak cherry blossom is linked to weather and climate factors. Warmer weather, due to climate change, is pulling peak cherry blossom earlier in the year.\n\nYasuyuki Aono compiled this data based on multiple sources:\n\n      - Taguchi,T. (1939) Climatic change in historical time in Japan J. Mar. Meteorol. Soc. 19 217–27\n\n      - Sekiguchi,T. (1969) The historical dates of Japanese cherry festival since the 8th century and her climatic changes Tokyo Geog. Pap. 13 175–90\n\n      - Aono,Y. and Omoto,Y. (1994) Estimation of temperature at Kyoto since 11th century using flowering data of cherry tree in old documents J. Agric. Meteorol. 49 263–72\n\n      - Aono,Y. and Kazui,K. (2008) Phenological data series of cherry tree flowering in Kyoto, Japan, and its application to reconstruction of springtime temperatures since the 9th century Int. J. Climatol. 28 905–14\n\n      - Aono,Y. and Saito,S. (2010) Clarifying springtime temperature reconstructions of the medieval period by gap-filling the cherry blossom phenological data series at Kyoto, Japan Int. J. Biometeorol. 54 211–9\n\n      Data for 812-2021 is available here - http://atmenv.envi.osakafu-u.ac.jp/aono/kyophenotemp4/\n\n      Data for 2022 & 2023 full blossom is gathered from personal communication with Yasuyuki Aono.', title_snapshot=None, description_snapshot=None, citation_full='- Aono and Kazui (2008) International Journal of Climatology, 28, 905-914\n- Aono and Saito (2010) International Journal of Biometeorology, 54, 211-219\n- Aono (2012) Chikyu Kankyo (Global Environment), 17, 21-29 (in Japanese with English abstract)', attribution=None, attribution_short='Aono', version_producer=None, url_main='http://atmenv.envi.osakafu-u.ac.jp/aono/kyophenotemp4/', url_download='http://atmenv.envi.osakafu-u.ac.jp/omu-content/uploads/sites/1215/2015/10/KyotoFullFlower7.xls', date_accessed='2024-01-25', date_published='2021-03-06', license=License(name='CC BY 4.0', url='https://creativecommons.org/licenses/by/4.0/')), Origin(producer='Yasuyuki Aono', title='Cherry Blossom Full Bloom Dates in Kyoto, Japan', description='A historical time series of peak cherry blossom bloom data from Kyoto, Japan. The timing of peak cherry blossom is linked to weather and climate factors. Warmer weather, due to climate change, is pulling peak cherry blossom earlier in the year.\n\nYasuyuki Aono compiled this data, most is available from an xls file available [here](http://atmenv.envi.osakafu-u.ac.jp/aono/kyophenotemp4/#:~:text=written%20in%20Japanese).-,Full%2Dflowering%20data%20in%20Excel%20varsion,-March%20mean%20temperature).\n\n      Data for 812-2021 is available here - http://atmenv.envi.osakafu-u.ac.jp/aono/kyophenotemp4/\n\n      Data for 2022 & 2023 full blossom is gathered from personal communication with Yasuyuki Aono.', title_snapshot=None, description_snapshot=None, citation_full='- Aono and Kazui (2008) International Journal of Climatology, 28, 905-914\n- Aono and Saito (2010) International Journal of Biometeorology, 54, 211-219\n- Aono (2012) Chikyu Kankyo (Global Environment), 17, 21-29', attribution=None, attribution_short='Aono', version_producer=None, url_main='http://atmenv.envi.osakafu-u.ac.jp/aono/kyophenotemp4/', url_download=None, date_accessed='2024-01-27', date_published='2024-01-27', license=License(name='CC BY 4.0', url='https://creativecommons.org/licenses/by/4.0/'))], licenses=[], unit='day of the year', short_unit='', display={'numDecimalPlaces': 0}, additional_info=None, processing_level='minor', processing_log=[], presentation=VariablePresentationMeta(grapher_config={'selectedEntityNames': ['Japan'], 'subtitle': 'The vertical axis shows the date of peak blossom, expressed as the number of days since 1st January. The timing of the peak cherry blossom is influenced by spring temperatures. Higher temperatures due to climate change have caused the peak blossom to gradually move earlier in the year since the early 20th century.\n', 'title': 'Day of the year with peak cherry tree blossom in Kyoto, Japan', 'yAxis': {'min': 70}}, title_public=None, title_variant=None, attribution_short=None, attribution=None, topic_tags=['Biodiversity', 'Climate Change'], faqs=[]), description_processing=None, license=None, sources=[])
```

!!! tip

    The metadata of datasets, tables and indicators are better printed in Jupyter Notebooks.

### Using our python API
Note that you can also read datasets using our catalog [python API](../../api/):

```pycon
>>> from owid import catalog
>>> tb = catalog.find_latest("cherry_blossom")
              full_flowering_date  average_20_years
country year
Japan   812                    92               NaN
        815                   105               NaN
        831                    96               NaN
        851                   108               NaN
        853                   104        101.000000
...                           ...               ...
        2019                   95         95.699997
        2020                   92         95.300003
        2021                   85         94.750000
        2022                   91         94.750000
        2023                   84         94.050003

[835 rows x 2 columns]
```

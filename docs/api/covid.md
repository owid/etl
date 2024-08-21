# COVID-19

This page is a compact summary of our COVID-19 work, with all the relevant links to download our COVID-19 datasets.

!!! tip "I just want [the data](#download-data)!"

## Our work

At Our World in Data, we have been collecting COVID-19 data from various domains since the pandemic started. We believe that to make progress against the outbreak of the Coronavirus disease – COVID-19 – we need to understand how the pandemic is developing. And for this, we need reliable and timely data. Therefore have focused our work on bringing together the research and statistics on the COVID-19 outbreak.

### Legacy data work

We started working on COVID-19 data in early 2020 when we developed and implemented several data pipelines to process and publish the data. All this work has been live and shared with the public via our GitHub repository [https://github.com/owid/covid-19-data](https://github.com/owid/covid-19-data), and our [old COVID documentation](https://docs.owid.io/projects/covid/en/latest/). We have complemented our data work with extensive research articles, which have been shared on our [topic page](https://ourworldindata.org/coronavirus).

### Publications

!!! abstract ""

    :material-file-document: Hasell, J., Mathieu, E., Beltekian, D. et al. **A cross-country database of COVID-19 testing**. _Sci Data_ 7, 345 (2020). [https://doi.org/10.1038/s41597-020-00688-8](https://doi.org/10.1038/s41597-020-00688-8)

!!! abstract ""

    :material-file-document: Mathieu, E., Ritchie, H., Ortiz-Ospina, E. et al. **A global database of COVID-19 vaccinations**. _Nat Hum Behav_ 5, 947–953 (2021). [https://doi.org/10.1038/s41562-021-01122-8](https://doi.org/10.1038/s41562-021-01122-8)

!!! abstract ""

    :material-file-document: Herre, B., Rodés-Guirao, L., Mathieu, E. et al. **Best practices for government agencies to publish data: lessons from COVID-19**. _The Lancet Public Health_, Viewpoint, Volume 9, ISSUE 6, e407-e410 (2024). [https://doi.org/10.1016/S2468-2667(24)00073-2](<https://doi.org/10.1016/S2468-2667(24)00073-2>)

### Transition to ETL

All our COVID-19 data work was done before we had developed our [ETL system](../../architecture). In mid-2024, we decided to migrate all our COVID-19 data work into ETL, and make our data available from our catalog.

## Download data

Our _compact COVID-19 dataset_ is a compilation of our most relevant COVID-19 indicators collected in the last years. It consolidates indicators from various datasets into a single file. It comes with metadata, which explains all the indicators in detail. In the past, this dataset was generated and shared in our [GitHub](https://github.com/owid/covid-19-data/blob/master/public/data) repository.

[:material-download: Download our compact dataset (CSV)](https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.csv){ .md-button .md-button--primary }
[:material-download: Download metadata](https://catalog.ourworldindata.org/garden/covid/latest/compact/compact.meta.json){ .md-button }

In addition to our compact dataset, we also provide individual datasets, with all our COVID-19 indicators. These files are direct exports from our ETL.


|                                      | **:material-database: Data**                                                                                                                                | **:material-book: Metadata**                                                                                                                                      |
| ------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Cases and Deaths**                 | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/cases_deaths/cases_deaths.csv)                                        | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/cases_deaths/cases_deaths.meta.json)                                        |
| **Excess Mortality**                 | [:material-download: download](https://catalog.ourworldindata.org/garden/garden/excess_mortality/latest/excess_mortality/excess_mortality.csv)              | [:material-download: download](https://catalog.ourworldindata.org/garden/excess_mortality/latest/excess_mortality/excess_mortality.meta.json)                     |
| **Excess Mortality (The Economist)** | [:material-download: download](https://catalog.ourworldindata.org/garden/excess_mortality/latest/excess_mortality_economist/excess_mortality_economist.csv) | [:material-download: download](https://catalog.ourworldindata.org/garden/excess_mortality/latest/excess_mortality_economist/excess_mortality_economist.meta.json) |
| **Hospitalizations**                 | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/hospital/hospital.csv)                                                | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/hospital/hospital.meta.json)                                                |
| **Vaccinations**                     | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/vaccinations_global/vaccinations_global.csv)                          | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/vaccinations_global/vaccinations_global.meta.json)                          |
| **Vaccinations (US)**                | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/vaccinations_us/vaccinations_us.csv)                                  | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/vaccinations_us/vaccinations_us.meta.json)                                  |
| **Testing**                          | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/testing/testing.csv)                                                  | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/testing/testing.meta.json)                                                  |
| **Reproduction rate**                | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/tracking_r/tracking_r.csv)                                            | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/tracking_r/tracking_r.meta.json)                                            |
| **Google mobility**                  | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/google_mobility/google_mobility.csv)                                  | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/google_mobility/google_mobility.meta.json)                                  |
| **Government response policy**       | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/oxcgrt_policy/oxcgrt_policy.csv)                                      | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/oxcgrt_policy/oxcgrt_policy.meta.json)                                      |
| **Attitudes (YouGov)**               | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/yougov/yougov_composite.csv)                                          | [:material-download: download](https://catalog.ourworldindata.org/garden/covid/latest/yougov/yougov_composite.meta.json)                                          |

All our COVID-19 data pipelines are specified in [our DAG](https://github.com/owid/etl/blob/master/dag/covid.yml).

### Data providers

The data produced by third parties and made available by Our World in Data is subject to the license terms from the original third-party authors. We will always indicate the original source of the data in our database, and you should always check the license of any such third-party data before use.

Learn more on the licensing in the metadata files.

### Understanding our metadata
Our metadata contains all the relevant information about an indicator. This includes licenses, descriptions, units, etc. We use this metadata to bake our charts on our site.

!!! info "Learn more in our [metadata reference](../architecture/metadata/reference/)."

## Acces the data with our catalog

!!! warning "Our catalog library is in alpha."

### Install our catalog package

```bash
pip install owid-catalog
```

### Usage and preview

Our data is identified by URIs, and for COVID data these go like:

```
data://garden/covid/latest/{DATASET_NAME}/{TABLE_NAME}
```

where:

-   `DATASET_NAME` is the name of the dataset (e.g. `case_death`)
-   `TABLE_NAME` is the name of the table (e.g. `case_death`)

[→ Learn more about our URIs](https://docs.owid.io/projects/etl/architecture/design/uri/?h=uri#path-for-data)

**Notes**:

-   A dataset can be a collection of tables (equivalent to DataFrames). For instance, there might be several files (or DataFrames) in our 'Vaccination' dataset (e.g. global data, US data, etc.).
-   Our excess mortality dataset is currently under the namespace `excess_mortality`, i.e. with URIs `data://garden/excess_mortality/latest/{DATASET_NAME}/{TABLE_NAME}`.

#### Check all our COVID data

Simply run:

```python
from owid import catalog

# Preview list of available datasets (each row = dataset)
catalogs.find(namespace="covid")

# You can load any dataset (using the row of the above-returned table)
tb = catalogs.find(namespace="covid").iloc[3].load()
```

### Load data

Use an `uri` from the table below[^1].

[^1]: more items are being added to this table shortly.

| **Data category**                | **URI**                                                                                |
| -------------------------------- | -------------------------------------------------------------------------------------- |
| Cases and deaths                 | `garden/covid/latest/cases_deaths/cases_deaths`                                        |
| Excess Mortality                 | `garden/excess_mortality/latest/excess_mortality/excess_mortality`                     |
| Excess Mortality (The Exonomist) | `garden/excess_mortality/latest/excess_mortality_economist/excess_mortality_economist` |
| Hospitalisations                 | `garden/covid/latest/hospital/hospital/`                                               |
| Google Mobility                  | `garden/covid/latest/google_mobility/google_mobility`                                  |
| Policy Response (OxCGRT)         | `garden/covid/latest/oxcgrt_policy/oxcgrt_policy`                                      |
| Indicator decoupling             | `garden/covid/latest/decoupling/decoupling`                                            |
| YouGov                           | `garden/covid/latest/yougov/yougov`                                                    |
| YouGov (Composite)               | `garden/covid/latest/yougov/yougov_composite`                                          |
| Vaccinations (US)                | `garden/covid/latest/vaccinations_us/vaccinations_us`                                  |
| Testing                          | `garden/covid/latest/testing/testing`                                                  |
| Sequencing / Variants            | `garden/covid/latest/sequence/sequence`                                                |
| Decoupling                       | `garden/covid/latest/decoupling/decoupling`                                            |
| Sweden confirmed deaths          | `garden/covid/latest/sweden_covid/sweden_covid/`                                       |
| UK COVID Data                    | `garden/covid/latest/uk_covid/uk_covid/`                                               |

and run the following code:

```python
from owid import catalog

rc = catalog.RemoteCatalog()
uri = "..."
df = rc[uri]
```

### Access metadata

Note that objects `df` are not pure pandas DataFrames, but rather `owid.catalog.Table` datasets, which behave like DataFrames but also contain metadata. You can access metadata like this:

```python
# Table metadata
df.metadata
# Column (or indicator) metadata
df[column_name].metadata
```

[→ Learn more about our metadata](https://docs.owid.io/projects/etl/architecture/metadata/)

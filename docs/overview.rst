Overview
========

At Our World In Data we ingest, transform and curate data from many different sources. We call this process the Our World in Data ETL (ETL is short for Extract, Transform & Load). The `ETL github repository <https://github.com/owid/etl>`_  is a crucial part of this process. It is set up in a way that allows anyone to rebuild and inspect all steps in our data processing on their own computers so that all of curation choices can be understood, alternatives investigated and problems fixed if there are any.

If you only care about the final curated dataset files for your own analysis then you should head over to `owid-catalog-py <https://github.com/owid/owid-catalog-py>`__. This is our python library that can access the OWID data catalog which is our curated catalog of all the data we have collected. You can think of the OWID data catalog of the result of running the code that is collected in the ETL repository.

If you want to rerun our steps to build a dataset, be it to reproduce what we did or to investigate an issue, then the  `ETL github repository`_ is what you are looking for.

The rest of this description concentrates on our ETL pipeline, the concepts used inside it and how to rerun our steps in case you want to try alternative approaches or investigate inconsistencies.

Data sources
------------

To a first approximation there are two kinds of data sources we ingest in our ETL and that we want to be able to use in our visualisation system called `Grapher <https://github.com/owid/owid-grapher>`__:

.. mermaid::
    :align: center

    graph LR

    upstream[institutional data] --> etl --> catalog[catalog on disk] --> Grapher
    researchers[individual research dataset] --> admin[Grapher web admin] --> Grapher

-  **Institutional data** like that published by the World Bank, the World Health Organisation etc. These are often large, complex and periodically updated data releases. This kind of data is the main focus of this repository for the time being.
-  **Individual research dataset releases**. These are usually small datasets published as part of academic papers. They are often originally only a single CSV or Excel file. Such files have historically been uploaded directly into the MySQL database of Grapher, our visualisation system. While these kinds of data sources will be ingested as part of this repository eventually, as of Summer 2022 they exist in ETL only as backported artifacts.

Terminology and organization
----------------------------

We deal almost exclusively with tabular data. Both our ETL scripts (python scripts or python jupyter notebooks) and the resulting files follow a common hierarchy. Our levels of organization for institutional data matches a structure of nested folders. This means that in the ETL repository you will find a hierarchy of folders in ``/etl/steps`` that follows the scheme below and likewise when you run the ETL steps the output data files will end up in ``/data/`` following this hierarchy. Finally, since the output files are published into our catalog, the hierarchy of directories is the same there as well. The levels of hierarchy are as follows (from highest level to finest level of detail):

.. mermaid::
    :align: center

    graph TD

    channel --> provider --> release --> dataset

- **Channel** - this refers to the top level folder that communicates the level of processing or the special data origin of the data. Important channels are:

  - ``meadow``, ``garden`` and ``grapher`` - these correspond to the logical steps described below
  - ``backport`` - these are datasets that were not originally imported via scripts here in the ETL repository. This includes both individual research paper data relases and older institutional data imports (roughly before 2022).
  - ``explorers`` - a special output channel for data that is used by one of our `Data Explorers <https://ourworldindata.org/charts>`__.
- **Provider** - this is the short form acronym of the institution that is the provider of the data (e.g. ``who`` for World Health Organisation, ``wb`` for World Bank, etc.)
- **Release** - this level matches the release cadence for datasets that are released yearly, quarterly etc. For datasets that are released continuously, the special value ``latest`` is often used
- **Dataset** - this is a logical grouping of data tables that are released together by the providers as a logical unit.

.. mermaid::
    :align: center

    flowchart LR
      subgraph Dataset
        direction TB
        index.json
        subgraph Table1
            direction RL
            table1.meta.json
            table1.parquet
            table1.feather
        end
        subgraph Table2
            direction RL
            table2.meta.json
            table2.parquet
            table2.feather
        end
      end


A dataset for us is then a collection of at least one data table and some metadata. Each table exists in the catalog as three files - one json file for the metadata; and then one `IPC feather <https://arrow.apache.org/docs/python/feather.html>`__ and one `Apache Parquet <https://parquet.apache.org/>`__ that are logically equivalent (but some systems prefer one over the other). The collection of these files is the end result of running the ETL steps. The “sidecar” json file contains additional metadata like better labels for the individual columns, information on the sources and so on.

A data table/dataframe is composed of two types of columns. The first kind is the index columns, also sometimes called dimension columns and similar conceptually to a composite primary key in database design. Usually for our data these are year and entity. Entity is usually the country but can also be some other concept like fish species etc for specific datasets.

The other kind of columns are value columns. We often call one single value column a “variable” (this naming comes from an older data model in a MySQL database). Rows are ususally observations where every index tuple is unique (i.e. there is only a single row for a given year+country combination). For a single variable we have additional metadata in the sidecar json file, for example a nice human readable name, in case of numeric variables often the unit (for plotting purposes often both a short and a long unit name), etc.

The first few rows of a typical OWID dataframe can thus look like this:

==== =========== ========== ==================
year entity_code population population_density
==== =========== ========== ==================
1950 AFG         7750000    13.52
1951 AFG         7840000    13.57
…    …           …          …
==== =========== ========== ==================

High level pipeline overview
----------------------------

On a high level our data pipeline consists of four steps:

.. mermaid::
    :align: center

    graph LR

    upstream --> download --> format[format dataframe] --> harmonise --> grapher[grapher export] --> plot

① Ingest the data and store it
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This entails locating data releases, downloading the data, storing a snapshot in our external data snaphot repository called **Walden** (named after `Walden Pond <https://en.wikipedia.org/wiki/Walden_Pond>`_), and collect and store some metadata alongside. At this point in the pipeline, the data exists as a zip file of all the files (or API responses) as they were fetched from the original source.

② Extract the data into dataframe form
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This entails bringing the data into pandas data frames and storing them on disk. The data is still in a form very similar to that provided by the upstream data source but it can now be easily loaded as a dataframe. More metadata is often added in this step (e.g. more extensive variable descriptions etc). We call this step **Meadow**, because this is still a relatively “wild” version of the data (as opposed to the more refined and groomed version of the next step which we call “Garden”)

③ Harmonize common dimensions and enrich the metadata
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This usually involves some data cleaning, adding more metadata like unit information and harmonizing of common dimensions like geographic area. The latter is important so that we can plot data from different data sources in one chart (e.g. a scatter plot of GDP from the world bank and child mortality by the WHO where each mark is a country in a given year). This version of the data is called the **Garden** level as this is a nicely curated, harmonized dataframe optimized for data science work. These dataframes can have more dimensions than our usual country+year combination - for example there can be an additional index column for the age group. For data science uses, this level in our pipeline is the most user friendly one.

④ Split the data into Grapher's simpler data model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Our visualization tool Grapher is optimized for time series display of country level statistics. As such it uses a restricted data model that makes this use case easier, but makes other use cases harder.

.. admonition:: Key point

    All data visualised on the Our World In Data site is represented internally as a four-tuple:

    ``(time, entity, variable, value)``

    This is flexible enough to capture all kinds of data, but struggles with datasets with subcategories, dimensions and fine detail.

For example, a variable representing GDP in international $ will have a value for Australia in 1990. The tuple for the could be ``(1990, "Australia", "GDP intl. $", 31016)``.

In general data analysis, a variable could have extra dimensions. For example, deaths over time could be broken down by country, cause, gender and age group. Since grapher's data model cannot contain these dimensions, we expand them into many variables.

.. mermaid::

    graph LR

    Deaths[Deaths] --> male[Deaths - Male]
    Deaths --> female[Deaths - Female]
    Deaths --> both[Deaths - Both genders]

    male --> male_0_5[Deaths - Male - 0-5 years old]
    male --> male_5_10[Deaths - Male - 5-10 years old]
    male --> male_dot_dot[Deaths - Male - ...]

    female --> female_dot_dot[Deaths - Female - ...]

    both --> both_dot_dot[Deaths - Both genders - ...]

The data now fits grapher's data model and can be charted.

If the ``entity`` is a country, then a map view will be enabled in Grapher plots, but in general the ``entity`` could be anything, for example species of fish, types of energy production, etc.

In general data analysis, the line between what should be interpreted as ``variable`` vs ``entity`` is not always clear cut. The data can often be pivoted either way depending on what makes sense for visualisation.


Harmonization tables
--------------------

For important and common index columns, notably countries/regions, there exists a regions dataset that enumerates the set of commonly understood entities. In this dataframe all countries and geographic regions are listed with their *unique code* used at Our World In Data, as well as additional information like contained smaller units, additional third party identifiers, etc.


.. admonition:: Unique code used at Our World In Data

    At Our World in Data we use an "enriched" ISO code for ocuntries. In the case of country/region we use ISO Alpha 3 country codes as a base, but we add additional ones for entities that we need that do not have such a code assigned (e.g. historic entities, continents, subnational regions, etc.).

Layout of the ETL repository
----------------------------

The `ETL github repository`_  contains the code of the OWID ETL pipeline. For every dataset release there exists a folder in ``/etl/steps`` with the python code to take this dataset from the walden snapshot stage all the way through the pipeline (usually all the way to the Grapher stage). When running the etl command, either a subset or all of these steps are executed and produce their output dataframes and acompanying metadat files in ``/data``. The folder structure between the steps and the produced data files matches closely by convention (some scripts generate more than one table but the folder structure is still mirrored between steps and data folders.)

Design goals and non-goals of the ETL
-------------------------------------

Goals
~~~~~

-  Create a centralized place for all our data transformation code
-  Make it easy for everyone to re-run our transformation code and make changes to it, without requiring to set up complicated infrastructure
-  Enable automatic dependency tracking and recomputing downstream dependencies (e.g. all per capita metrics across datasets whenever the population dataset is updated)

Non-goals
~~~~~~~~~

-  Require expensive/complicated infrastructure to run our code
-  Optimize for data that is released in realtime
-  Support multiple terabytes of data

Introduction to the ETL
=======================

.. contents::
    :local:
    :depth: 2

Data management at OWID
-----------------------

Our World In Data is a data republisher, meaning that we import data from a wide range of upstream data sources and then try to share it with people in standard ways.

.. mermaid::
    :align: center

    graph LR

    wb[World Bank] --> catalog --> site[Our World in Data site]
    un[UN FAO] --> catalog
    wpp[UN WPP] --> catalog
    dot[...] --> catalog

The ETL (from "extract, transform, load") is our system for bringing in and harmonising data for use in Our World In Data site.

Originally, there was one way to get data in, using the internal Grapher admin site:

.. mermaid::
    :align: center

    graph LR

    upstream --> admin --> mysql

Over time, OWID developed a data team and started importing much larger institutional datasets, ones that needed substantial code for importing. This added a second way to get data in, by running code in the ``importers`` repository.

.. mermaid::
    :align: center

    graph LR

    upstream --> admin --> mysql
    upstream --> importers --> mysql

The ETL aims to replace ``importers``, and make our handling of *big institutional datasets* even better. It also creates an on-disk data catalog that can be reused outside of our site.

.. mermaid::
    :align: center

    graph LR

    upstream --> admin --> mysql
    upstream --> etl --> catalog[catalog on disk] --> mysql

The main users of the ETL are data managers. The ETL is desinged to make their work fully repeatable and reviewable. The admin is still available for manual imports, and may be faster for datasets that have been transformed by hand.

Design goals
------------

Why create a new system for importing data, when we already had one?

The ETL emerged from interviews and discussions with the data and engineering teams mid-2021. It is designed to tackle several issues that were spotted in OWID's data management.

Allow an incremental workflow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The data team spent very long on large institutional datasets. At the end of their work, a large amount of data was polished and imported into Grapher, of which only a small amount made it onto the OWID site. Since OWID's data is meant to be evergreen, the data already in the catalog had become a maintanence burden for the data team, even though 95% of it had never been used.

Improve the ability to review and QA work
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Promote reuse of OWID's data work
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Stages of data management
-------------------------

Our World In Data has a whole team dedicated to data management that takes data from publicly available sources, such as the UN Food and Agriculture Organisation, and makes it available to our researchers to visualise in the articles that they write.

.. mermaid::
    :align: center

    graph LR

    upstream --> download --> format --> harmonise --> import --> plot

To make something chartable on the Our World In Data site, a data manager must:

1. Locate the *upstream* data source
2. *Download* and keep a copy of the data for later use (:ref:`walden<Walden: the data lake>`)
3. Bring the data into a *common format* (:ref:`meadow<Meadow: standardised format>`)
4. *Harmonise* the names of countries, genders and any other columns we may want to join on (:ref:`garden<Garden: ready for data science>`)
5. *Import* the data to our internal MySQL database (:ref:`grapher<Grapher: ready for the OWID site>`)

After these steps, the data is available to be plotted on our site.

Steps in the ETL
----------------

The ETL is a Python project that is designed to be run from the command line. It can be used to build from raw ingredients any dataset that Our World In Data features on our site.

For example, running::

    etl data://garden/un/2022-07-11/un_wpp

will trigger a series of steps to build the UN World Population Prospects dataset locally in the ``data/garden/un/2022-07-11/un_wpp`` folder.
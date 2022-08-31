History
=======

.. contents::
    :local:
    :depth: 2

A brief history
---------------

This document gives some more history that is especially relevant for staff at OWID to understand how various pieces of legacy workflows fit in with ETL.

For context, recall that the basic flow of data from institutional publishers to our site is (and has been for a long time), something like this on the high level:

.. mermaid::
    :align: center

    graph LR

    wb[World Bank] --> catalog --> site[Our World in Data site]
    un[UN FAO] --> catalog
    wpp[UN WPP] --> catalog
    dot[...] --> catalog

Originally, there was one way to get data in, using the internal Grapher admin site:

.. mermaid::
    :align: center

    graph LR

    upstream --> admin (manual upload) --> mysql

Over time, OWID developed a data team and started importing much larger institutional datasets, ones that needed substantial code for importing. This added a second way to get data in, by running code in the ``importers`` repository.

.. mermaid::
    :align: center

    graph LR

    upstream --> admin (manual upload) --> mysql
    upstream --> importers --> mysql

This project, the ETL, aims replace ``importers``, and make our handling of big institutional datasets even better. It also creates an on-disk data catalog that can be reused outside of our site.

.. mermaid::
    :align: center

    graph LR

    upstream --> admin (manual upload) --> mysql
    upstream --> etl --> catalog[catalog on disk] --> mysql

The main users of the ETL are data managers. The ETL is desinged to make their work fully repeatable and reviewable. The admin is still available for manual imports, and may be faster for datasets that have been transformed by hand.

Stages of data management
-------------------------

Our World In Data has a whole team dedicated to data management that takes data from publicly available sources, such as the UN Food and Agriculture Organisation, and makes it available to our researchers to visualise in the articles that they write.

.. mermaid::
    :align: center

    graph LR

    upstream --> download --> format --> harmonise --> import --> plot

To make something chartable on the Our World In Data site, a data manager must:

1. Locate the *upstream* data source
2. *Download* and keep a copy of the data for later use (``walden``)
3. Bring the data into a *common format* (``meadow``)
4. *Harmonise* the names of countries, genders and any other columns we may want to join on (``garden``)
5. *Import* the data to our internal MySQL database (``grapher``)

After these steps, the data is available to be plotted on our site. Alongside the later steps are optional moments for review by data managers or researchers.

The design of the ETL involves stages that mirror the steps above. These help us to meet several design goals of the project.
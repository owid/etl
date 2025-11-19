---
tags:
  - Architecture
---

# ETL steps

We have designed an ETL (Extract, Transform, Load) system that allows us to manage the entire lifecycle of data, from ingestion to publication.

There are three main types of ETL steps: snapshots (ingest data from source), data steps (process and curate data), and export steps (make data available to other parts of the system). We elaborate on these below.

## Five stages

The ETL project provides an opinionated data management workflow, which separates a data manager's work into five stages:

```mermaid
graph LR

snapshot --> format --> harmonize/process --> import --> publish
```

The design of the ETL involves steps that mirror the stages above, which help us to meet several design goals of the project:

1. [Snapshot step](#snapshot): Take a **snapshot** of the upstream data product and store it.
2. [Meadow step](#meadow): Bring the data into a **common format**.
3. [Garden step](#garden): **Harmonise** the names of countries, genders and any other columns we may want to join on. Also do the necessary **data processing** to make the dataset usable for our needs.
4. [Grapher step](#grapher): **Adapt** the data to our internal MySQL database.
5. [Export step](#export-steps): **Publish** the data to other parts of the system, such as data explorers or multi-dimensional indicators.

A data manager must implement all these steps to make something chartable on the Our World in Data site.

!!! info

    When all steps (1 to 4) are implemented, the data is available for publication on our site. The publication step can involve creating new charts or updating existing ones with the new data.

# Our journey

We generally do not produce data but instead stand on the shoulders of big institutions like the UN and World Bank (and many others), as well as the work of individual researchers and small research groups. Our role is to promote the work of high-quality data providers and share it with the public in a context that makes it understandable.

```mermaid
graph LR

wb[World Bank] --> catalog --> site[Our World in Data site]
un[UN FAO] --> catalog
wpp[UN WPP] --> catalog
dot[...] --> catalog
```

## Early days

In our early days, we developed the [Grapher](https://github.com/owid/owid-grapher) data visualization library to give us more control over how we visualize data. Like other tools for data journalism, such as Datawrapper, Grapher had an admin interface that let you upload a CSV to a database and make a chart with it for the site.

```mermaid
graph LR

upstream --> upload[manual upload] --> mysql --> site[Our World in Data site]
```

## Importers

??? failure "Deprecated"

    The `importers` repository is now deprecated, and only `etl` repository is used.


During the pandemic, we had to update data daily, and clicking through an admin interface became inefficient. We developed a series of scripts to directly insert data into our database (MySQL) for use on our site, the [importers](https://github.com/owid/importers) repo.

```mermaid
graph LR

upstream --> upload[manual upload] --> mysql --> site
upstream --> importers --> mysql
```

Over time, we began _remixing_ data from more sources and noticed that our data scripts were insufficient to maintain a constantly growing catalog of datasets.

## ETL: Current solution
We needed a better solution to build a large dataset catalog, which should be a scalable and maintainable solution:

- It should be friendly to data scientists and well-established tools (such as python). Data should be consumable with `python` and in analytics environments like Jupyter.
- It should be transparent. Anyone should be able to trace back how the data had been processed, from one of our charts all the way to the original data source.
- Versioning of datasets. Git is great at keeping a history of versions, but it is unsuitable for large files. Data recipes (scripts) can live in Git, but data files should be on a dedicated disk.

To this end, we developed the current project, the ETL, as the next stage in how we process data. The ETL generates an on-disk data catalog based on flat files. We import this catalog into MySQL for use on our site, but we also use it to power a [public API](../../api/).

```mermaid
graph LR

upstream --> upload[manual upload] --> mysql --> site
upstream --> etl --> catalog[catalog on disk] --> mysql
catalog --> API
```

The main users of the ETL are data managers. The ETL is designed to make their work fully repeatable and reviewable. The admin is still available for manual imports and may be faster for datasets that have been transformed by hand.


<!-- Read about our [data management workflow](workflow/index.md) to learn more about how we use the ETL. -->

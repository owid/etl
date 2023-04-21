
Our World In Data has a whole team dedicated to data management that takes data from publicly available sources, such as the UN Food and Agriculture Organisation, and makes it available to our researchers to visualise in the articles that they write.

## Five stages

The ETL project supports an opinionated data management workflow, which separates a data manager's work into several stages:

```mermaid
graph LR

snapshot --> format --> harmonise --> import --> publish
```

The design of the ETL involves steps that mirror the stages above, which help us to meet several design goals of the project:

1. [Snapshot step](architecture/workflow/snapshot.md): Take a **snapshot** of the upstream data source.
- [Meadow step](architecture/workflow/common-format.md): Bring the data into a **common format**.
- [Garden step](architecture/workflow/harmonization.md): **Harmonise** the names of countries, genders and any other columns we may want to join on.
- [Grapher step](architecture/workflow/grapher.md): **Import** the data to our internal MySQL database.

A data manager must implement all these steps to make something chartable on the Our World in Data site. Afterwards, the data is available for publication on our site. The [publication step](architecture/workflow/publish.md) can involve creating new charts or updating existing ones with the new data.
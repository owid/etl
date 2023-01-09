# Data management workflow

## Five stages

Our World In Data has a whole team dedicated to data management that takes data from publicly available sources, such as the UN Food and Agriculture Organisation, and makes it available to our researchers to visualise in the articles that they write.

The ETL project supports an opinionated data management workflow, which separates a data manager's work into several stages:

```mermaid
graph LR

snapshot --> format --> harmonise --> import --> publish
```

To make something chartable on the Our World In Data site, a data manager must:

1. Take a snapshot of the upstream data source
2. Bring the data into a _common format_ (`meadow`)
3. _Harmonise_ the names of countries, genders and any other columns we may want to join on (`garden`)
4. _Import_ the data to our internal MySQL database (`grapher`)
5. _Publish_ charts that make use of the data to the Our World in Data site

After these steps, the data is available to be plotted on our site. Alongside the later steps are optional moments for review by data managers or researchers.

The design of the ETL involves stages that mirror the steps above. These help us to meet several design goals of the project.
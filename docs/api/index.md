# Our World In Data - Data APIs

At Our World In Data we maintain a highly curated set of charts on our website. The data and metadata for these charts is available through our Public Chart API. It is available as CSV/JSON via HTTP and so is easily consumed from any programming language. The data this API provides is optimized for creating interactive charts and is easy to use.

We also have a larger data catalog in our ETL. This is the space where we fetch, process and prepare the data used in our charts. It contains substantially more data but it is curated to different degrees in different parts. It also offers an API although one that is less straightforward to access - for now we offer only a Python client to interact with this API. This data is structured according to our internal needs and varies a lot more in shape than the data in the public charts API - e.g. the public charts API only has time series data using time (usually the year) and entity (usually the country), whereas our ETL often has much larger datasets than contain additional dimensions like a breakdown by age group and gender.

This documentation briefly describes both of these APIs.

# Chart data API

Our chart API is structured around charts on our website, i.e. at https://ourworldindata.org/grapher/* . You can find charts by searching for them in our data catalog at https://ourworldindata.org/data.

Once you have found the chart with the data you care about, fetching it is just appending the suffix ".csv" to fetch the data or ".metadata.json" to fetch the metadata. You can also append ".zip" to fetch a zip file containing these two files plus a readme markdown file that describes the data.

An example for our life expectancy chart:
- https://ourworldindata.org/grapher/life-expectancy - the page on our website where you can see the chart
- https://ourworldindata.org/grapher/life-expectancy.csv - the data for this chart (see below for options)
- https://ourworldindata.org/grapher/life-expectancy.metadata.json - the metadata for this chart, like the chart title, the units, how to cite the data sources
- https://ourworldindata.org/grapher/life-expectancy.zip - the above two plus a readme as zip file archive

## Options

The following options can be specified for all of these endpoints:

**csvType**
- `full` (default): Get the full data, i.e. all time points and all entities
- `filtered`: Get only the data needed to display the visible chart. For a map chart this will be only data for a single year but all countries, for a line chart it will be the selected time range and visible entities, ...

Note that if you use `filtered`, the other query parameters in the URL will change what is downloaded. E.g. if you navigate to our life-expectancy chart and then visually select the country "Italy" and change the time range to 1950-2000 you will see that the URL in the browser is modified to include `?time=1980..2000&country=~ITA`. When you make a request to any of the endpoints above you can include any of these modifications to get exactly that data:

```
https://ourworldindata.org/grapher/life-expectancy.csv?csvType=filtered&time=1980..2000&country=~ITA
```

**useColumnShortNames**
- `false` (default): Column names are long, use capitalization and whitespace - e.g. `Period life expectancy at birth - Sex: all - Age: 0`
- `true`: Column names are short and don't use whitespace - e.g. `life_expectancy_0__sex_all__age_0`

```
https://ourworldindata.org/grapher/life-expectancy.csv?useShortNames=true
```

## Example notebooks

Check out this list of public example notebooks that demonstrate the use of our chart API:
- https://colab.research.google.com/drive/1HDcqCy6ZZ05IznXzaaP9Blvvp3qoPnP8?usp=sharing
- https://observablehq.com/@owid/recreating-the-life-expectancy-chart

## CSV structure

The high level structure of the CSV file is that each row is an observation for an entity (usually a country or region) and a timepoint (usually a year). For example, the first three lines of the data for our life expectancy chart look like this:

> Entity,Code,Year,Period life expectancy at birth - Sex: all - Age: 0
> Afghanistan,AFG,1950,27.7275
> Afghanistan,AFG,1951,27.9634

The first two columns in the CSV file are "Entity" and "Code". "Entity" is the name of the entity, which often is a country, for example "United States". "Code" is the OWID internal entity code that we use if the entity is a country or region. For normal countries, this is the same as the [iso alpha-3](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3) code of the entity (e.g. "USA") - for non-standard countries like historical countries these are custom codes. Country/Region codes across all the data at Our World In Data is harmonized, so you can join two or more of our datasets on either of these columns.

The third column is either "Year" or "Day". If the data is annual, this is "Year" and contains only the year as an integer. If the column is "Day", the column contains a date string in the form "YYYY-MM-DD".

The final columns are the data columns, which are the time series that powers the chart. For simple line charts there is only a single data column, more complex charts can have more columns.

## Metadata structure

The .metadata.json file contains metadata about the data package. The "charts" key contains information to recreate the chart, like the title, subtitle etc.. The "columns" key contains information about each of the columns in the csv, like the unit, timespan covered, citation for the data etc.. Here is a (slightly shortened) example of the metadata for the life-expectancy chart:

```json
{
    "chart": {
        "title": "Life expectancy",
        "subtitle": "The [period life expectancy](#dod:period-life-expectancy) at birth, in a given year.",
        "citation": "UN WPP (2022); HMD (2023); Zijdeman et al. (2015); Riley (2005)",
        "originalChartUrl": "https://ourworldindata.org/grapher/life-expectancy",
        "selection": ["World", "Americas", "Europe","Africa","Asia","Oceania"]
    },
    "columns": {
        "Period life expectancy at birth - Sex: all - Age: 0": {
            "titleShort": "Life expectancy at birth",
            "titleLong": "Life expectancy at birth - Various sources – period tables",
            "descriptionShort": "The period life expectancy at birth, in a given year.",
            "descriptionKey": [
                "Period life expectancy is a metric that summarizes death rates across all age groups in one particular year.",
                "..."
            ],
            "shortUnit": "years",
            "unit": "years",
            "timespan": "1543-2021",
            "type": "Numeric",
            "owidVariableId": 815383,
            "shortName": "life_expectancy_0__sex_all__age_0",
            "lastUpdated": "2023-10-10",
            "nextUpdate": "2024-11-30",
            "citationShort": "UN WPP (2022); HMD (2023); Zijdeman et al. (2015); Riley (2005) – with minor processing by Our World in Data",
            "citationLong": "UN WPP (2022); HMD (2023); Zijdeman et al. (2015); Riley (2005) – ...",
            "fullMetadata": "https://api.ourworldindata.org/v1/indicators/815383.metadata.json"
        }
    },
    "dateDownloaded": "2024-10-30"
}
```

# ETL catalog API

The ETL catalog API makes it possible to access the dataframes our data scientists use to prepare the data for our public charts.

When using this API, you have access to the public catalog of data processed by our data team. The catalog indexes _tables_ of data, rather than datasets or individual indicators. To learn more, read about our [data model](../architecture/design/common-format.md).

At the moment, this API only supports [Python](python.ipynb).


!!! warning "Our ETL API is in beta"

    We currently only provide a python API for our ETL catalog. Our hope is to extend this to other languages in the future. Please [report any issue](https://github.com/owid/etl) that you may find.

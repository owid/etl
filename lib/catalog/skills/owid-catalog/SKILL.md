---
name: owid-catalog
description: >-
  Access Our World in Data from Python with the owid-catalog library: load chart
  data, catalog tables or individual indicators as pandas DataFrames that carry
  their own units, descriptions, sources and citations. Use this skill whenever
  the work happens in Python or a notebook (pandas, a uv script, matplotlib,
  parquet); whenever you need an indicator's metadata, units or codebook;
  whenever you need dimensions that published charts flatten away, such as sex,
  age group or projection variant; or whenever you need to search OWID's full
  catalog of indicators and tables, including semantic search by meaning, rather
  than only its published charts. For language-agnostic HTTP access without
  Python, use the `owid` skill from github.com/owid/skills instead.
metadata:
  owner: Marigold
---

The `owid-catalog` library provides a unified Python API for discovering and loading OWID datasets. It supports three search kinds: **charts** (published visualizations), **tables** (catalog datasets), and **indicators** (semantic search via embeddings).

Charts are the most curated and best-documented uses of the data, so for answering questions about data they are often a better starting point than indicators. One chart can use a single indicator or several.

Indicators give access to the full catalog of time series, with varying levels of curation. Indicators and tables are addressed by ETL catalog paths, for example `garden/un/2024-07-12/un_wpp/population#population`. The path fragments are:
- channel: stage of curation
- namespace: often the data provider (who, un, wb), sometimes a topic area when that is more useful
- version: the dataset release identifier — the date OWID released the dataset, not the source's release date
- dataset: the dataset short name
- table: the table the indicator belongs to
- column: the indicator's short name (after `#`)

Channels are levels of curation. **meadow** is upstream data as a dataframe. **garden** is where the data is cleaned and processed; garden tables can carry extra dimensions beyond time and entity (sex, age group, projection variant) and tend to be wide. **grapher** is the data reshaped for OWID's charting tool, which only understands time and entity, so the extra dimensions are flattened into separate columns. For indicator work, grapher or garden is usually what you want: garden when the extra dimensions help, grapher when you want simple series that merge easily.

Tables are whole datasets' worth of indicators. Table search is more primitive and the frames can be large (hundreds of columns, or millions of rows — `garden/un/2024-07-12/un_wpp/population` is ~13M rows because of its sex × age × variant dimensions), but when you need several indicators from one dataset they save you joining them by hand. Fetching a single indicator with `#column` still loads every row of its table.

Country names are harmonized across OWID data, so tables join cleanly on entity and time.

Once you know which data you need, **always print the codebook** to bring units, descriptions and sources into context. It is what keeps an analysis from misreading a percentage as a share or a per-capita value as a total.

Suggest to the user that they credit the data. If an origin has `citation_full`, suggest that. Otherwise build an acknowledgment like "PROVIDER 1, PROVIDER 2, ... with processing by Our World in Data", using each origin's `attribution`, or `producer` as a fallback (see [Metadata and citations](#metadata-and-citations)).

## Installation

If `uv` is available (preferred), use inline script dependencies — no separate install step:

```python
# /// script
# requires-python = ">=3.11"
# dependencies = ["owid-catalog"]
# ///
```

Run with:
```bash
uv run --no-project script.py
```

Without `uv`:
```bash
pip install owid-catalog
```

## Quick start

```python
# /// script
# requires-python = ">=3.11"
# dependencies = ["owid-catalog"]
# ///

from owid.catalog import fetch, search

# Fetch chart data by slug — returns a Table (a DataFrame with metadata)
tb = fetch("life-expectancy")
print(tb.head(30).to_csv())
print(tb.codebook.to_csv())

# Search charts, then fetch the top result
results = search("population")
print(results.to_frame().head(30).to_csv())
tb = results[0].fetch()
```

## Plain-text output

The default display of `ResponseSet`, `Table` and the codebook is rich formatting meant for notebooks; printed as text it is truncated to a few columns. Convert to CSV instead:

```python
print(search("gdp per capita").to_frame().to_csv())  # search results
print(tb.head(30).to_csv())                          # data
print(tb.codebook.to_csv())                          # column, title, description, unit, source
```

## Charts

Fetch data from any published chart by slug or full URL:

```python
from owid.catalog import fetch, search

tb = fetch("life-expectancy")
tb = fetch("https://ourworldindata.org/grapher/life-expectancy")

# Search charts (10 results by default; pass limit= for more)
results = search("child mortality", limit=30)
print(results.to_frame().to_csv())  # titles, descriptions, URLs
tb = results[0].fetch()
```

Chart tables are indexed by `entities` and `years` (not `country`/`year`), and their value columns get generated names such as `life_expectancy_0`. Read `tb.columns` before referring to a column.

## Tables

Search the full data catalog for tables by name, namespace, dataset, version or channel. This covers every dataset in the catalog, not just those behind published charts.

```python
from owid.catalog import fetch, search

# Fuzzy, typo-tolerant matching on the table name (default)
results = search("population", kind="table")
print(results.to_frame().head(30).to_csv())

# Filter by data provider
results = search("wdi", kind="table", namespace="worldbank_wdi")

# Matching modes: "fuzzy" (default), "exact", "contains", "regex"
results = search("gdp.*capita", kind="table", match="regex")

# Keep only the latest version of each table
results = search("population", kind="table", latest=True)

# Fetch by catalog path: a whole table, or one indicator from it
tb = fetch("garden/un/2024-07-12/un_wpp/population")
tb = fetch("garden/un/2024-07-12/un_wpp/population#population")
```

`namespace`, `version`, `dataset`, `channel`, `match` and `case` only apply to table search; chart and indicator search ignore them.

## Indicators

Semantic search using vector embeddings — finds indicators by meaning, not just keywords:

```python
from owid.catalog import Client, search

results = search("share of energy from renewable sources", kind="indicator", latest=True)
print(results.to_frame().head(30).to_csv())

# All fields: unit, score, n_charts, popularity, channel, namespace, ...
print(results.to_frame(all_fields=True).head(30).to_csv())

tb = results[0].fetch()        # the single indicator column
tb = results[0].fetch_table()  # the full table containing it
```

Results are ranked by relevance: a blend of semantic similarity (60%) and popularity, i.e. how much the indicator is viewed (40%). Without `latest=True` the same indicator often appears several times, once per dataset version. `latest=True` deduplicates *after* `limit` is applied, so it can return far fewer than `limit` results — raise `limit` (e.g. `limit=50`) when you use it. `search()` has no sort argument; re-sort the `ResponseSet`, or rank by similarity alone through the client:

```python
results = results.sort_by("n_charts", reverse=True)  # any result field
results = Client().indicators.search("CO2 emissions per capita", sort_by="similarity")
```

## Metadata and citations

Every column of a fetched `Table` carries its own metadata, including the origins that the citation guidance above draws on:

```python
meta = tb["life_expectancy_0"].metadata
print(meta.unit, meta.short_unit, meta.description_short)
for origin in meta.origins:
    print(origin.producer, origin.attribution, origin.citation_full)
```

## Working with results

Search returns a `ResponseSet`:

```python
results = search("gdp", kind="table")

first = results[0]
for r in results[:5]:
    print(r.title)

filtered = results.filter(lambda r: "worldbank" in r.namespace)
sorted_results = results.sort_by("popularity", reverse=True)

# The single newest result (by version, or last_updated for charts) — not a filtered set
newest = results.latest()

df = results.to_frame()                  # DataFrame of the main fields
df = results.to_frame(all_fields=True)   # every field
records = results.to_dict()              # list of plain dicts

# In Jupyter, for human users only: switch the notebook display
results.set_ui_advanced()
results.set_ui_basic()  # default
```

## Plotting with owid-grapher-py

`owid-grapher-py` renders OWID-style interactive charts in a notebook. Its `plot()` expects `year` and `entity` columns by default, so name the chart table's columns explicitly:

```python
# dependencies = ["owid-catalog", "owid-grapher-py"]
from owid.catalog import fetch
from owid.grapher import plot

tb = fetch("life-expectancy")
df = tb.reset_index()
chart = plot(df, x="years", entity="entities", y="life_expectancy_0", title="Life expectancy", types=["line", "map"])
```

"""Bespoke viz step publishing the JSON feed for the "How democracy relates to development" visualization.

The visualization (owid-grapher `bespoke/projects/democracy-development`) draws four scatter panels --
GDP per capita, children surviving to age 5, people above the $10-a-day poverty line, expected years
of schooling -- against a switchable democracy index (V-Dem's liberal and electoral indices, the EIU
Democracy Index, Freedom House's total score), for one year at a time with a slider, with bubbles
optionally sized by population.

The feed is three files in the step's output folder:

* `democracy-development.metadata.json` -- the manifest the bundle fetches first: the entities (each
  with its continent; its position in the list is the id the data file uses), the year range the
  slider covers, and per series the unit, the tolerance and the provenance of the garden column it is
  built from, plus the source line assembled from every column's origins.
* `democracy-development.data.json` -- the values: `series[<key>][<entity id>] = [[year, value], ...]`,
  sorted by year, countries only, from `FIRST_YEAR` on, rounded to the precision the chart shows.
  Nothing is interpolated or matched across years here: the bundle applies grapher's tolerance rule
  (nearest year within the indicator's tolerance) when it draws a year, and says so in the tooltip.
* `metadata.json` -- the feed's provenance derived from the garden columns (see `etl.viz.bespoke`).

The files go to the step's output folder; the framework syncs that folder to the R2 path of the
environment being built, so the feed is served at
`<root>/v1/bespoke/democracy/latest/democracy_development/democracy-development.metadata.json` --
`api.ourworldindata.org` on production, and `api-staging.owid.io/<env>` on a staging server or a
laptop. Run without --grapher to skip the upload and only write the local files:

  .venv/bin/etlr viz://bespoke/democracy/latest/democracy_development
"""

import json
from dataclasses import dataclass
from typing import Callable

import pandas as pd
from owid.catalog import Dataset, Table, Variable
from structlog import get_logger

from etl.helpers import PathFinder
from etl.viz.bespoke import build_feed_metadata, variable_meta_to_api_dict, write_feed_metadata

log = get_logger()
paths = PathFinder(__file__)

MANIFEST_FILENAME = "democracy-development.metadata.json"
DATA_FILENAME = "democracy-development.data.json"

# What a reader is told the feed is, beyond the citation derived from the garden columns.
FEED_TITLE = "How democracy relates to development"

# The slider runs from 1990 to the latest year; the feed starts ten years earlier because the widest
# tolerance the bundle applies (expected years of schooling, 10 years) can reach back that far, and a
# value the bundle can never show has no business in the file.
FIRST_YEAR_SHOWN = 1990
MAX_TOLERANCE_YEARS = 10
FIRST_YEAR = FIRST_YEAR_SHOWN - MAX_TOLERANCE_YEARS

# The continents that color the dots, in legend order. Membership comes from the regions dataset.
CONTINENTS = ["Africa", "Asia", "Europe", "North America", "Oceania", "South America"]

# A feed with fewer countries than this has lost a source, not a few small islands.
MIN_ENTITIES = 150

# The series a reader can put on the x axis; their last year is the slider's last year.
INDEX_KEYS = ["libdem", "electdem", "eiu", "fh"]


@dataclass(frozen=True)
class Series:
    """One series of the feed and where it comes from."""

    key: str
    dataset: str
    table: str
    column: str
    # Rows of the garden table to keep, for the tables carrying several indicators or dimensions.
    select: Callable[[Table], pd.Series] | None
    # Decimals kept in the file: the precision the chart shows, so the file is not padded with noise.
    decimals: int
    # The name a reader sees for this column in the feed's provenance (`metadata.json`).
    label: str
    # Plausible range of the values, inclusive; a value outside it is a unit or a selection mistake.
    value_range: tuple[float, float]


SERIES = [
    Series("libdem", "vdem", "vdem_multi_with_regions", "libdem_vdem", lambda tb: tb["estimate"] == "best", 3, "Liberal democracy index (V-Dem)", (0, 1)),
    Series("electdem", "vdem", "vdem_multi_with_regions", "electdem_vdem", lambda tb: tb["estimate"] == "best", 3, "Electoral democracy index (V-Dem)", (0, 1)),
    Series("eiu", "eiu", "eiu", "democracy_eiu", None, 2, "Democracy Index (Economist Intelligence Unit)", (0, 10)),
    # Freedom House deducts points for some countries, so a few total scores fall just below 0.
    # NOTE: Luxembourg scores 101 in 2002 in the source itself (a discretionary extra point that year).
    Series("fh", "fh", "fh", "total_score", None, 0, "Total democracy score (Freedom House)", (-5, 101)),
    Series("gdp", "wdi", "wdi", "ny_gdp_pcap_pp_kd", None, 0, "GDP per capita", (100, 300_000)),
    Series(
        "child_mortality", "igme", "igme", "observation_value",
        lambda tb: (tb["indicator"] == "Child mortality rate") & (tb["sex"] == "Total") & (tb["wealth_quintile"] == "Total") & (tb["unit_of_measure"] == "Deaths per 100 live births"),
        2, "Child mortality rate", (0, 100),
    ),
    Series(
        "poverty10", "world_bank_pip", "poverty", "headcount_ratio",
        lambda tb: (tb["ppp_version"].astype(str) == "2021") & (tb["poverty_line"].astype(str) == "1000") & (tb["welfare_type"].astype(str) == "income or consumption") & (tb["table"].astype(str) == "Income or consumption consolidated") & (tb["survey_comparability"].astype(str) == "No spells"),
        2, "Share of population living on less than $10 a day", (0, 100),
    ),
    Series("eys", "undp_hdr", "undp_hdr_sex", "eys", lambda tb: tb["sex"] == "total", 2, "Expected years of schooling", (0, 30)),
    Series("population", "population", "population", "population", None, 0, "Population", (1, 3e9)),
]
assert len({s.key for s in SERIES}) == len(SERIES), "Series keys must be unique."


def load_series(series: Series, ds: Dataset) -> Table:
    """One series as `country, year, value` rows, countries and years the feed covers only."""
    tb = ds.read(series.table, reset_index=True, safe_types=False)
    if series.select is not None:
        tb = tb[series.select(tb)]
    tb = tb[["country", "year", series.column]].rename(columns={series.column: "value"})
    tb = tb.dropna(subset=["value"])
    tb = tb[tb["year"] >= FIRST_YEAR]
    tb = tb.astype({"country": "string", "year": int})
    return tb


def select_countries(regions: Table) -> dict[str, str]:
    """Every current country and the continent it belongs to, `{country: continent}`.

    Regions, income groups and historical entities are left out: the chart is about countries, and a
    dot for "Europe" or "USSR" would sit among them as if it were one.
    """
    countries = set(regions.loc[(regions["region_type"] == "country") & (~regions["is_historical"].astype(bool)), "name"])
    members = paths.regions.get_regions(names=CONTINENTS, only_subregions=True)
    continent_of = {}
    for continent in CONTINENTS:
        for member in members[continent]:
            if member in countries:
                assert member not in continent_of, f"{member} is listed in both {continent_of[member]} and {continent}."
                continent_of[member] = continent
    return continent_of


def sanity_check_series(series: Series, tb: Table) -> None:
    """Check one series before it goes into the feed."""
    assert not tb.empty, f"{series.key}: no rows after selecting {series.column} from {series.dataset}/{series.table}."
    duplicated = tb[tb.duplicated(["country", "year"], keep=False)]
    assert duplicated.empty, f"{series.key}: several rows per country-year, e.g. {duplicated.head(3).to_dict('records')}."
    lo, hi = series.value_range
    outside = tb[(tb["value"] < lo) | (tb["value"] > hi)]
    assert outside.empty, f"{series.key}: {len(outside)} values outside [{lo}, {hi}], e.g. {outside.head(3).to_dict('records')}."
    latest = tb["year"].max()
    assert latest >= FIRST_YEAR_SHOWN, f"{series.key}: the latest year is {latest}, before the slider starts."


def sanity_check_feed(entities: list[str], continent_of: dict[str, str], series_rows: dict[str, Table]) -> None:
    """Check the assembled feed."""
    assert len(entities) >= MIN_ENTITIES, f"Only {len(entities)} countries in the feed; a source is probably missing."
    assert set(continent_of[e] for e in entities) == set(CONTINENTS), "Some continent has no country in the feed."
    # Every democracy index has to be there for the slider's whole range, or a year draws four empty panels.
    for key in ["libdem", "electdem"]:
        years = set(series_rows[key]["year"])
        missing = [y for y in range(FIRST_YEAR_SHOWN, max(years) + 1) if y not in years]
        assert not missing, f"{key} has no data for {missing}."
    # The four outcomes need a broad enough coverage in the latest decade to fill a panel.
    for key in ["gdp", "child_mortality", "poverty10", "eys"]:
        recent = series_rows[key][series_rows[key]["year"] >= 2015]
        assert recent["country"].nunique() >= 100, f"{key} covers only {recent['country'].nunique()} countries since 2015."


def indicator_entry(variable: Variable, tb: Table, update_period_days: int | None, label: str) -> dict:
    """The manifest's block for one series: what the bundle needs to read and cite the column."""
    api = variable_meta_to_api_dict(variable, update_period_days=update_period_days, default_title=label)
    display = api.get("display") or {}
    # Every origin the column carries, in order: a column built from several reports (the EIU index) or
    # with population-weighted aggregates (also the EIU index) has more than one, and the modal lists them.
    origins = [
        {field: origin.get(field) for field in ("producer", "title", "attribution", "datePublished", "dateAccessed", "urlMain", "citationFull")}
        for origin in api.get("origins") or []
    ]
    return {
        "name": api.get("name"),
        "titlePublic": (api.get("presentation") or {}).get("titlePublic"),
        "unit": api.get("unit") or "",
        "shortUnit": api.get("shortUnit") or "",
        "tolerance": display.get("tolerance") or 0,
        "numDecimalPlaces": display.get("numDecimalPlaces"),
        "descriptionShort": api.get("descriptionShort"),
        "descriptionKey": api.get("descriptionKey"),
        "origins": origins,
        "timespan": f"{int(tb['year'].min())}-{int(tb['year'].max())}",
        "catalogPath": f"{variable.metadata.dataset_path()}/{variable.name}" if hasattr(variable.metadata, "dataset_path") else None,
    }


def save_json(data: dict, filename: str) -> None:
    """Write one JSON file of the feed into the step's output folder."""
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    with open(paths.output_dir / filename, "w") as f:
        json.dump(data, f, separators=(",", ":"), ensure_ascii=False)


def run() -> None:
    #
    # Load data.
    #
    datasets = {name: paths.load_dataset(name) for name in sorted({s.dataset for s in SERIES})}
    continent_of = select_countries(paths.regions.tb_regions)

    series_rows: dict[str, Table] = {}
    columns: dict[str, Variable] = {}
    for series in SERIES:
        tb = load_series(series, datasets[series.dataset])
        tb = tb[tb["country"].isin(continent_of)]
        sanity_check_series(series, tb)
        series_rows[series.key] = tb
    # The slider ends where the democracy indices end; population projections to 2100 have no year to be
    # drawn against, so every series is cut there.
    last_year = max(int(series_rows[key]["year"].max()) for key in INDEX_KEYS)
    series_rows = {key: tb[tb["year"] <= last_year] for key, tb in series_rows.items()}
    for series in SERIES:
        tb = series_rows[series.key]
        # The column `build_feed_metadata` reads a series' origins off. Only the head of it: the metadata
        # is the same on every row, and the one thing that reads the values is the type inference.
        columns[series.label] = tb["value"].head(1000)

    #
    # Assemble the feed.
    #
    # An entity is in the feed if some series has a row for it; its id is its position in this list.
    entities = sorted(set().union(*(set(tb["country"]) for tb in series_rows.values())))
    sanity_check_feed(entities, continent_of, series_rows)
    entity_id = {name: i for i, name in enumerate(entities)}

    data = {}
    for series in SERIES:
        tb = series_rows[series.key].sort_values(["country", "year"])
        values = tb["value"].round(series.decimals)
        values = values.astype(int) if series.decimals == 0 else values.astype(float)
        rows = pd.DataFrame({"eid": tb["country"].map(entity_id).astype(int), "year": tb["year"].astype(int), "value": values})
        data[series.key] = {
            str(eid): [[int(y), (int(v) if series.decimals == 0 else float(v))] for y, v in zip(group["year"], group["value"])]
            for eid, group in rows.groupby("eid", sort=True)
        }

    # The feed's provenance, from the origins of the data it is built on, rather than a source string
    # typed in here that goes stale the next time a dataset is updated. The shortest update period among
    # the inputs is the one the "next update" line should promise.
    update_period_days = min(d.metadata.update_period_days for d in datasets.values() if d.metadata.update_period_days)
    feed_metadata = build_feed_metadata(title=FEED_TITLE, columns=columns, update_period_days=update_period_days)
    write_feed_metadata(paths.output_dir, feed_metadata)

    manifest = {
        "meta": {
            "title": FEED_TITLE,
            "source": feed_metadata["feed"]["citation"],
            "note": (
                "Values are the garden datasets' own, rounded to the precision the chart shows. The bundle matches a "
                "country's development measures to the democracy score of the year shown using each indicator's "
                "tolerance (the nearest year within that many years), and marks matched years in the tooltip."
            ),
        },
        "yearRange": {"first": FIRST_YEAR_SHOWN, "last": last_year},
        "continents": CONTINENTS,
        "entities": [{"name": name, "continent": CONTINENTS.index(continent_of[name])} for name in entities],
        "indicators": {
            series.key: indicator_entry(
                series_rows[series.key]["value"], series_rows[series.key],
                datasets[series.dataset].metadata.update_period_days, series.label,
            )
            for series in SERIES
        },
        "dataFile": DATA_FILENAME,
    }
    save_json(manifest, MANIFEST_FILENAME)
    save_json({"series": data}, DATA_FILENAME)

    log.info(
        "democracy_development.written",
        n_entities=len(entities),
        n_rows=sum(len(rows) for s in data.values() for rows in s.values()),
        years=f"{FIRST_YEAR_SHOWN}-{last_year}",
        dest=str(paths.output_dir),
    )

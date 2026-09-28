"""Bespoke viz step publishing the JSON feed for the migrant-demographics visualization.

The feed is built from the `migrant_stock_age_sex` garden dataset, and comes out as
`migrant-demographics.metadata.json`, which carries the age bands, the years, the source line the
visualization shows, and every entity with the code naming its file, plus one
`migrant-demographics.<code>.json` per entity holding that entity's migrant stock by sex and
five-year age band (`m`/`f`) alongside the resident population on the same bands (`pm`/`pf`), which
the visualization subtracts one from the other to get the population born in the country. Alongside
them goes `metadata.json`, the feed's provenance derived from the garden columns
(see `etl.viz.bespoke`).

Entities are named by their UN M49 location code rather than by a slug, because that is the file
name the published bundle already fetches.

The files go to the step's output folder; the framework syncs that folder to the R2 path of the
environment being built, so the feed is served at
`<root>/v1/bespoke/un_migration/latest/migrant_demographics/migrant-demographics.metadata.json` and
`.../migrant-demographics.<code>.json` -- `api.ourworldindata.org` on production, and
`api-staging.owid.io/<env>` on a staging server or a laptop.

Run without --grapher to skip the upload and only write the local files:
  .venv/bin/etlr viz://bespoke/un_migration/latest/migrant_demographics
"""

import json

import pandas as pd
from owid.catalog import Table, Variable
from structlog import get_logger
from tqdm.auto import tqdm

from etl.helpers import PathFinder
from etl.viz.bespoke import build_feed_metadata, write_feed_metadata

log = get_logger()
paths = PathFinder(__file__)

# The file that indexes the rest of the feed.
INDEX_FILENAME = "migrant-demographics.metadata.json"

# The bands each entity's arrays are ordered by, youngest first. The garden table also carries the
# `all` sex, which is the sum of the two the pyramid draws and would double count.
AGE_GROUPS = [
    "0-4",
    "5-9",
    "10-14",
    "15-19",
    "20-24",
    "25-29",
    "30-34",
    "35-39",
    "40-44",
    "45-49",
    "50-54",
    "55-59",
    "60-64",
    "65-69",
    "70-74",
    "75+",
]

# The feed's four arrays, and the garden row each reads from.
SERIES = {
    "m": ("male", "migrant_stock"),
    "f": ("female", "migrant_stock"),
    "pm": ("male", "population"),
    "pf": ("female", "population"),
}

# What a reader is told the feed is, beyond the citation derived from the garden columns.
FEED_TITLE = "International migrant stock by age and sex"
FEED_NOTE = (
    "'m'/'f' = migrant stock by sex and 5-year age band. 'pm'/'pf' = total resident population by "
    "sex and age band. Native-born = total population - migrants. Territories lacking UN/WPP "
    "population estimates, and entities reporting zero migrant stock in every year, have been "
    "excluded. Population figures are WPP interpolations; do not read person-level precision into "
    "them."
)

# The visualization requires a finite number in every band of every year it lists, and throws into
# its error state otherwise (`parseEntityYears` in owid-grapher's
# `bespoke/projects/migrant-demographics`). The UN reports no migrant stock at all for the years
# before South Sudan, Montenegro and Curacao existed as separate countries, so those years are
# published as zero -- which is what the hand-built feed did, and is wrong in the same way: it says
# nobody had migrated there rather than that nobody counted. Fixing it properly means letting an
# entity carry its own years, which is a change on the visualization's side.
UNREPORTED_STOCK = 0


def select_entities(tb: Table) -> Table:
    """The entities the feed carries, as `location_code, country`, ordered by name.

    Two kinds of entity are dropped, both of which reach a reader as a pyramid that says something
    false rather than as no data: the 34 small territories the UN publishes no population for, whose
    native-born population would draw as zero, and the entities it reports no migrant stock for in
    any year, whose pyramid would be empty under a subtitle counting its zero migrants.
    """
    totals = tb[tb["sex"] == "all"].groupby(["location_code", "country"], observed=True)
    totals = totals[["migrant_stock", "population"]].sum(min_count=1).reset_index()
    selected = totals[(totals["migrant_stock"] > 0) & (totals["population"] > 0)]
    return selected[["location_code", "country"]].sort_values("country").reset_index(drop=True)


def build_entity_data(rows: Table, years: list[int]) -> dict:
    """One entity's file: `{year: {"m": [...], "f": [...], "pm": [...], "pf": [...]}}`."""
    values = rows.set_index(["year", "sex", "age"]).sort_index()
    data = {}
    for year in years:
        data[str(year)] = {
            key: [_count(values.loc[(year, sex, age), column]) for age in AGE_GROUPS]
            for key, (sex, column) in SERIES.items()
        }
    return data


def _count(value) -> int:
    return UNREPORTED_STOCK if pd.isna(value) else int(value)


def metadata_column(tb: Table, column: str) -> Variable:
    """The column `build_feed_metadata` reads a measure's origins off.

    Only the head of it: the metadata is the same on every row, and the one thing that reads the
    values is the type inference, which stringifies each of them for an answer the first thousand
    already give.
    """
    return tb[column].head(1000)


def sanity_check_entities(tb: Table, entities: Table, years: list[int]) -> None:
    """Check the selected entities cover every year and band the feed's index promises."""
    expected = len(years) * len(AGE_GROUPS) * 2
    for code, country in zip(entities["location_code"], entities["country"]):
        rows = tb[(tb["location_code"] == code) & (tb["sex"] != "all")]
        assert len(rows) == expected, f"{country} has {len(rows)} rows of male and female data, not {expected}."
        # Only the migrant stock is published as a zero where the UN reports nothing; a gap in the
        # population would silently draw the whole of that band as native-born.
        missing = rows[rows["population"].isna()]
        assert missing.empty, f"{country} is missing the resident population in {len(missing)} band-years."


def save_json(data: dict, filename: str) -> None:
    """Write one JSON file of the feed into the step's output folder."""
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    with open(paths.output_dir / filename, "w") as f:
        json.dump(data, f, separators=(",", ":"))


def run() -> None:
    #
    # Load data.
    #
    ds_garden = paths.load_dataset("migrant_stock_age_sex")
    tb = ds_garden.read("migrant_stock_age_sex")
    tb = tb.astype({"country": "string", "sex": "string", "age": "string"})

    unknown_bands = sorted(set(tb["age"]) - set(AGE_GROUPS))
    assert not unknown_bands, f"`migrant_stock_age_sex` has age bands the feed does not order: {unknown_bands}"

    years = sorted(tb["year"].unique().tolist())
    entities = select_entities(tb)
    sanity_check_entities(tb, entities, years)

    #
    # Build the feed.
    #
    # The feed's provenance, from the origins of the data it is built on, rather than a source
    # string typed in here that goes stale the next time the dataset is updated.
    feed_metadata = build_feed_metadata(
        title=FEED_TITLE,
        columns={
            "Migrant stock": metadata_column(tb, "migrant_stock"),
            "Resident population": metadata_column(tb, "population"),
        },
        update_period_days=ds_garden.metadata.update_period_days,
    )
    write_feed_metadata(paths.output_dir, feed_metadata)

    save_json(
        {
            "meta": {
                "title": FEED_TITLE,
                "source": feed_metadata["feed"]["citation"],
                "unit": "persons",
                "note": FEED_NOTE,
            },
            "ageBands": AGE_GROUPS,
            "years": years,
            "entities": [
                {"code": int(code), "name": country}
                for code, country in zip(entities["location_code"], entities["country"])
            ],
        },
        INDEX_FILENAME,
    )

    for code, country in tqdm(
        zip(entities["location_code"], entities["country"]), total=len(entities), desc="Writing entity files"
    ):
        data = build_entity_data(tb[tb["location_code"] == code], years)
        save_json(data, f"migrant-demographics.{int(code)}.json")

    log.info(
        "migrant_demographics.written",
        n_files=len(entities) + 2,
        n_entities=len(entities),
        dest=str(paths.output_dir),
    )

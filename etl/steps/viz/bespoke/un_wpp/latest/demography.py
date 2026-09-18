"""Bespoke viz step publishing the JSON feed for the demography visualization.

The feed is built from the `un_wpp` garden dataset, and comes out as `demography.metadata.json`,
which lists every entity and the file name carrying it, plus one `demography.<slug>.data.json` per
entity holding that entity's population by age and sex, age-specific fertility rates, deaths by age
and sex, and net migration rate -- as estimates up to `LAST_ESTIMATE_YEAR`, and as the UN's medium
projection variant after it. Alongside them goes `metadata.json`, the feed's provenance derived
from the garden columns (see `etl.viz.bespoke`).

The files go to the step's output folder; the framework syncs that folder to the R2 path of the
environment being built, so the feed is served at
`<root>/v1/bespoke/un_wpp/latest/demography/demography.metadata.json` and
`.../demography.<slug>.data.json` -- `api.ourworldindata.org` on production, and
`api-staging.owid.io/<env>` on a staging server or a laptop.

Run without --grapher to skip the upload and only write the local files:
  .venv/bin/etlr viz://bespoke/un_wpp/latest/demography
"""

import json
import re
import unicodedata
from collections.abc import Callable
from typing import Any

from owid.catalog import Dataset, Table, Variable
from structlog import get_logger
from tqdm.auto import tqdm

from etl.helpers import PathFinder
from etl.viz.bespoke import build_feed_metadata, write_feed_metadata

log = get_logger()
paths = PathFinder(__file__)

# The file that indexes the rest of the feed.
INDEX_FILENAME = "demography.metadata.json"

# The five-year bands the visualization draws its population pyramid from. `un_wpp` also carries
# single years of age and wider bands over the same people (`0-14`, `15-64`), which would double
# count if they came along.
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
    "75-79",
    "80-84",
    "85-89",
    "90-94",
    "95-99",
    "100+",
]

# The bands the UN reports age-specific fertility rates over -- childbearing age, rather than the
# whole of life. `un_wpp` also carries the total fertility rate as age `all`, which the feed leaves
# out because the visualization sums the bands itself.
FERTILITY_AGE_GROUPS = [
    "10-14",
    "15-19",
    "20-24",
    "25-29",
    "30-34",
    "35-39",
    "40-44",
    "45-49",
    "50-54",
]

# `un_wpp` holds estimates and projections in one table, told apart by `variant`. The feed keeps
# them apart instead: the estimates are what its charts show, and the medium variant is the UN's
# own projection that the visualization's simulation is benchmarked against.
ESTIMATES_VARIANT = "estimates"
PROJECTION_VARIANT = "medium"

# Where one ends and the other begins. It is asserted rather than used to filter, so that a WPP
# revision moving the boundary is loud instead of silently reshaping the feed.
LAST_ESTIMATE_YEAR = 2023

# Entities `un_wpp` carries that the feed leaves out.
EXCLUDED_ENTITIES = [
    # OWID's own continents, aggregated over the same countries as the UN's (`Africa (UN)` and so
    # on) that the feed keeps. Listing both would offer a reader two spellings of one aggregate.
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
    # The UN's development groupings. They overlap each other and the income groups the feed keeps,
    # and three of them are defined by what they exclude, which reads as a data error in a picker.
    "Land-locked developing countries (LLDC)",
    "Least developed countries",
    "Less developed regions",
    "Less developed regions, excluding China",
    "Less developed regions, excluding least developed countries",
    "More developed regions",
    "Small island developing states (SIDS)",
    # The UN publishes a placeholder age structure for the Holy See rather than an estimate -- a
    # flat five residents in most bands, and 96 deaths across the whole of 1950-2100 -- so the
    # simulation the visualization runs on top of it would be nonsense.
    "Vatican",
]


def slugify(name: str) -> str:
    """The file name an entity's data is published under, e.g. `Africa (UN)` -> `africa-un`."""
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-zA-Z0-9]+", "-", ascii_name).strip("-").lower()


def read_measure(ds: Dataset, table_name: str, column: str, age_groups: list[str], sexes: list[str]) -> Table:
    """Read one `un_wpp` table down to the rows the feed is built from.

    Every measure the feed carries is a slice of a long table keyed on country, year, sex, age and
    variant, so all four are read the same way and differ only in which slice they take.
    """
    tb = ds.read(table_name, safe_types=False)
    tb = tb[
        tb["variant"].isin([ESTIMATES_VARIANT, PROJECTION_VARIANT]) & tb["sex"].isin(sexes) & tb["age"].isin(age_groups)
    ]
    # The dimensions come back as categoricals (`safe_types=False`), whose categories still list
    # every value of the full table; casting makes them plain strings to key the feed's JSON with.
    tb = tb.astype({"country": "string", "sex": "string", "age": "string", "variant": "string"})
    return tb[["country", "year", "sex", "age", "variant", column]]


def nest(tb: Table, column: str, cast: Callable[[Any], Any]) -> dict[tuple[str, str, str], dict[str, dict[str, Any]]]:
    """Index one measure as `{(entity, variant, sex): {year: {age group: value}}}`.

    Rows with no value are dropped rather than written as null: the visualization reads a missing
    key as "no data" (`row?.[ageGroup] || 0`), and its type has no room for a null.
    """
    tb = tb.dropna(subset=[column]).sort_values(["year", "age"], kind="stable")
    nested: dict[tuple[str, str, str], dict[str, dict[str, Any]]] = {}
    for entity, variant, sex, year, age, value in zip(
        tb["country"], tb["variant"], tb["sex"], tb["year"], tb["age"], tb[column]
    ):
        nested.setdefault((entity, variant, sex), {}).setdefault(str(year), {})[age] = cast(value)
    return nested


def nest_migration(tb: Table) -> dict[tuple[str, str], dict[str, dict[str, float]]]:
    """Index the migration rate as `{(entity, variant): {year: {"net_migration_rate": rate}}}`.

    It is the one measure with no age or sex dimension, and the visualization reads it out of a
    one-key object rather than as a bare number, so it is nested by hand rather than through `nest`.
    """
    tb = tb.dropna(subset=["net_migration_rate"]).sort_values("year", kind="stable")
    nested: dict[tuple[str, str], dict[str, dict[str, float]]] = {}
    for entity, variant, year, rate in zip(tb["country"], tb["variant"], tb["year"], tb["net_migration_rate"]):
        nested.setdefault((entity, variant), {})[str(year)] = {"net_migration_rate": float(rate)}
    return nested


def metadata_column(tb: Table, column: str) -> Variable:
    """The column `build_feed_metadata` reads a measure's origins off.

    Only the head of it: the metadata is the same on every row, and the one thing that reads the
    values is the type inference, which stringifies each of them for an answer the first thousand
    already give.
    """
    return tb[column].head(1000)


def sanity_check_inputs(tables: dict[str, Table], entities: list[str]) -> None:
    """Check the slices the feed is built from cover what the visualization expects of them."""
    for name, tb in tables.items():
        years = {variant: set(tb.loc[tb["variant"] == variant, "year"]) for variant in tb["variant"].unique()}
        assert max(years[ESTIMATES_VARIANT]) == LAST_ESTIMATE_YEAR, (
            f"`{name}` estimates now end in {max(years[ESTIMATES_VARIANT])}, not {LAST_ESTIMATE_YEAR}. The "
            "visualization splits history from projection at a year of its own (`HISTORICAL_END_YEAR` in "
            "owid-grapher's `bespoke/projects/demography`), so a revision that moves this one has to move that too."
        )
        assert min(years[PROJECTION_VARIANT]) == LAST_ESTIMATE_YEAR + 1, (
            f"`{name}` projections start in {min(years[PROJECTION_VARIANT])}, leaving a gap after {LAST_ESTIMATE_YEAR}."
        )
        assert not tb.duplicated(subset=["country", "year", "sex", "age", "variant"]).any(), (
            f"`{name}` has more than one row per country/year/sex/age/variant."
        )
        # An entity the index names but that carries none of a measure reaches a reader as an empty
        # chart with nothing to explain it, so the gaps are worth knowing about even when they are
        # the source's own (`Americas (UN)` has no migration rate).
        missing = sorted(set(entities) - set(tb["country"]))
        if missing:
            log.warning("demography.measure_missing_for_entities", measure=name, entities=missing)


def sanity_check_entity(entity: str, data: dict) -> None:
    """Check one entity's data before it is written."""
    latest = str(LAST_ESTIMATE_YEAR)
    for key in ("malePopulation", "femalePopulation"):
        assert data[key], f"{entity} has no {key}."
        missing = sorted(set(AGE_GROUPS) - set(data[key][latest]))
        assert not missing, f"{entity} is missing age groups in {key} for {latest}: {missing}"
    # Each pyramid is drawn from both sexes at once, so a year present in one and not the other
    # would draw half a pyramid rather than fail.
    assert set(data["malePopulation"]) == set(data["femalePopulation"]), (
        f"{entity} has different years of male and female population."
    )
    assert set(data["projection"]["male"]) == set(data["projection"]["female"]), (
        f"{entity} has different years of male and female projected population."
    )


def save_json(data: dict, filename: str) -> None:
    """Write one JSON file of the feed into the step's output folder."""
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    with open(paths.output_dir / filename, "w") as f:
        json.dump(data, f, separators=(",", ":"))


def run() -> None:
    #
    # Load data.
    #
    ds_garden = paths.load_dataset("un_wpp")
    tb_population = read_measure(ds_garden, "population", "population", AGE_GROUPS, ["male", "female"])
    tb_deaths = read_measure(ds_garden, "deaths", "deaths", AGE_GROUPS, ["male", "female"])
    tb_fertility = read_measure(ds_garden, "fertility_rate", "fertility_rate", FERTILITY_AGE_GROUPS, ["all"])
    tb_migration = read_measure(ds_garden, "migration", "net_migration_rate", ["all"], ["all"])

    entities = sorted(set(tb_population["country"]) - set(EXCLUDED_ENTITIES))
    # An entity renamed upstream would stop being excluded without anything noticing, and come back
    # into the feed under its new name.
    unknown = sorted(set(EXCLUDED_ENTITIES) - set(tb_population["country"]))
    assert not unknown, f"These entities are excluded from the feed but are no longer in `un_wpp`: {unknown}"

    sanity_check_inputs(
        {
            "population": tb_population,
            "deaths": tb_deaths,
            "fertility_rate": tb_fertility,
            "migration": tb_migration,
        },
        entities,
    )

    #
    # Build the feed.
    #
    # Population counts whole people; the rest are counts and rates the UN publishes as decimals.
    population = nest(tb_population, "population", int)
    deaths = nest(tb_deaths, "deaths", float)
    fertility = nest(tb_fertility, "fertility_rate", float)
    migration = nest_migration(tb_migration)

    # The feed's provenance, from the origins of the data it is built on, rather than a source
    # string typed in here that goes stale the next time the dataset is updated.
    write_feed_metadata(
        paths.output_dir,
        build_feed_metadata(
            title="Population, fertility, deaths and migration",
            columns={
                "Population": metadata_column(tb_population, "population"),
                "Fertility rate": metadata_column(tb_fertility, "fertility_rate"),
                "Deaths": metadata_column(tb_deaths, "deaths"),
                "Net migration rate": metadata_column(tb_migration, "net_migration_rate"),
            },
            update_period_days=ds_garden.metadata.update_period_days,
        ),
    )

    save_json({"countries": entities, "slugs": {entity: slugify(entity) for entity in entities}}, INDEX_FILENAME)

    for entity in tqdm(entities, desc="Writing entity files"):
        data = {
            "country": entity,
            "femalePopulation": population.get((entity, ESTIMATES_VARIANT, "female"), {}),
            "malePopulation": population.get((entity, ESTIMATES_VARIANT, "male"), {}),
            "fertility": fertility.get((entity, ESTIMATES_VARIANT, "all"), {}),
            "deaths": {
                "male": deaths.get((entity, ESTIMATES_VARIANT, "male"), {}),
                "female": deaths.get((entity, ESTIMATES_VARIANT, "female"), {}),
            },
            "migration": migration.get((entity, ESTIMATES_VARIANT), {}),
            "projection": {
                "male": population.get((entity, PROJECTION_VARIANT, "male"), {}),
                "female": population.get((entity, PROJECTION_VARIANT, "female"), {}),
            },
            "projectionScenario": {
                "fertility": fertility.get((entity, PROJECTION_VARIANT, "all"), {}),
                "deaths": {
                    "male": deaths.get((entity, PROJECTION_VARIANT, "male"), {}),
                    "female": deaths.get((entity, PROJECTION_VARIANT, "female"), {}),
                },
                "migration": migration.get((entity, PROJECTION_VARIANT), {}),
            },
        }
        sanity_check_entity(entity, data)
        save_json(data, f"demography.{slugify(entity)}.data.json")

    log.info("demography.written", n_files=len(entities) + 2, n_entities=len(entities), dest=str(paths.output_dir))

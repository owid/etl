"""Common logic of the food supply chain viz steps: the bespoke step `food_supply_chain_scl` (interactive waterfall) and
the static step `viz://static/faostat/2026-09-04/food_supply_chain_fbs` (waterfall of the world in 1968).

Each step reads whichever food supply chain garden dataset is its dependency in the DAG (`food_supply_chain_fbs` or
`food_supply_chain_scl`) and writes, in the shape of the other bespoke-visualization feeds (food trade, causes of
death):

  * one metadata JSON at `food-supply-chain.metadata.json`: the method, the sources, the year range, the stages of
    the chain in order (with a label and whether the bar adds to or takes from the chain), the units, the
    entity id-to-name mapping, and the feed's provenance derived from the garden columns (see `etl.viz.bespoke`);
  * one JSON per entity at `food-supply-chain.<entity_id>.json`, with the years and, for each unit (energy,
    protein, mass), one array per stage aligned with the years.

Reshaping only; all logic lives in the garden steps. Values are FAO's sign convention: stages listed with
"direction": "out" are magnitudes to subtract along the chain (a negative value there adds back), and the chain
lands exactly on "food".

The files go to the step's output folder. For the bespoke step, the framework syncs that folder to the R2 path of the
environment being built, so each feed is served at `<root>/v1/bespoke/faostat/latest/<step_name>/` --
`api.ourworldindata.org` on production, and `api-staging.owid.io/<env>` on a staging server or a laptop. The static
step's files stay local.
"""

import json

from tqdm.auto import tqdm

from etl.helpers import PathFinder
from etl.viz.bespoke import add_feed_metadata, build_feed_metadata

FILE_SLUG = "food-supply-chain"

# The two garden datasets a step can publish, and how the method is named in the metadata.
METHODS = {
    "food_supply_chain_fbs": "Food Balance Sheets",
    "food_supply_chain_scl": "Supply Utilization Accounts",
}
# The three tables of the garden dataset.
NUTRIENTS = ["energy", "protein", "mass"]
# Stages in chain order, with a display label and whether the bar adds to ("in") or takes from ("out") the chain.
# "food" is the total the chain lands on.
STAGES = [
    ("crop_production", "Crop production", "in"),
    ("imports", "Imports", "in"),
    ("exports", "Exports", "out"),
    ("stock_variation", "Stock change", "out"),
    ("seed", "Seed", "out"),
    ("losses", "Losses in the supply chain", "out"),
    ("other_uses", "Industrial and other non-food uses", "out"),
    ("processing_net", "Processing, net", "out"),
    ("feed", "Animal feed", "out"),
    ("animal_products", "Livestock, dairy, eggs and fish", "in"),
    ("tourist_consumption", "Tourist consumption", "out"),
    ("data_adjustments", "Data adjustments", "out"),
    ("food", "Food available to eat", "total"),
]
# Decimal places kept per unit.
NUM_DECIMALS = {"energy": 1, "protein": 2, "mass": 4}


def _save(paths: PathFinder, data: dict, filename: str) -> None:
    """Write one JSON file of the feed into the step's output folder."""
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    with open(paths.output_dir / filename, "w") as f:
        json.dump(data, f, separators=(",", ":"))


def build_feed(
    paths: PathFinder,
    countries: list[str] | None = None,
    years: list[int] | None = None,
    zero_stages_to_drop: list[str] | None = None,
) -> None:
    """Write the feed files; `countries` and `years`, if given, restrict them to those countries and years, and
    `zero_stages_to_drop` lists stages left out of the files, which must be zero in all of them."""
    #
    # Load inputs: whichever of the two garden datasets is the dependency.
    #
    dependencies = [d for d in paths.dependencies if d.split("/")[-1] in METHODS]
    assert len(dependencies) == 1, f"Expected exactly one food supply chain garden dependency, found {dependencies}."
    short_name = dependencies[0].split("/")[-1]
    ds = paths.load_dataset(short_name)
    tables = {nutrient: ds.read(nutrient) for nutrient in NUTRIENTS}
    for nutrient, tb in tables.items():
        if countries is not None:
            assert set(countries) <= set(tb["country"].astype(str)), f"Countries missing in {nutrient!r}: {countries}"
            tb = tb[tb["country"].astype(str).isin(countries)]
        if years is not None:
            assert set(years) <= set(tb["year"]), f"Years missing in {nutrient!r}: {years}"
            tb = tb[tb["year"].isin(years)]
        tables[nutrient] = tb.reset_index(drop=True)
        for key in zero_stages_to_drop or []:
            assert (tables[nutrient][key] == 0).all(), (
                f"Stage {key!r} is not zero in {nutrient!r}, so it can't be dropped."
            )

    stages = [stage for stage in STAGES if stage[0] not in (zero_stages_to_drop or [])]
    stage_keys = [key for key, _, _ in stages]
    for nutrient, tb in tables.items():
        assert set(stage_keys) <= set(tb.columns), (
            f"Table {nutrient!r} lacks stages: {set(stage_keys) - set(tb.columns)}"
        )
    reference = tables["energy"]

    # The feed's provenance, derived from the garden columns it is built from.
    feed_metadata = build_feed_metadata(
        title=f"Food supply chain ({METHODS[short_name]} method)",
        columns={
            f"{name} ({tables[nutrient][key].metadata.short_unit})": tables[nutrient][key]
            for nutrient in NUTRIENTS
            for key, name, _ in stages
        },
        update_period_days=ds.metadata.update_period_days,
    )

    #
    # Metadata: entities get 1-based alphabetical ids, as in the other bespoke exports.
    #
    entities = sorted(set(reference["country"].astype(str)))
    entity_to_id = {name: i + 1 for i, name in enumerate(entities)}
    metadata = {
        "method": METHODS[short_name],
        # Every origin behind the chain (FAO's balances, OWID's population), not only the first.
        "sources": sorted({origin.attribution for origin in reference["food"].metadata.origins if origin.attribution}),
        "timeRange": {"start": int(reference["year"].min()), "end": int(reference["year"].max())},
        "units": {nutrient: tables[nutrient]["food"].metadata.unit for nutrient in NUTRIENTS},
        "stages": [{"key": key, "name": name, "direction": direction} for key, name, direction in stages],
        "dimensions": {"entities": [{"id": entity_to_id[name], "name": name} for name in entities]},
    }
    _save(paths, add_feed_metadata(metadata, feed_metadata), f"{FILE_SLUG}.metadata.json")

    #
    # One file per entity: years, then for each unit one array per stage aligned with the years.
    #
    for name in tqdm(entities, desc=f"{paths.short_name} per-entity JSON"):
        years = None
        data = {}
        for nutrient in NUTRIENTS:
            rows = tables[nutrient][tables[nutrient]["country"].astype(str) == name].sort_values("year")
            if years is None:
                years = rows["year"].astype(int).tolist()
            assert rows["year"].astype(int).tolist() == years, f"Tables have different years for {name}."
            data[nutrient] = {
                key: [None if v != v else v for v in rows[key].astype(float).round(NUM_DECIMALS[nutrient])]
                for key in stage_keys
            }
        _save(paths, {"years": years, **data}, f"{FILE_SLUG}.{entity_to_id[name]}.json")

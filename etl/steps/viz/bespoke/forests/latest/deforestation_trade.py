"""Bespoke viz step writing the JSON files read by the deforestation-trade sankey.

  * `metadata.json`: provenance derived from the garden metadata (see `etl.viz.bespoke`).
  * `deforestation-trade.metadata.json`: years, entities, commodity groups, and world totals per year.
  * `deforestation-trade.<entityId>.json`: one per entity, with an `imports` block (flows consumed
    by the entity; partners are the producing countries) and an `exports` block (flows produced by
    the entity; partners are the consuming countries). Each block has parallel arrays `partners`,
    `groups` and `values`, where `values[i]` is aligned to `years` and `null` means no data.
    Domestic flows appear in both blocks.

Only hectares are published; the sankey does not show emissions.
"""

import json

import pandas as pd

from etl.helpers import PathFinder
from etl.viz.bespoke import build_feed_metadata, write_feed_metadata

paths = PathFinder(__file__)

FILE_SLUG = "deforestation-trade"
NUM_DECIMALS = 1


def build_block(flows: pd.DataFrame, years: list[int]) -> dict:
    wide = flows.pivot(index=["partner", "group"], columns="year", values="value").reindex(columns=years)
    wide = wide.loc[wide.sum(axis=1).sort_values(ascending=False).index].round(NUM_DECIMALS)
    return {
        "partners": [int(partner) for partner, _ in wide.index],
        "groups": [int(group) for _, group in wide.index],
        "values": [[None if pd.isna(v) else float(v) for v in row] for row in wide.itertuples(index=False)],
    }


def save(data: dict, filename: str) -> None:
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    with open(paths.output_dir / filename, "w") as f:
        json.dump(data, f, separators=(",", ":"), ensure_ascii=False)


def run() -> None:
    #
    # Load inputs.
    #
    ds = paths.load_dataset("deforestation_embedded_in_trade")
    tb = ds.read("deforestation_embedded_in_trade")

    metadata = build_feed_metadata(
        title="Deforestation embedded in trade",
        columns={"Deforestation risk embedded in trade": tb["deforestation_risk"]},
        update_period_days=ds.metadata.update_period_days,
    )
    write_feed_metadata(paths.output_dir, metadata)

    #
    # Assign 1-based alphabetical ids to entities and commodity groups.
    #
    countries = sorted(set(tb["producer_country"]) | set(tb["consumer_country"]))
    entity_id = {name: i + 1 for i, name in enumerate(countries)}

    groups = sorted(tb["commodity_group"].unique())
    group_id = {name: i + 1 for i, name in enumerate(groups)}

    years = sorted(int(y) for y in tb["year"].unique())

    flows = pd.DataFrame(
        {
            "producer": tb["producer_country"].map(entity_id),
            "consumer": tb["consumer_country"].map(entity_id),
            "group": tb["commodity_group"].map(group_id),
            "year": tb["year"],
            "value": tb["deforestation_risk"],
        }
    )

    #
    # Write the metadata JSON and one file per entity.
    #
    save(
        {
            "timeRange": {"start": years[0], "end": years[-1]},
            "years": years,
            "source": metadata["feed"]["citation"],
            "dimensions": {
                "entities": [{"id": entity_id[c], "name": c} for c in countries],
                "commodityGroups": [{"id": group_id[g], "name": g} for g in groups],
            },
            "worldTotals": [round(float(v), NUM_DECIMALS) for v in flows.groupby("year")["value"].sum().reindex(years)],
        },
        f"{FILE_SLUG}.metadata.json",
    )

    for entity in entity_id.values():
        imports = flows[flows["consumer"] == entity].rename(columns={"producer": "partner"})
        exports = flows[flows["producer"] == entity].rename(columns={"consumer": "partner"})
        save(
            {"imports": build_block(imports, years), "exports": build_block(exports, years)},
            f"{FILE_SLUG}.{entity}.json",
        )

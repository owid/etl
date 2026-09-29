"""Bespoke viz step producing the JSON files read by the deforestation-trade sankey.

Loads the `deforestation_embedded_in_trade` garden dataset and writes:

  * `metadata.json`, the provenance of the bespoke viz, derived from the garden columns (see `etl.viz.bespoke`);
  * `deforestation-trade.metadata.json`, the manifest: the years, the entities (with the source's
    ISO code and region), the commodity groups, and the worldwide hectares per year;
  * `deforestation-trade.<entityId>.json`, one file per entity, with an `imports` block (flows
    consumed by the entity, partners are the producing countries) and an `exports` block (flows
    produced by the entity, partners are the consuming countries). Each block holds parallel
    arrays `partners`, `groups` and `values`, where `values[i]` is aligned to the manifest's
    `years` and `null` means no data. Domestic flows appear in both blocks. Rows are sorted by
    their total over all years, largest first.

Only hectares are published; the sankey does not show emissions.

The files go to the step's output folder; the framework syncs that folder to the R2 path of the
environment being built, so the files are served at
`<root>/v1/bespoke/forests/latest/deforestation_trade/deforestation-trade.metadata.json` and
`.../deforestation-trade.<entityId>.json` -- `api.ourworldindata.org` on production, and
`api-staging.owid.io/<env>` on a staging server or a laptop.
"""

import json

import pandas as pd
from structlog import get_logger

from etl.helpers import PathFinder
from etl.viz.bespoke import build_feed_metadata, write_feed_metadata

log = get_logger()
paths = PathFinder(__file__)

FILE_SLUG = "deforestation-trade"

# Decimal places kept for hectare values written to the JSON files.
NUM_DECIMALS = 1


def _build_block(rows: pd.DataFrame, years: list[int]) -> dict:
    """A block of flows as parallel arrays, sorted by total over all years, largest first.

    Flows whose hectares round to zero in every year are dropped: they would only add rows the
    sankey cannot draw.
    """
    wide = rows.pivot_table(index=["partner", "group"], columns="year", values="value", aggfunc="sum")
    wide = wide.reindex(columns=years).round(NUM_DECIMALS)
    wide = wide[(wide.fillna(0) != 0).any(axis=1)]
    wide = wide.loc[wide.sum(axis=1).sort_values(ascending=False).index]
    return {
        "partners": [int(partner) for partner, _ in wide.index],
        "groups": [int(group) for _, group in wide.index],
        "values": [[None if pd.isna(v) else float(v) for v in row] for row in wide.itertuples(index=False)],
    }


def _save(data: dict, filename: str) -> None:
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    with open(paths.output_dir / filename, "w") as f:
        json.dump(data, f, separators=(",", ":"), ensure_ascii=False)


def run() -> None:
    #
    # Load inputs.
    #
    ds = paths.load_dataset("deforestation_embedded_in_trade")
    tb = ds.read("deforestation_embedded_in_trade", safe_types=False)
    tb_countries = ds.read("countries", safe_types=False)

    viz_metadata = build_feed_metadata(
        title="Deforestation embedded in trade",
        columns={"Deforestation risk embedded in trade": tb["deforestation_risk"]},
        update_period_days=ds.metadata.update_period_days,
    )
    write_feed_metadata(paths.output_dir, viz_metadata)

    df = pd.DataFrame(tb)[["producer_country", "consumer_country", "commodity_group", "year", "deforestation_risk"]]
    for col in ("producer_country", "consumer_country", "commodity_group"):
        df[col] = df[col].astype(str)
    df = df.rename(columns={"deforestation_risk": "value"})

    #
    # Build id mappings: 1-based alphabetical ids for entities and commodity groups.
    #
    countries = sorted(tb_countries["country"].astype(str))
    assert set(df["producer_country"]) | set(df["consumer_country"]) <= set(countries)
    entity_to_id = {name: i + 1 for i, name in enumerate(countries)}
    groups = sorted(df["commodity_group"].unique())
    group_to_id = {name: i + 1 for i, name in enumerate(groups)}
    years = sorted(int(y) for y in df["year"].unique())

    df["producer"] = df["producer_country"].map(entity_to_id)
    df["consumer"] = df["consumer_country"].map(entity_to_id)
    df["group"] = df["commodity_group"].map(group_to_id)

    #
    # Write the manifest.
    #
    country_info = tb_countries.set_index("country")
    world_totals = df.groupby("year")["value"].sum().reindex(years)
    manifest = {
        "timeRange": {"start": years[0], "end": years[-1]},
        "years": years,
        "source": viz_metadata["feed"]["citation"],
        "dimensions": {
            "entities": [
                {
                    "id": entity_to_id[c],
                    "name": c,
                    "iso": str(country_info.loc[c, "iso_code"]),
                    "region": str(country_info.loc[c, "region"]),
                }
                for c in countries
            ],
            "commodityGroups": [{"id": group_to_id[g], "name": g} for g in groups],
        },
        "worldTotals": [round(float(v), NUM_DECIMALS) for v in world_totals],
    }
    log.info("deforestation_trade.write_manifest", n_entities=len(countries), n_groups=len(groups), n_years=len(years))
    _save(manifest, f"{FILE_SLUG}.metadata.json")

    #
    # Write one file per entity. Domestic flows (producer == consumer) land in both blocks.
    #
    imports = df.rename(columns={"producer": "partner"})[["consumer", "partner", "group", "year", "value"]]
    exports = df.rename(columns={"consumer": "partner"})[["producer", "partner", "group", "year", "value"]]
    imports_by_entity = {k: v for k, v in imports.groupby("consumer")}
    exports_by_entity = {k: v for k, v in exports.groupby("producer")}
    empty = imports.iloc[0:0]
    log.info("deforestation_trade.write_per_entity", n_files=len(countries))
    for country in countries:
        entity_id = entity_to_id[country]
        data = {
            "imports": _build_block(imports_by_entity.get(entity_id, empty), years),
            "exports": _build_block(exports_by_entity.get(entity_id, empty), years),
        }
        _save(data, f"{FILE_SLUG}.{entity_id}.json")

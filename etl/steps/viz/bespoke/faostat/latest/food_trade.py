"""Bespoke viz step producing the JSON feed for the FAOSTAT food-trade Sankey viz.

Loads the `food_trade` garden table and writes two kinds of files:

  * one metadata JSON at `food-trade.metadata.json` listing the year, source,
    the entity/product id-to-name mappings, and a `productsByEntity` map
    that tells the viz which (entity, product) combinations have any data;
  * one per-product JSON at `food-trade.<product_id>.json` carrying that
    product's `flows` (every (exporter, importer, value) triple in the data
    for that item).

The product-keyed split naturally powers "Global trade of item X" views in
the viz: pick a product, fetch one JSON, render. The metadata's
`productsByEntity` map lets the viz pre-compute "what can this country be
the exporter / importer of?" without loading every product file.

It also writes `metadata.json`, the feed's provenance derived from the garden columns (see
`etl.viz.bespoke`).

The files go to the step's output folder; the framework syncs that folder to the R2 path of the
environment being built, so the feed is served at
`<root>/v1/bespoke/faostat/latest/food_trade/food-trade.metadata.json` and
`.../food-trade.<product_id>.json` -- `api.ourworldindata.org` on production, and
`api-staging.owid.io/<env>` on a staging server or a laptop.
"""

import json

import pandas as pd
from structlog import get_logger
from tqdm.auto import tqdm

from etl.helpers import PathFinder
from etl.viz.bespoke import build_feed_metadata, write_feed_metadata

log = get_logger()
paths = PathFinder(__file__)

FILE_SLUG = "food-trade"

# Decimal places kept for tonnage values written to the JSON files. Trade
# quantities are large, so 3 decimals is far below any meaningful precision
# while keeping the files compact.
NUM_DECIMALS = 3


def _build_product_data(df: pd.DataFrame, product: str, entity_to_id: dict) -> dict:
    """Build the per-product JSON.

    Schema:
        {
          "flows": {"exporters": [<entity_id>],
                    "importers": [<entity_id>],
                    "values":    [<tonnes>]}
        }
    """
    rows = df[df["item"] == product]

    return {
        "flows": {
            "exporters": [entity_to_id[e] for e in rows["exporter"]],
            "importers": [entity_to_id[e] for e in rows["importer"]],
            "values": rows["value"].astype(float).round(NUM_DECIMALS).tolist(),
        }
    }


def _build_products_by_entity(df: pd.DataFrame, entity_to_id: dict, product_to_id: dict) -> dict:
    """Build {entity_id_str: [sorted product_ids]} listing every product the
    entity trades, whether as exporter or importer.

    Keys are stringified ints because JSON object keys must be strings.
    """
    out = {}
    exp = df.groupby("exporter", observed=True)["item"].apply(lambda s: set(s))
    imp = df.groupby("importer", observed=True)["item"].apply(lambda s: set(s))
    all_entities = set(exp.index) | set(imp.index)
    for ent in all_entities:
        items = exp.get(ent, set()) | imp.get(ent, set())
        out[str(entity_to_id[ent])] = sorted(product_to_id[i] for i in items)
    return out


def _save(data: dict, filename: str) -> None:
    """Write one JSON file of the feed into the step's output folder."""
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    with open(paths.output_dir / filename, "w") as f:
        json.dump(data, f, separators=(",", ":"))


def run() -> None:
    #
    # Load inputs.
    #
    ds = paths.load_dataset("food_trade")
    tb = ds.read("food_trade", safe_types=False)

    # The feed's provenance, derived from the origins of the data it is built on, in the same
    # shape the MDIM download packages publish.
    feed_metadata = build_feed_metadata(
        title="Food trade",
        columns={"Bilateral trade flow": tb["value"]},
        update_period_days=ds.metadata.update_period_days,
    )
    write_feed_metadata(paths.output_dir, feed_metadata)

    df = pd.DataFrame(tb)
    for col in ("exporter", "importer", "item"):
        df[col] = df[col].astype(str)

    # Year comes from the data itself (the garden step exports a single year).
    years = df["year"].unique()
    assert len(years) == 1, f"Expected a single year in the food_trade table, found {sorted(years)}."
    year = int(years[0])

    #
    # Build id mappings.
    # - Entities: no canonical external id (FAO uses country names), so we
    #   assign 1-based alphabetical ids — matches causes-of-death / migration.
    # - Products: use the item ids the garden step carries in the data. For most
    #   items this is the canonical FAO item code (stable across FAOSTAT releases
    #   and shared with QCL and TM), so the per-product URL `food-trade.<id>.json`
    #   is externally recognisable. Items that combine several codes use
    #   100000 + their first code, an out-of-range integer that signals the id is
    #   not a single FAO commodity (see the garden step).
    #
    countries = sorted(set(df["exporter"]) | set(df["importer"]))
    entity_to_id = {name: i + 1 for i, name in enumerate(countries)}

    product_to_id = {
        item: int(code) for item, code in df[["item", "item_code"]].drop_duplicates().itertuples(index=False)
    }
    products = sorted(df["item"].unique())

    #
    # Write metadata.
    #
    metadata = {
        "year": year,
        "source": feed_metadata["feed"]["citation"],
        "dimensions": {
            "entities": [{"id": entity_to_id[c], "name": c} for c in countries],
            "products": [{"id": product_to_id[p], "name": p} for p in products],
        },
        "productsByEntity": _build_products_by_entity(df, entity_to_id, product_to_id),
    }
    log.info("food_trade.write_metadata", n_entities=len(countries), n_products=len(products))
    _save(metadata, f"{FILE_SLUG}.metadata.json")

    #
    # Write one file per product.
    #
    log.info("food_trade.write_per_product", n_files=len(products))
    for product in tqdm(products, desc="food_trade per-product JSON"):
        data = _build_product_data(df, product, entity_to_id)
        _save(data, f"{FILE_SLUG}.{product_to_id[product]}.json")

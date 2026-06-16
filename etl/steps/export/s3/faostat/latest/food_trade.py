"""S3 JSON export step for the FAOSTAT food-trade Sankey viz.

Loads the `food_trade` garden table and writes two kinds of files:

  * one metadata JSON at `food-trade.metadata.json` listing the year, source,
    the entity/product id-to-name mappings, and a `productsByEntity` map
    that tells the viz which (entity, product) combinations have any data;
  * one per-product JSON at `food-trade.<product_id>.json` carrying that
    product's `flows` (every (exporter, importer, value) triple in the data
    for that item), plus `production` and `supply` per entity.

The product-keyed split naturally powers "Global trade of item X" views in
the viz: pick a product, fetch one JSON, render. The metadata's
`productsByEntity` map lets the viz pre-compute "what can this country be
the exporter / importer of?" without loading every product file.

Output URLs:
    https://owid-public.owid.io/data/food-trade/food-trade.metadata.json
    https://owid-public.owid.io/data/food-trade/food-trade.<product_id>.json

Layout mirrors the causes-of-death JSON pattern
(`etl/steps/export/s3/ihme_gbd/latest/gbd_treemap_json.py`).
"""

import json
from pathlib import Path

import pandas as pd
import yaml
from owid.catalog import s3_utils
from structlog import get_logger
from tqdm.auto import tqdm

from etl.config import DRY_RUN
from etl.helpers import PathFinder
from etl.paths import EXPORT_DIR, STEPS_GARDEN_DIR

log = get_logger()
paths = PathFinder(__file__)

# Public S3 bucket and prefix.
S3_BUCKET_NAME = "owid-public"
S3_DATA_DIR = Path("data/food-trade")
FILE_SLUG = "food-trade"

# Source of the (item name -> FAO item code) mapping. Same yaml the garden
# step uses, so the product ids in this export are the canonical FAOSTAT codes
# (e.g. 15 = Wheat, 56 = Maize (corn), 236 = Soya beans). Stable across
# FAOSTAT releases and shared with QCL and TM.
ITEMS_YAML_PATH = STEPS_GARDEN_DIR / "faostat/2026-05-07/food_trade.items.yaml"


def _build_product_data(df: pd.DataFrame, product: str, entity_to_id: dict) -> dict:
    """Build the per-product JSON.

    Schema:
        {
          "flows":      {"exporters": [<entity_id>],
                         "importers": [<entity_id>],
                         "values":    [<tonnes>]},
          "production": {"entities":  [<entity_id>], "values": [<tonnes>]},
          "supply":     {"entities":  [<entity_id>], "values": [<tonnes>]}
        }

    `production` / `supply` list only the entities that have a value for
    this product — NaN entries are dropped so the consumer never has to
    handle JSON `null`.
    """
    rows = df[df["item"] == product]

    out: dict = {
        "flows": {
            "exporters": [entity_to_id[e] for e in rows["exporter"]],
            "importers": [entity_to_id[e] for e in rows["importer"]],
            "values": rows["value"].astype(float).round(3).tolist(),
        }
    }

    # Production: per-exporter for this product. Dedupe by exporter (same
    # value repeats across all rows that share the exporter), drop NaN.
    prod = rows[["exporter", "exporter_production"]].dropna().drop_duplicates("exporter").sort_values("exporter")
    out["production"] = {
        "entities": [entity_to_id[e] for e in prod["exporter"]],
        "values": prod["exporter_production"].astype(float).round(3).tolist(),
    }

    # Supply: per-importer for this product. Dedupe by importer, drop NaN.
    sup = rows[["importer", "importer_supply"]].dropna().drop_duplicates("importer").sort_values("importer")
    out["supply"] = {
        "entities": [entity_to_id[e] for e in sup["importer"]],
        "values": sup["importer_supply"].astype(float).round(3).tolist(),
    }

    return out


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


def _save_and_upload(data: dict, filename: str) -> None:
    """Write JSON locally and upload to S3 (skipping the upload under DRY_RUN)."""
    export_dir = EXPORT_DIR / paths.channel / paths.namespace / paths.version / paths.short_name
    export_dir.mkdir(parents=True, exist_ok=True)
    local_file = export_dir / filename
    s3_path = f"s3://{S3_BUCKET_NAME}/{S3_DATA_DIR / filename}"

    with open(local_file, "w") as f:
        json.dump(data, f, separators=(",", ":"))

    if DRY_RUN:
        tqdm.write(f"[DRY RUN] Would upload {local_file} -> {s3_path}")
    else:
        s3_utils.upload(s3_path, local_file, public=True, downloadable=True)


def run() -> None:
    #
    # Load inputs.
    #
    ds = paths.load_dataset("food_trade")
    tb = ds.read("food_trade", safe_types=False)

    # Source attribution for the metadata JSON is read from the `value`
    # column's origin (TM snapshot) — the default grapher "producer (year)"
    # form — so it stays in sync with the dataset's metadata.
    source = tb["value"].metadata.origins[0].attribution

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
    # - Products: use the canonical FAO item codes from the yaml. These are
    #   stable across FAOSTAT releases and shared with QCL and TM, so the
    #   per-product URLs `food-trade.<item_code>.json` are externally
    #   recognisable.
    #
    countries = sorted(set(df["exporter"]) | set(df["importer"]))
    entity_to_id = {name: i + 1 for i, name in enumerate(countries)}

    with open(ITEMS_YAML_PATH) as f:
        items_config = yaml.safe_load(f)
    product_to_id = {entry["display"]: int(entry["item_code"]) for entry in items_config["items"]}
    products = sorted(df["item"].unique())
    missing = set(products) - set(product_to_id)
    assert not missing, f"items in the data are not in items.yaml: {sorted(missing)}"

    #
    # Write metadata.
    #
    metadata = {
        "year": year,
        "source": source,
        "dimensions": {
            "entities": [{"id": entity_to_id[c], "name": c} for c in countries],
            "products": [{"id": product_to_id[p], "name": p} for p in products],
        },
        "productsByEntity": _build_products_by_entity(df, entity_to_id, product_to_id),
    }
    log.info("food_trade.write_metadata", n_entities=len(countries), n_products=len(products))
    _save_and_upload(metadata, f"{FILE_SLUG}.metadata.json")

    #
    # Write one file per product.
    #
    log.info("food_trade.write_per_product", n_files=len(products))
    for product in tqdm(products, desc="food_trade per-product JSON"):
        data = _build_product_data(df, product, entity_to_id)
        _save_and_upload(data, f"{FILE_SLUG}.{product_to_id[product]}.json")

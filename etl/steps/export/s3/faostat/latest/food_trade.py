"""S3 JSON export step for the FAOSTAT food-trade Sankey viz.

Loads the `food_trade` garden table and writes two kinds of files:

  * one metadata JSON at `food-trade.metadata.json` listing the year, source
    and the entity/product id-to-name mappings used in the per-country files;
  * one per-country JSON at `food-trade.{entity_id}.json` carrying that
    country's `exports`, `imports`, `production` and `supply` arrays.

The split lets the viz lazy-load only the country the user has selected,
rather than the full dataset upfront. The layout mirrors the causes-of-death
JSON used by other OWID custom visualisations (see
`etl/steps/export/s3/ihme_gbd/latest/gbd_treemap_json.py`).

Output URLs:
    https://owid-public.owid.io/data/food-trade/food-trade.metadata.json
    https://owid-public.owid.io/data/food-trade/food-trade.<entity_id>.json

Schema of each file is documented inline below.
"""

import json
from pathlib import Path

import pandas as pd
from owid.catalog import s3_utils
from structlog import get_logger
from tqdm.auto import tqdm

from etl.config import DRY_RUN
from etl.helpers import PathFinder
from etl.paths import EXPORT_DIR

log = get_logger()
paths = PathFinder(__file__)

# Public S3 bucket and prefix. Matches the directory layout used by other
# OWID custom-viz JSON exports (causes-of-death, migration-stock-flows).
S3_BUCKET_NAME = "owid-public"
S3_DATA_DIR = Path("data/food-trade")
FILE_SLUG = "food-trade"

# Hard-coded to match the YEAR constant in the garden step. If/when the
# garden step bumps its YEAR, bump this too (and re-run).
YEAR = 2023
SOURCE = "FAO, Detailed Trade Matrix (2026)"
LICENSE = "CC BY-NC-SA 3.0 IGO"


def _build_entity_data(df: pd.DataFrame, country: str, entity_to_id: dict, product_to_id: dict) -> dict:
    """Build the per-country JSON.

    Schema:
        {
          "exports":    {"partners": [<id>], "products": [<id>], "values": [<float>]},
          "imports":    {"partners": [<id>], "products": [<id>], "values": [<float>]},
          "production": {"products": [<id>], "values": [<float>]},
          "supply":     {"products": [<id>], "values": [<float>]}
        }

    `exports.partners[i]` is the *importer* entity-id for the i-th outbound
    flow; `imports.partners[i]` is the *exporter* entity-id for the i-th
    inbound flow.

    `production` and `supply` list only products where QCL Production /
    apparent-supply has a value — NaN entries are omitted so the consumer
    never has to handle JSON `null`.
    """
    exports = df[df["exporter"] == country]
    imports = df[df["importer"] == country]

    out: dict = {
        "exports": {
            "partners": [entity_to_id[p] for p in exports["importer"]],
            "products": [product_to_id[p] for p in exports["item"]],
            "values": exports["value"].astype(float).round(3).tolist(),
        },
        "imports": {
            "partners": [entity_to_id[p] for p in imports["exporter"]],
            "products": [product_to_id[p] for p in imports["item"]],
            "values": imports["value"].astype(float).round(3).tolist(),
        },
    }

    # Production: per-item value for this country as an exporter. Repeats across
    # all rows with the same (exporter, item); dedupe and drop NaN.
    prod = exports[["item", "exporter_production"]].dropna().drop_duplicates("item").sort_values("item")
    out["production"] = {
        "products": [product_to_id[p] for p in prod["item"]],
        "values": prod["exporter_production"].astype(float).round(3).tolist(),
    }

    # Supply: per-item value for this country as an importer.
    sup = imports[["item", "importer_supply"]].dropna().drop_duplicates("item").sort_values("item")
    out["supply"] = {
        "products": [product_to_id[p] for p in sup["item"]],
        "values": sup["importer_supply"].astype(float).round(3).tolist(),
    }

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
    df = pd.DataFrame(tb)
    for col in ("exporter", "importer", "item"):
        df[col] = df[col].astype(str)

    #
    # Build id mappings. Entities sorted alphabetically by name; same for
    # products. 1-based ids match the causes-of-death / migration convention.
    #
    countries = sorted(set(df["exporter"]) | set(df["importer"]))
    products = sorted(df["item"].unique())
    entity_to_id = {name: i + 1 for i, name in enumerate(countries)}
    product_to_id = {name: i + 1 for i, name in enumerate(products)}

    #
    # Write metadata.
    #
    metadata = {
        "year": YEAR,
        "source": SOURCE,
        "license": LICENSE,
        "dimensions": {
            "entities": [{"id": entity_to_id[c], "name": c} for c in countries],
            "products": [{"id": product_to_id[p], "name": p} for p in products],
        },
    }
    log.info("food_trade.write_metadata", n_entities=len(countries), n_products=len(products))
    _save_and_upload(metadata, f"{FILE_SLUG}.metadata.json")

    #
    # Write one file per country.
    #
    log.info("food_trade.write_per_country", n_files=len(countries))
    for country in tqdm(countries, desc="food_trade per-country JSON"):
        data = _build_entity_data(df, country, entity_to_id, product_to_id)
        _save_and_upload(data, f"{FILE_SLUG}.{entity_to_id[country]}.json")

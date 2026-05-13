"""S3 export step for the FAOSTAT food-trade Sankey viz.

Produces a slim CSV of bilateral trade flows for the latest well-covered year
in tonnes, matching the exact schema fetched by the bespoke `food-trade`
project in owid-grapher (see `bespoke/projects/food-trade/src/data.ts`).

Schema (one row per directional A → B flow for each item):
    Exporter (str)   — country that exports the goods
    Importer (str)   — country that imports the goods
    Item     (str)   — viz-display item (see food_trade.items.yaml)
    Value    (float) — quantity in tonnes

The display items shown in the dropdown are curated in `food_trade.items.yaml`.
Each entry there names an FBS item (commodity or group total); the actual list
of TM items that roll up under it is **auto-derived** from FAOSTAT's metadata:
each FBS commodity carries a "Default composition" description that lists the
FAO item codes it aggregates, and those same FAO codes appear as `item_code`
in the TM data. So the YAML stays small (just the dropdown definition) and
the rollup is code-based and authoritative.

TM items that don't belong to any FBS commodity (industrial / non-food:
Cigarettes, Cotton, "Food preparations n.e.c.", etc.) are dropped automatically.

For each (Exporter, Importer, Item) the FAOSTAT detailed trade matrix
typically has two reports (one from each side) that can disagree. We pick
the **importer-reported** quantity by default (FAOSTAT convention — import
figures are more thoroughly tracked at customs) and fall back to the
exporter-reported quantity when the importer didn't report.

Output:
    https://owid-public.owid.io/food-trade/trade.csv
"""

import json
import re
import tempfile
from pathlib import Path

import pandas as pd
import structlog
import yaml
from owid.catalog import Table, s3_utils

from etl.config import DRY_RUN
from etl.helpers import PathFinder

log = structlog.get_logger()

# Public S3 bucket and prefix. The viz fetches
# https://owid-public.owid.io/food-trade/trade.csv?nocache, so the file must
# land at `s3://owid-public/food-trade/trade.csv`.
S3_BUCKET_NAME = "owid-public"
S3_DATA_DIR = Path("food-trade")
S3_FILENAME = "trade.csv"

# Year exported by the Sankey viz. Hard-coded on purpose: when FAOSTAT
# publishes a new release, the assertion below will fail and force a
# deliberate bump rather than silently shifting the exported slice forward.
YEAR = 2023

# Sibling config files. Names start with "food_trade" so ETL change-detection
# picks them up automatically (see etl/steps/__init__.py:_step_files).
ITEMS_CONFIG_PATH = Path(__file__).parent / "food_trade.items.yaml"

# Regex that pulls every FAO item code out of an FBS item's "Default composition"
# description. The description format is e.g.
#   "Default composition: 15 Wheat, 16 Flour, wheat, 17 Bran, wheat, ..."
# so we just grab every integer that is followed by a name (letter).
_FAO_CODE_IN_DESCRIPTION = re.compile(r"\b(\d+)\s+[A-Za-z]")

paths = PathFinder(__file__)


# ─────────────────────────────────────────────────────────────────────────────
# FAOSTAT-metadata-driven rollup derivation
# ─────────────────────────────────────────────────────────────────────────────


# FBS item codes ≥ this are group totals (e.g. 2905 "Cereals - Excluding Beer").
# Codes below this are commodities (e.g. 2511 "Wheat and products"). The
# distinction matters only because group rollups have to be assembled via the
# itemgroup table, whereas commodity rollups come straight from each
# commodity's own "Default composition" description.
_FBS_GROUP_CODE_MIN = 2900


def _load_fbs_rollup_maps() -> tuple[dict[int, set[int]], dict[int, str]]:
    """Parse the slim FBS-metadata snapshot and build:

    * fbs_code_to_fao_codes — keyed by FBS item code (works for both
      commodities and group totals). For commodities (code < 2900) the FAO
      codes are taken from the commodity's "Default composition" description
      (e.g. 2511 "Wheat and products" → {15, 16, 17, ...}). For group totals
      (code ≥ 2900) it is the union of FAO codes from all constituent
      commodities listed in the FBS itemgroup table.
    * fbs_code_to_name — keyed by FBS item code, returns the current FBS
      item name. Used only for log lines and error messages; not for joins."""
    snap = paths.load_snapshot("faostat_fbs_metadata.json")
    with open(snap.path) as f:
        meta = json.load(f)

    fbs_code_to_name: dict[int, str] = {}
    commodity_codes: dict[int, set[int]] = {}
    for entry in meta["item"]["data"]:
        code = int(entry["Item Code"])
        fbs_code_to_name[code] = entry["Item"]
        if code >= _FBS_GROUP_CODE_MIN:
            continue
        desc = entry.get("Description") or ""
        codes = {int(c) for c in _FAO_CODE_IN_DESCRIPTION.findall(desc)}
        if codes:
            commodity_codes[code] = codes

    group_codes: dict[int, set[int]] = {}
    for entry in meta["itemgroup"]["data"]:
        group_name = entry["Item Group"]
        if group_name == "Grand Total":
            continue
        group_code = int(entry["Item Group Code"])
        member_code = int(entry["Item Code"])
        fbs_code_to_name.setdefault(group_code, group_name)
        group_codes.setdefault(group_code, set()).update(commodity_codes.get(member_code, set()))

    fbs_code_to_fao_codes = {**commodity_codes, **group_codes}
    return fbs_code_to_fao_codes, fbs_code_to_name


def _is_group_code(fbs_item_code: int) -> bool:
    return fbs_item_code >= _FBS_GROUP_CODE_MIN


# ─────────────────────────────────────────────────────────────────────────────
# YAML config loading & validation
# ─────────────────────────────────────────────────────────────────────────────


def _load_items_config() -> dict:
    """Load the curated items config from food_trade.items.yaml."""
    with open(ITEMS_CONFIG_PATH) as f:
        return yaml.safe_load(f)


def _sanity_check_items_config(
    config: dict,
    fbs_code_to_fao_codes: dict[int, set[int]],
    fbs_code_to_name: dict[int, str],
    tm_codes_in_data: set[int],
) -> None:
    """Validate the items config structurally and against FAOSTAT metadata.

    Checks:
      1. Required top-level shape: a single 'items' list.
      2. Each items entry has required fields (display, fbs_item_code).
      3. display names are unique.
      4. Each fbs_item_code exists in FBS metadata (catches typos / removals).
      5. The auto-derived rollup maps to at least one item present in the TM
         snapshot (catches FBS items that have no TM coverage).
    """
    assert isinstance(config, dict) and "items" in config, "items config must be a mapping with an 'items' key"
    items = config["items"]
    assert isinstance(items, list) and items, "config['items'] must be a non-empty list"

    required = {"display", "fbs_item_code"}
    for entry in items:
        missing = required - entry.keys()
        assert not missing, f"items entry missing keys {sorted(missing)}: {entry}"

    displays = [e["display"] for e in items]
    dupes = sorted({d for d in displays if displays.count(d) > 1})
    assert not dupes, f"Duplicate display names in items config: {dupes}"

    for entry in items:
        code = int(entry["fbs_item_code"])
        assert code in fbs_code_to_fao_codes, (
            f"FBS item code {code} (entry '{entry['display']}') not found in FAOSTAT FBS metadata. "
            f"Code may have been removed or renumbered; check meta['item'] / meta['itemgroup']."
        )
        fao_codes = fbs_code_to_fao_codes[code]
        in_tm = fao_codes & tm_codes_in_data
        fbs_name = fbs_code_to_name.get(code, "<unknown>")
        assert in_tm, (
            f"Entry '{entry['display']}' (fbs_item_code={code}, FBS name {fbs_name!r}) maps to FAO codes "
            f"{sorted(fao_codes)[:10]} but none of them appear in the TM snapshot."
        )


# ─────────────────────────────────────────────────────────────────────────────
# Build pipeline
# ─────────────────────────────────────────────────────────────────────────────


def _assert_year_is_latest_well_covered(tb: Table, year: int) -> None:
    """Assert that `year` is the latest year whose row count is at least 90%
    of the series maximum — i.e. the latest year that is not the partially
    reported tail year. Fails loudly when FAOSTAT extends the matrix so we
    can update the `YEAR` constant deliberately."""
    rows_per_year = tb.groupby("year", observed=True).size()
    threshold = 0.9 * rows_per_year.max()
    latest_well_covered = int(rows_per_year[rows_per_year >= threshold].index.max())
    assert latest_well_covered == year, (
        f"YEAR is hard-coded to {year}, but the latest well-covered year in the data is "
        f"{latest_well_covered}. Bump YEAR (and re-run the viz) deliberately."
    )


def build_food_trade_slice(tb: Table) -> pd.DataFrame:
    """Reshape the garden table into the slim one-row-per-directional-flow
    slice consumed by the bespoke food-trade viz.

    Steps:
      1. Filter the garden table to the chosen year, physical quantities in
         tonnes, drop self-trade rows.
      2. Load FAOSTAT metadata and parse FBS Descriptions to map each FBS
         commodity / group to its set of FAO item codes.
      3. Load the curated items config (food_trade.items.yaml) and validate
         it against the FBS metadata.
      4. For each items entry, filter TM rows by item_code and aggregate
         per (reporter, partner, element). Label rows with the display name.
      5. Join exporter-reported and importer-reported flows into a single
         row per (Exporter, Importer, Item) and pick one Value (importer
         preferred, exporter fallback).
    """
    _assert_year_is_latest_well_covered(tb, YEAR)

    # Keep only physical quantities in tonnes for the chosen year.
    qty = tb[
        (tb["year"] == YEAR) & tb["element"].isin(["Export quantity", "Import quantity"]) & (tb["unit"] == "t")
    ].copy()
    qty["reporter_country"] = qty["reporter_country"].astype(str)
    qty["partner_country"] = qty["partner_country"].astype(str)
    qty["item_code"] = qty["item_code"].astype(int)
    qty = qty[qty["reporter_country"] != qty["partner_country"]]

    fbs_code_to_fao_codes, fbs_code_to_name = _load_fbs_rollup_maps()
    items_config = _load_items_config()
    _sanity_check_items_config(
        items_config,
        fbs_code_to_fao_codes=fbs_code_to_fao_codes,
        fbs_code_to_name=fbs_code_to_name,
        tm_codes_in_data=set(qty["item_code"].unique()),
    )

    # Roll up: for each entry, filter TM rows by item_code via the auto-derived
    # FAO codes, aggregate per (reporter, partner, element), label with the
    # display name.
    pieces = []
    for entry in items_config["items"]:
        codes = fbs_code_to_fao_codes[int(entry["fbs_item_code"])]
        sub = qty[qty["item_code"].isin(codes)]
        if sub.empty:
            log.warning("food_trade.empty_rollup", display=entry["display"])
            continue
        agg = (
            sub.groupby(["reporter_country", "partner_country", "element"], observed=True)["value"].sum().reset_index()
        )
        agg["item"] = entry["display"]
        pieces.append(agg)
    rolled = pd.concat(pieces, ignore_index=True)

    # Split into exporter-side and importer-side reports, keyed on the
    # directional (Exporter, Importer, Item) tuple.
    #
    # * Export-quantity rows are already keyed (reporter=Exporter, partner=Importer).
    # * Import-quantity rows are keyed (reporter=Importer, partner=Exporter); we
    #   swap the columns so they share a key with the exporter side.
    exp_side = rolled.loc[
        rolled["element"] == "Export quantity", ["reporter_country", "partner_country", "item", "value"]
    ].rename(
        columns={
            "reporter_country": "Exporter",
            "partner_country": "Importer",
            "item": "Item",
            "value": "value_exporter",
        }
    )
    imp_side = rolled.loc[
        rolled["element"] == "Import quantity", ["reporter_country", "partner_country", "item", "value"]
    ].rename(
        columns={
            "reporter_country": "Importer",
            "partner_country": "Exporter",
            "item": "Item",
            "value": "value_importer",
        }
    )

    merged = exp_side.merge(imp_side, on=["Exporter", "Importer", "Item"], how="outer")

    # Reconcile to a single Value. Prefer the importer-reported number; fall
    # back to the exporter-reported one when the importer didn't report.
    merged["Value"] = merged["value_importer"].fillna(merged["value_exporter"])
    merged = merged.dropna(subset=["Value"])
    merged = merged[merged["Value"] > 0]

    out = pd.DataFrame(merged[["Exporter", "Importer", "Item", "Value"]])
    out = out.sort_values(["Exporter", "Importer", "Item"]).reset_index(drop=True)

    log.info("food_trade.rows", n=len(out), year=YEAR, items=len(items_config["items"]))
    return out


def run() -> None:
    #
    # Load data.
    #
    ds_garden = paths.load_dataset("faostat_tm")
    tb = ds_garden.read("faostat_tm", safe_types=False)

    #
    # Process data.
    #
    out = build_food_trade_slice(tb)

    #
    # Save outputs.
    #
    with tempfile.TemporaryDirectory() as tmp:
        local_file = Path(tmp) / S3_FILENAME
        out.to_csv(local_file, index=False)
        s3_url = f"s3://{S3_BUCKET_NAME}/{S3_DATA_DIR / S3_FILENAME}"
        size_mb = f"{local_file.stat().st_size / 1e6:.1f}"
        if DRY_RUN:
            log.info("food_trade.dry_run_skip_upload", local=str(local_file), s3=s3_url, size_mb=size_mb)
        else:
            log.info("food_trade.uploading", s3=s3_url, size_mb=size_mb)
            s3_utils.upload(s3_url, local_file, public=True, downloadable=False)

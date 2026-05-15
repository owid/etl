"""S3 export step for the FAOSTAT food-trade Sankey viz.

Produces a slim CSV of bilateral trade flows for the latest well-covered year
in tonnes, matching the exact schema fetched by the bespoke `food-trade`
project in owid-grapher (see `bespoke/projects/food-trade/src/data.ts`).

Schema (one row per directional A → B flow for each item):
    Exporter            (str)   — country that exports the goods
    Importer            (str)   — country that imports the goods
    Item                (str)   — viz-display item (see food_trade.items.yaml)
    Value               (float) — bilateral trade flow in tonnes
    ExporterProduction  (float) — total Production tonnes of the Item in the
                                  Exporter country in the same year. Sourced
                                  from FAOSTAT QCL. NaN if QCL has no
                                  Production data for that (country, Item).
    ImporterSupply      (float) — apparent domestic supply tonnes of the Item
                                  in the Importer country in the same year,
                                  computed as
                                      Production + Total imports − Total exports
                                  where Production is the Importer's QCL
                                  Production, and Total imports / exports are
                                  the Importer's own TM Import / Export quantity
                                  rows summed across all partners (missing
                                  flows treated as zero). NaN when QCL has no
                                  Production figure for the (Importer, Item).

The display items shown in the dropdown are curated in `food_trade.items.yaml`.
Each entry there names a single FAO commodity item code (the same codebook
used by TM and QCL — codes 1-1296). The rollup is a direct integer-code filter
against TM's `item_code` column; no Description-field parsing, no FBS code
space, no cross-dataset reconciliation.

For each (Exporter, Importer, Item) the FAOSTAT detailed trade matrix
typically has two reports (one from each side) that can disagree. We pick
the **importer-reported** quantity by default (FAOSTAT convention — import
figures are more thoroughly tracked at customs) and fall back to the
exporter-reported quantity when the importer didn't report.

Output:
    https://owid-public.owid.io/food-trade/trade.csv
"""

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

# Sibling config file. Name starts with "food_trade" so ETL change-detection
# picks it up automatically (see etl/steps/__init__.py:_step_files).
ITEMS_CONFIG_PATH = Path(__file__).parent / "food_trade.items.yaml"

paths = PathFinder(__file__)


# ─────────────────────────────────────────────────────────────────────────────
# YAML config loading & validation
# ─────────────────────────────────────────────────────────────────────────────


def _load_items_config() -> dict:
    """Load the curated items config from food_trade.items.yaml."""
    with open(ITEMS_CONFIG_PATH) as f:
        return yaml.safe_load(f)


def _sanity_check_items_config(config: dict, tm_codes_in_data: set[int]) -> None:
    """Validate the items config structurally and against the TM snapshot.

    Checks:
      1. Required top-level shape: a single 'items' list.
      2. Each items entry has required fields (display, item_code).
      3. display names are unique.
      4. Each item_code exists in the TM snapshot (catches typos / removed codes).
    """
    assert isinstance(config, dict) and "items" in config, "items config must be a mapping with an 'items' key"
    items = config["items"]
    assert isinstance(items, list) and items, "config['items'] must be a non-empty list"

    required = {"display", "item_code"}
    for entry in items:
        missing = required - entry.keys()
        assert not missing, f"items entry missing keys {sorted(missing)}: {entry}"

    displays = [e["display"] for e in items]
    dupes = sorted({d for d in displays if displays.count(d) > 1})
    assert not dupes, f"Duplicate display names in items config: {dupes}"

    missing_codes = sorted(int(e["item_code"]) for e in items if int(e["item_code"]) not in tm_codes_in_data)
    assert not missing_codes, (
        f"{len(missing_codes)} item_code(s) not found in TM snapshot for {YEAR}: {missing_codes[:10]}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Build pipeline
# ─────────────────────────────────────────────────────────────────────────────


def _load_qcl_production(year: int) -> pd.DataFrame:
    """Return QCL Production tonnes for `year`, keyed on (country, item_code).

    QCL uses the same FAO commodity item codebook as TM (codes 1-1296), so the
    join with our TM slice is a direct integer-code lookup. We pick rows with
    `element == "Production"` and `unit_short_name == "t"`; QCL stores Production
    in tonnes for crops (and as `"head"` for live animals — those rows are
    filtered out here because the YAML deliberately exposes only crops).

    Country names come from QCL (already OWID-harmonized by the broader
    FAOSTAT pipeline). Our TM Exporter names are also OWID-harmonized via
    `faostat_tm.countries.json`, so the merge is name-based and safe — checked
    separately: 209 of the 221 TM countries also appear in QCL; the 12 unmapped
    ones are tiny territories with no agricultural production."""
    ds = paths.load_dataset("faostat_qcl")
    tb = ds.read("faostat_qcl", safe_types=False)

    prod = tb[(tb["element"] == "Production") & (tb["year"] == year) & (tb["unit_short_name"] == "t")].copy()
    # QCL stores `item_code` as a zero-padded string ("00000015"); cast to int.
    prod["item_code"] = prod["item_code"].astype(int)
    prod["country"] = prod["country"].astype(str)
    out = pd.DataFrame(prod[["country", "item_code", "value"]]).rename(columns={"value": "Production"})
    # If QCL has multiple rows per (country, item_code) — e.g. China-mainland +
    # China-aggregate ambiguity — sum them deterministically.
    return out.groupby(["country", "item_code"], as_index=False, observed=True)[["Production"]].sum()


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
      1. Filter the garden table to YEAR, physical quantities in tonnes,
         drop self-trade rows.
      2. Load and validate the items config against the TM item_code universe.
      3. For each entry, filter TM rows by item_code and tag with the display name.
      4. Join exporter-reported and importer-reported flows into a single row
         per (Exporter, Importer, Item) and pick one Value (importer preferred,
         exporter fallback).
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

    items_config = _load_items_config()
    _sanity_check_items_config(items_config, tm_codes_in_data=set(qty["item_code"].unique()))

    # Tag each TM row with its viz-display name via a direct item_code lookup.
    code_to_display = {int(e["item_code"]): e["display"] for e in items_config["items"]}
    qty = qty[qty["item_code"].isin(code_to_display)].copy()
    qty["item"] = qty["item_code"].map(code_to_display)

    # QCL Production by (country, Item), restricted to our curated codes.
    qcl_prod = _load_qcl_production(YEAR)
    qcl_prod = qcl_prod[qcl_prod["item_code"].isin(code_to_display)].copy()
    qcl_prod["Item"] = qcl_prod["item_code"].map(code_to_display)
    production = qcl_prod[["country", "Item", "Production"]]
    log.info("food_trade.qcl_production_rows", n=len(production), year=YEAR)

    # Total Import / Export quantity per (country, Item), aggregated across all
    # partners — used as the trade-flow components of apparent domestic supply.
    # Each country's totals come from its own reported rows (`reporter_country`).
    totals = pd.DataFrame(
        qty.groupby(["reporter_country", "item", "element"], observed=True)["value"]
        .sum()
        .unstack("element", fill_value=0.0)
        .reset_index()
    ).rename(
        columns={
            "reporter_country": "country",
            "item": "Item",
            "Import quantity": "country_imports",
            "Export quantity": "country_exports",
        }
    )
    for col in ("country_imports", "country_exports"):
        if col not in totals.columns:
            totals[col] = 0.0

    # Apparent domestic supply = Production + Imports − Exports. We require
    # Production to be known (a missing QCL row leaves ImporterSupply NaN);
    # missing import / export flows are treated as zero (a country that didn't
    # report a flow for an item simply didn't trade it).
    supply = totals.merge(production, on=["country", "Item"], how="left")
    supply["ImporterSupply"] = (
        supply["Production"] + supply["country_imports"].fillna(0) - supply["country_exports"].fillna(0)
    )
    supply = supply.rename(columns={"country": "Importer"})[["Importer", "Item", "ImporterSupply"]]

    # Same production table again, just renamed for the Exporter-side merge.
    exporter_production = production.rename(columns={"country": "Exporter", "Production": "ExporterProduction"})

    # Split into exporter-side and importer-side reports, keyed on the
    # directional (Exporter, Importer, Item) tuple.
    #
    # * Export-quantity rows are already keyed (reporter=Exporter, partner=Importer).
    # * Import-quantity rows are keyed (reporter=Importer, partner=Exporter); we
    #   swap the columns so they share a key with the exporter side.
    exp_side = qty.loc[
        qty["element"] == "Export quantity", ["reporter_country", "partner_country", "item", "value"]
    ].rename(
        columns={
            "reporter_country": "Exporter",
            "partner_country": "Importer",
            "item": "Item",
            "value": "value_exporter",
        }
    )
    imp_side = qty.loc[
        qty["element"] == "Import quantity", ["reporter_country", "partner_country", "item", "value"]
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

    # Add Exporter Production and Importer apparent domestic supply. Each
    # value repeats across all rows sharing the same (Exporter, Item) /
    # (Importer, Item) — fine because the viz aggregates one side at a time.
    out = out.merge(exporter_production, on=["Exporter", "Item"], how="left")
    out = out.merge(supply, on=["Importer", "Item"], how="left")

    out = out.sort_values(["Exporter", "Importer", "Item"]).reset_index(drop=True)

    log.info(
        "food_trade.rows",
        n=len(out),
        year=YEAR,
        items=len(items_config["items"]),
        with_production=int(out["ExporterProduction"].notna().sum()),
        with_supply=int(out["ImporterSupply"].notna().sum()),
    )
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

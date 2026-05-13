"""S3 export step for the FAOSTAT food-trade Sankey viz.

Produces a slim CSV of bilateral trade flows for the latest well-covered year
in tonnes, matching the exact schema fetched by the bespoke `food-trade`
project in owid-grapher (see `bespoke/projects/food-trade/src/data.ts`).

Schema (one row per directional A → B flow for each item):
    Exporter (str)   — country that exports the goods
    Importer (str)   — country that imports the goods
    Item     (str)   — FAOSTAT item / product
    Value    (float) — quantity in tonnes

For each (Exporter, Importer, Item) the FAOSTAT detailed trade matrix
typically has two reports (one from each side) that can disagree. We pick
the **importer-reported** quantity by default — by trade-economics
convention, import figures are more thoroughly tracked (for tariff /
customs purposes) — and fall back to the exporter-reported quantity when
the importer didn't report.

Output:
    https://owid-public.owid.io/food-trade/trade.csv
"""

import tempfile
from pathlib import Path

import pandas as pd
import structlog
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

paths = PathFinder(__file__)


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
    slice consumed by the bespoke food-trade viz."""
    _assert_year_is_latest_well_covered(tb, YEAR)

    # Keep only physical quantities in tonnes for the chosen year. ~99% of
    # Export/Import quantity rows are in tonnes; the remainder are live-animal
    # counts (head / 1000-head) which aren't suitable for a tonnes-based Sankey.
    qty = tb[
        (tb["year"] == YEAR) & tb["element"].isin(["Export quantity", "Import quantity"]) & (tb["unit"] == "t")
    ].copy()
    qty["reporter_country"] = qty["reporter_country"].astype(str)
    qty["partner_country"] = qty["partner_country"].astype(str)
    qty = qty[qty["reporter_country"] != qty["partner_country"]]

    # Split into exporter-side and importer-side reports, keyed on the
    # directional (Exporter, Importer, Item) tuple.
    #
    # * Export-quantity rows are already keyed (reporter=Exporter, partner=Importer).
    # * Import-quantity rows are keyed (reporter=Importer, partner=Exporter); we
    #   swap the columns so they share a key with the exporter side.
    exp_side = (
        qty.loc[
            qty["element"] == "Export quantity",
            ["reporter_country", "partner_country", "item", "value"],
        ]
        .rename(columns={"reporter_country": "Exporter", "partner_country": "Importer", "item": "Item"})
        .rename(columns={"value": "value_exporter"})
    )
    imp_side = (
        qty.loc[
            qty["element"] == "Import quantity",
            ["reporter_country", "partner_country", "item", "value"],
        ]
        .rename(columns={"reporter_country": "Importer", "partner_country": "Exporter", "item": "Item"})
        .rename(columns={"value": "value_importer"})
    )

    # Full-outer-join so the union of all known flows is preserved.
    merged = exp_side.merge(imp_side, on=["Exporter", "Importer", "Item"], how="outer")

    # Reconcile to a single Value. Prefer the importer's number (FAOSTAT
    # convention — better tracked at customs); fall back to the exporter's
    # number when the importer didn't report.
    merged["Value"] = merged["value_importer"].fillna(merged["value_exporter"])
    # Drop flows where both sides are missing (shouldn't happen by construction
    # but harmless) and flows that round to zero.
    merged = merged.dropna(subset=["Value"])
    merged = merged[merged["Value"] > 0]

    out = pd.DataFrame(merged[["Exporter", "Importer", "Item", "Value"]])
    out = out.sort_values(["Exporter", "Importer", "Item"]).reset_index(drop=True)

    log.info("food_trade.rows", n=len(out), year=YEAR)
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

"""S3 export step for the FAOSTAT food-trade Sankey viz.

Produces a slim long-format slice of `faostat_tm` for the latest well-covered
year — one row per (focal country, partner, item, direction) in tonnes — so a
bespoke Sankey-style viz can pick a country + product + direction and show the
top trade partners without having to load the full 50 M-row garden table.

Each row reflects what the *focal country itself* reported (i.e.
`country = reporter_country` in the source). So for the same A→B trade flow
you may get two rows — A's reported export and B's reported import — and these
may disagree on the exact quantity. We do not try to reconcile them here; the
viz can pick whichever side of the report it wants to display.

Outputs (uploaded to S3, publicly readable):
  * https://owid-public.owid.io/data/faostat/food-trade.csv      (~77 MB, universal)
  * https://owid-public.owid.io/data/faostat/food-trade.parquet  (~5 MB, efficient)
"""

import tempfile
from pathlib import Path

import pandas as pd
import structlog
from owid.catalog import Table, s3_utils

from etl.config import DRY_RUN
from etl.helpers import PathFinder

log = structlog.get_logger()

# Public S3 bucket and prefix.
S3_BUCKET_NAME = "owid-public"
S3_DATA_DIR = Path("data/faostat")

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
    """Filter and reshape the garden table into the slim long-format slice
    consumed by the food-trade viz."""
    _assert_year_is_latest_well_covered(tb, YEAR)

    # Keep only physical-quantity rows in tonnes for the chosen year.
    qty = tb[
        (tb["year"] == YEAR) & tb["element"].isin(["Export quantity", "Import quantity"]) & (tb["unit"] == "t")
    ].copy()

    # Drop self-trade rows (small but non-zero).
    qty["reporter_country"] = qty["reporter_country"].astype(str)
    qty["partner_country"] = qty["partner_country"].astype(str)
    qty = qty[qty["reporter_country"] != qty["partner_country"]]

    # Map element to a clearer direction tag.
    qty["direction"] = qty["element"].map({"Export quantity": "export", "Import quantity": "import"}).astype(str)

    df = pd.DataFrame(
        qty[["reporter_country", "partner_country", "item", "item_code", "direction", "value", "year"]]
    ).rename(columns={"value": "tonnes"})

    # Sort conveniently.
    df = df.sort_values(
        ["reporter_country", "item", "direction", "tonnes"],
        ascending=[True, True, True, False],
    ).reset_index(drop=True)

    log.info("food_trade.rows", n=len(df), year=YEAR)
    return df


def run() -> None:
    #
    # Load data.
    #
    ds_garden = paths.load_dataset("faostat_tm")
    tb = ds_garden.read("faostat_tm", safe_types=False)

    #
    # Process data.
    #
    df = build_food_trade_slice(tb)

    #
    # Save outputs.
    #
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        # CSV — universal, browse-able / downloadable by humans.
        csv_path = tmp_path / "food-trade.csv"
        df.to_csv(csv_path, index=False)

        # Parquet — compact, faster for the viz to fetch and parse.
        parquet_path = tmp_path / "food-trade.parquet"
        df.to_parquet(parquet_path, index=False, compression="zstd")

        for local_file in (csv_path, parquet_path):
            s3_file = S3_DATA_DIR / local_file.name
            s3_url = f"s3://{S3_BUCKET_NAME}/{s3_file}"
            if DRY_RUN:
                log.info(
                    "food_trade.dry_run_skip_upload",
                    local=str(local_file),
                    s3=s3_url,
                    size_mb=f"{local_file.stat().st_size / 1e6:.1f}",
                )
            else:
                log.info(
                    "food_trade.uploading",
                    s3=s3_url,
                    size_mb=f"{local_file.stat().st_size / 1e6:.1f}",
                )
                s3_utils.upload(s3_url, local_file, public=True, downloadable=False)

"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define columns to keep and their new names
COLUMNS_TO_KEEP = {
    "cname": "country",
    "year": "year",
    "indicator": "indicator",
    "variable": "welfare_type",
    "equiv": "equivalence_scale",
    "value": "value",
}

# NOTE: The percentiles file used to carry a `year_ppp` column (the PPP base year); LIS stopped
# shipping it in the 2026-09 release. The garden metadata hardcodes 2021 prices, so confirm the PPP
# base year with LIS on each update — the guard below only fires if the column comes back changed.
PERCENTILES_FILE = "lis_percentiles.csv"


def run() -> None:
    #
    # Load inputs.
    #
    snapshot_names = [
        "lis_incomes.csv",
        "lis_absolute_poverty.csv",
        "lis_inequality.csv",
        "lis_relative_poverty.csv",
        PERCENTILES_FILE,
    ]
    tables = []
    for snapshot_name in snapshot_names:
        # Retrieve snapshot.
        snap = paths.load_snapshot(
            snapshot_name,
        )

        # Load data from snapshot.
        tb = snap.read()

        #
        # Process data.
        #

        # If LIS reinstates the `year_ppp` column in the percentiles file, surface a PPP rebase — the
        # garden metadata hardcodes 2021 prices. Warn-only by design: a rebase is a review-worthy
        # signal handled at the version bump (where ppp_version and the price-year unit labels are
        # revisited), not a build-breaker. Its absence is the norm since 2026-09 and is not flagged.
        if snapshot_name == PERCENTILES_FILE and "year_ppp" in tb.columns:
            ppp_years = sorted(tb["year_ppp"].dropna().unique())
            if ppp_years != [2021]:
                paths.log.warning(
                    f"{snapshot_name}: unexpected year_ppp {ppp_years} (expected [2021]); check garden ppp_version."
                )

        # Keep only relevant columns and rename them.
        tb = tb[list(COLUMNS_TO_KEEP.keys())].rename(columns=COLUMNS_TO_KEEP, errors="raise")

        # Improve table format.
        tb = tb.format(
            ["country", "year", "indicator", "welfare_type", "equivalence_scale"],
            short_name=snapshot_name.replace(".csv", "").replace("lis_", ""),
        )

        # Append current table to list of tables.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables)

    # Save meadow dataset.
    ds_meadow.save()

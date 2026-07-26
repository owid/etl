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

# The percentiles file is the only one that carries a `year_ppp` column (the PPP base year).
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

        # The percentiles file ships an extra `year_ppp` column (the PPP base year) the other files
        # lack. The column selection below drops it, but first surface any change — a PPP rebase, or
        # the column disappearing — so it is noticed: the garden metadata hardcodes 2021 prices. This
        # is warn-only by design; a PPP rebase is a review-worthy signal handled at the annual version
        # bump (where ppp_version and the price-year unit labels are revisited), not a build-breaker.
        if snapshot_name == PERCENTILES_FILE:
            if "year_ppp" not in tb.columns:
                paths.log.warning(f"{snapshot_name}: expected `year_ppp` column is missing; check garden ppp_version.")
            else:
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

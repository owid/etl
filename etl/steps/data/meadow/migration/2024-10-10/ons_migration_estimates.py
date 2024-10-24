"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ons_migration_estimates.xlsx")

    # Load data from snapshot.
    tb = snap.read_excel(sheet_name="Table 2", header=4)

    tb["country"] = "United Kingdom"
    tb["year"] = tb["Period"].apply(year_from_period)
    tb["period"] = tb["Period"].apply(period_comment)
    tb = tb.drop(columns=["Period"], errors="raise")

    for col in ["Upper bound", "Lower bound", "Estimate"]:
        tb[col] = tb[col].astype(str)

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year", "period", "flow"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def year_from_period(period: str) -> int:
    return int(period.split(" ")[-1]) + 2000


def period_comment(period: str) -> str:
    year_end = period.split(" ")[1]
    if year_end == "Dec":
        return "Year ending December"
    elif year_end == "Jun":
        return "Year ending June"
    else:
        raise ValueError(f"Unexpected period: {period}")

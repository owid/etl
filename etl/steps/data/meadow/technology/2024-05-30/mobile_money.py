"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("mobile_money.xlsx")

    # Load data from snapshot.
    tb = snap.read_excel(sheet_name="All Data Table")

    # Select data of interest.
    tb = tb[tb.Measure == "Active, 90-day Accounts"]
    tb = tb[tb.Geo_view == "Region"]

    # Drop unnecessary columns.
    tb = tb.drop(columns=["Measure", "Geo_view", "Attribute", "Unit", "Metric"])

    # Data frame now starts with column Geo_name, then a series of quarterly columns.
    # Melt the data frame to a long format.
    tb = tb.melt(id_vars="Geo_name", var_name="date", value_name="active_accounts_90d")

    # Only keep dates for Q4, with regex "\d{2}/12/\d{4}"
    tb = tb[tb.date.str.match(r"\d{2}/12/\d{4}")]

    # Transform date to year.
    tb["year"] = tb.date.str[-4:]
    tb = tb.drop(columns=["date"])

    # Rename columns.
    tb = tb.rename(columns={"Geo_name": "country"})

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

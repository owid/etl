"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from data, and how to rename them.
COLUMNS = {
    "Year": "year",
    "Value": "corn_yield",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and read its data.
    snap = paths.load_snapshot("us_corn_yields.csv")
    tb = snap.read_csv()

    #
    # Process data.
    #
    # Sanity check.
    error = "Data does not have the expected characteristics."
    assert tb[
        ["Program", "Period", "Geo Level", "State", "Commodity", "Data Item", "Domain", "Domain Category"]
    ].drop_duplicates().values.tolist() == [
        [
            "SURVEY",
            "YEAR",
            "NATIONAL",
            "US TOTAL",
            "CORN",
            "CORN, GRAIN - YIELD, MEASURED IN BU / ACRE",
            "TOTAL",
            "NOT SPECIFIED",
        ]
    ], error

    # Select and rename required columns, and add a country column.
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise").assign(**{"country": "United States"})

    # Set an appropriate index and sort conveniently.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()

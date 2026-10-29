"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns of the UNHCR population API, renamed to the names used in previous versions of this dataset.
COLUMNS = {
    "year": "year",
    "coo_name": "country_of_origin",
    "coa_name": "country_of_asylum",
    "refugees": "refugees",
    "returned_refugees": "returned_refugees",
    "asylum_seekers": "asylum_seekers",
    "idps": "idps",
    "returned_idps": "returned_idps",
    "stateless": "stateless",
    "hst": "hst",
    "ooc": "ooc",
    # Other people in need of international protection.
    "oip": "oip",
}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("refugee_data.csv")

    # Load data from snapshot ("-" marks missing values in the API).
    tb = snap.read_csv(na_values=["-"])

    #
    # Process data.
    #
    # Keep country names and indicators (drop UNHCR's internal ids and ISO codes).
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS, errors="raise")

    # Use categoricals for the country columns.
    tb["country_of_origin"] = tb["country_of_origin"].astype("category")
    tb["country_of_asylum"] = tb["country_of_asylum"].astype("category")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country_of_origin", "country_of_asylum", "year"])

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

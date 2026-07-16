"""Load a snapshot and create a meadow dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to keep from the dataset sheet. Sector columns follow ISIC rev. 3.1 groupings.
COLUMNS = {
    "Country": "country",
    "Variable": "variable",
    "Year": "year",
    "Agriculture": "agriculture",
    "Mining": "mining",
    "Manufacturing": "manufacturing",
    "Utilities": "utilities",
    "Construction": "construction",
    "Trade, restaurants and hotels": "trade_restaurants_hotels",
    "Transport, storage and communication": "transport_communication",
    "Finance, insurance, real estate and business services": "finance_business_services",
    "Government services": "government_services",
    "Community, social and personal services": "community_services",
    "Summation of sector GDP": "total",
}

# Variables to keep: value added at current national prices (VA) and persons engaged (EMP).
# The remaining variables (VA_Q05, VA_Q10, VA_Q91) are value added at constant prices.
VARIABLES = ["VA", "EMP"]


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ggdc_10_sector.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="dataset")

    #
    # Process data.
    #
    # Strip stray whitespace from column names (e.g. "Agriculture ").
    tb = tb.rename(columns={column: column.strip() for column in tb.columns})

    # Keep value added at current prices and persons engaged.
    tb = tb[tb["Variable"].isin(VARIABLES)]
    tb = tb[list(COLUMNS)].rename(columns=COLUMNS)

    # Ensure numeric values (the total column contains empty strings in some rows).
    data_columns = [column for column in tb.columns if column not in ("country", "variable", "year")]
    for column in data_columns:
        tb[column] = pr.to_numeric(tb[column], errors="coerce")

    # Improve tables format.
    tables = [tb.format(["country", "variable", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

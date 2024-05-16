"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset
from owid import catalog

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("computer_memory_storage")

    # Read table from meadow dataset.
    tb = ds_meadow["computer_memory_storage"].reset_index()

    #
    # Process data.
    #
    us_cpi = catalog.find_latest(dataset="us_consumer_prices").reset_index()[["year", "all_items"]]

    # Left-merge and validate 1:1
    tb = tb.merge(us_cpi, on="year", how="left", validate="1:1")

    # If there are any missing values, raise an error.
    if tb["all_items"].isnull().any():
        raise ValueError("Missing values in the CPI table.")

    # Adjust for inflation, using 2020 as the base year.
    cpi_2020 = us_cpi[us_cpi["year"] == 2020]["all_items"].values[0]
    for col in ["ddrives", "flash", "memory", "ssd"]:
        tb[col] = tb[col] * cpi_2020 / tb["all_items"]
    # Drop the CPI column.
    tb = tb.drop(columns=["all_items"])

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

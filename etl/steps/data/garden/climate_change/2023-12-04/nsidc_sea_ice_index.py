"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("nsidc_sea_ice_index")

    # Read table from meadow dataset.
    tb = ds_meadow["nsidc_sea_ice_index"].reset_index()

    #
    # Process data.
    #
    # Remove column with annual average.
    tb = tb.drop(columns=["annual"])

    # Convert table to long format.
    tb = tb.melt(id_vars=["location", "year"], var_name="month", value_name="sea_ice_extent")

    # Create column of date, assuming each measurement is taken mid month.
    tb["date"] = pd.to_datetime(tb["year"].astype(str) + tb["month"].str[0:3] + "15", format="%Y%b%d")

    # Copy metadata from any other previous column.
    tb["date"] = tb["date"].copy_metadata(tb["year"])

    # Drop empty rows and unnecessary columns.
    tb = tb.dropna().drop(columns=["year", "month"])

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["location", "date"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the combined table.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()

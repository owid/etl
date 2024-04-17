"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("surface_temperature_analysis")
    tb = ds_meadow["surface_temperature_analysis_world"]

    #
    # Process data.
    #
    # Initialize dictionary to store processed tables.
    tables = {}
    for table_name in ds_meadow.table_names:
        # Read table.
        tb = ds_meadow[table_name].reset_index()
        # Get location from table name.
        location = table_name.split("surface_temperature_analysis_")[-1].replace("_", " ").title()
        # Add column for location.
        tb["location"] = location
        # Convert table to long format.
        tb = tb.melt(id_vars=["year", "location"], var_name="month", value_name="temperature_anomaly")
        # Create column of date, assuming each measurement is taken mid month.
        tb["date"] = pd.to_datetime(tb["year"].astype(str) + tb["month"] + "15", format="%Y%b%d")
        # Copy metadata from any other previous column.
        tb["date"] = tb["date"].copy_metadata(tb["location"])
        # Select necessary columns.
        tb = tb[["location", "date", "temperature_anomaly"]]
        # Remove rows with missing values.
        tb = tb.dropna(subset=["temperature_anomaly"]).reset_index(drop=True)
        # Update table.
        tables[location] = tb

    # Concatenate all tables.
    tb = pr.concat(list(tables.values()), ignore_index=True, short_name=paths.short_name)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["location", "date"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the combined table.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()

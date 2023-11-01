"""Load a meadow dataset and create a garden dataset."""

import numpy as np

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("who_statins")

    # Read table from meadow dataset.
    tb = ds_meadow["who_statins"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = tb.set_index(["country", "year"], verify_integrity=True)

    # Asserting that the unique values in the column are a subset of the provided list
    provided_values = ["Yes", "Don't know", "No", "No data received", "No response"]
    unique_values_in_column = tb["general_availability_of_statins_in_the_public_health_sector"].unique()
    assert set(unique_values_in_column).issubset(provided_values)

    # Replace the specified values where there is no data with "NaN" for consistency on grapher charts
    values_to_replace = ["No data received", "No response", "Don't know"]
    tb["general_availability_of_statins_in_the_public_health_sector"] = tb[
        "general_availability_of_statins_in_the_public_health_sector"
    ].replace(values_to_replace, np.nan)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

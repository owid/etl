"""Load a meadow dataset and create a garden dataset."""
import numpy as np
import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("plastic_waste_2019")

    # Read table from meadow dataset.
    tb = ds_meadow["plastic_waste_2019"].reset_index()
    # Convert million to actual number
    tb["plastic_waste"] = tb["plastic_waste"] * 1e6

    #
    # Process data.
    #
    country_mapping_path = paths.directory / "plastic_pollution.countries.json"
    tb = geo.harmonize_countries(df=tb, countries_file=country_mapping_path)
    total_df = tb.groupby(["year", "polymer", "application"])["plastic_waste"].sum().reset_index()

    total_df["country"] = "World"
    combined_df = pr.merge(
        total_df, tb, on=["country", "year", "polymer", "application", "plastic_waste"], how="outer"
    ).copy_metadata(from_table=tb)

    # Drop rows that only have 0 values to avoid indicators with 0t after pivoting
    combined_df = combined_df.loc[combined_df["plastic_waste"] != 0]
    tb = (
        combined_df.underscore()
        .set_index(["country", "year", "polymer", "application"], verify_integrity=True)
        .sort_index()
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

"""Load a meadow dataset and create a garden dataset."""

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
    ds_meadow = paths.load_dataset("plastic_fate_regions_projections")

    # Read table from meadow dataset.
    tb = ds_meadow["plastic_fate_regions_projections"].reset_index()
    # Convert million to actual number
    tb["value"] = tb["value"] * 1e6
    #
    # Process data.
    #
    country_mapping_path = paths.directory / "plastic_pollution.countries.json"
    tb = geo.harmonize_countries(df=tb, countries_file=country_mapping_path)
    total_df = tb.groupby(["year", "plastic_fate"])["value"].sum().reset_index()

    total_df["country"] = "World"
    combined_df = pr.merge(total_df, tb, on=["country", "year", "plastic_fate", "value"], how="outer").copy_metadata(
        from_table=tb
    )
    total_df = total_df.rename(columns={"value": "global_value"})

    # Merge the global totals back to the original DataFrame
    df_with_share = pr.merge(combined_df, total_df, on=["year", "plastic_fate"], how="left")

    # Calculate the share from global total
    df_with_share["share"] = (df_with_share["value"] / df_with_share["global_value"]) * 100

    # Optionally, drop the 'Global Value' column if it's not needed
    df_with_share = df_with_share.drop(columns=["global_value", "country_y"])
    df_with_share.rename(columns={"country_x": "country"}, inplace=True)

    tb = df_with_share.underscore().set_index(["country", "year", "plastic_fate"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

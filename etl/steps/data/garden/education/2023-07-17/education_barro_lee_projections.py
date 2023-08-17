"""Load a meadow dataset and create a garden dataset."""

from typing import List, cast

import education_lee_lee
import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = [
    "North America",
    "South America",
    "Europe",
    "European Union (27)",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
    "World",
]


def add_data_for_regions(tb: Table, regions: List[str], ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    tb_with_regions = tb.copy()

    aggregations = {column: "median" for column in tb_with_regions.columns if column not in ["country", "year"]}
    for region in REGIONS:
        # Find members of current region.
        members = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
        )
        tb_with_regions = geo.add_region_aggregates(
            df=tb_with_regions,
            region=region,
            countries_in_region=members,
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.99999,
            aggregations=aggregations,
        )
    tb_with_regions = tb_with_regions.copy_metadata(from_table=tb)

    return tb_with_regions


def run(dest_dir: str) -> None:
    # Load dependencies.
    # These datasets contain information required for the calculations.

    # Load dataset containing Barro-Lee education projections.
    ds_meadow = cast(Dataset, paths.load_dependency("education_barro_lee_projections"))

    # Load dataset containing regions data.
    ds_regions: Dataset = paths.load_dependency("regions")

    # Load dataset containing income groups data.
    ds_income_groups: Dataset = paths.load_dependency("income_groups")

    # Extract the table for education projections and reset its index.
    tb = ds_meadow["education_barro_lee_projections"]
    tb.reset_index(inplace=True)

    # Process data.

    # Harmonize the country names in the table.
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Rename the age groups for clarity.
    tb["age_group"] = tb["age_group"].replace(
        {"15-64": "Youth and Adults (15-64 years)", "15-24": "Youth (15-24 years)", "25-64": "Adults (25-64 years)"}
    )

    # Prepare attainment data using the Lee-Lee method.
    df_projections = education_lee_lee.prepare_attainment_data(tb)

    # Drop columns containing thousands values as they are not needed.
    columns_to_drop = [column for column in df_projections.columns if "__thousands" in column]
    df_projections = df_projections.drop(columns=columns_to_drop)

    # Convert the DataFrame to a Table object and set required properties.
    tb_projections = Table(df_projections, short_name=paths.short_name, underscore=True)

    # Add regional and income group data to the projections.
    tb_projections = add_data_for_regions(
        tb=tb_projections, regions=REGIONS, ds_regions=ds_regions, ds_income_groups=ds_income_groups
    )

    # Set the index to be based on country and year.
    tb_projections.set_index(["country", "year"], inplace=True)

    # Create a copy of the projections table with a suffix in the column names.
    tb_projections_copy = tb_projections.copy(deep=True)
    suffix = "_projections"
    tb_projections_copy.columns = tb_projections_copy.columns + suffix

    # Load historical education data and drop columns related to enrollment rates.
    ds_past = cast(Dataset, paths.load_dependency("education_lee_lee"))
    tb_past = ds_past["education_lee_lee"]
    cols_to_drop = [col for col in tb_past.columns if "enrollment_rates" in col]
    tb_past = tb_past.drop(columns=cols_to_drop)

    # Concatenate the projections with historical data below the year 2015.
    df_below_2015 = tb_past[(tb_past.index.get_level_values("year") < 2015)]
    stiched = pd.concat([tb_projections, df_below_2015])

    # Merge the original projections and the concatenated data.
    stiched_projections = pd.merge(
        tb_projections_copy,
        stiched,
        on=["country", "year"],
        how="outer",
    )

    # Convert the merged data into a Table object.
    tb_stiched = Table(stiched_projections, short_name=paths.short_name)

    # Save outputs.

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_stiched], default_metadata=ds_meadow.metadata)

    # Save the newly created dataset.
    ds_garden.save()

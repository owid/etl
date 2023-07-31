"""Load a meadow dataset and create a garden dataset."""

from typing import List, cast

import education_lee_lee
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
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("education_barro_lee_projections"))

    # Load regions dataset.
    ds_regions: Dataset = paths.load_dependency("regions")

    # Load income groups dataset.
    ds_income_groups: Dataset = paths.load_dependency("income_groups")
    # Read table from meadow dataset.
    tb = ds_meadow["education_barro_lee_projections"]
    tb.reset_index(inplace=True)

    #
    # Process data.
    #
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb["age_group"] = tb["age_group"].replace(
        {"15-64": "Youth and Adults (15-64 years)", "15-24": "Youth (15-24 years)", "25-64": "Adults (25-64 years)"}
    )

    df_projections = education_lee_lee.prepare_attainment_data(tb)
    columns_to_drop = [column for column in df_projections.columns if "__thousands" in column]
    df_projections = df_projections.drop(columns=columns_to_drop)

    tb_projections = Table(df_projections, short_name=paths.short_name, underscore=True)

    tb_projections = add_data_for_regions(
        tb=tb_projections, regions=REGIONS, ds_regions=ds_regions, ds_income_groups=ds_income_groups
    )

    tb_projections.set_index(["country", "year"], inplace=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_projections], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

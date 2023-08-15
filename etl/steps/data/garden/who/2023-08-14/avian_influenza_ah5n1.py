"""Load a meadow dataset and create a garden dataset."""

from typing import cast

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Regions
REGIONS = [
    "Asia",
    "Africa",
    "North America",
    "South America",
    "Europe",
    "Oceania",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("avian_influenza_ah5n1"))

    # Load regions dataset.
    ds_regions: Dataset = paths.load_dependency("regions")

    # Read table from meadow dataset.
    tb = ds_meadow["avian_influenza_ah5n1"].reset_index()

    #
    # Process data.
    #
    # To date
    tb["date"] = pd.to_datetime(tb["month"]).astype("datetime64[ns]")

    # Drop columns
    tb = tb.drop(columns=["range", "month"])

    # Harmonize country names
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Add aggregates
    tb = add_regions(tb, ds_regions)
    tb = add_world(tb)

    # Set index
    tb = tb.set_index(["country", "date"], verify_integrity=True)

    # Set short_name
    tb.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_regions(tb: Table, ds_regions: Dataset) -> Table:
    "Add regions to the table."
    for region in REGIONS:
        # List of countries in region.
        countries_in_region = geo.list_members_of_region(region=region, ds_regions=ds_regions)

        # Add region
        tb_region = tb[tb["country"].isin(countries_in_region)]
        tb_region = tb_region.assign(country=region)
        tb_region = tb_region.groupby(["date", "country"], as_index=False)["avian_cases"].sum()

        # Combine
        tb = pr.concat([tb, tb_region], ignore_index=True)

    return tb


def add_world(tb: Table) -> Table:
    """Add world aggregate to the table."""
    # Ignore regions
    tb_world = tb[~tb["country"].isin(REGIONS)].copy()

    # Aggregate
    tb_world = tb_world.groupby("date", as_index=False)["avian_cases"].sum()
    tb_world = tb_world.assign(country="World")

    # Combine
    tb = pd.concat(
        [
            tb,
            tb_world,
        ],
        ignore_index=True,
    )

    return tb

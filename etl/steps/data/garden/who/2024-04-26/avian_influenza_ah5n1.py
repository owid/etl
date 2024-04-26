"""Load a meadow dataset and create a garden dataset."""


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
    ds_meadow = paths.load_dataset("avian_influenza_ah5n1")
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")
    # Read table from meadow dataset.
    tb = ds_meadow["avian_influenza_ah5n1"].reset_index()

    #
    # Process data.
    #
    mask = tb["range"] == "All"
    tb_year = tb[mask].drop(columns=["range"])
    tb_month = tb[~mask].drop(columns=["range"])

    # Obtain date
    ## Yearly data
    tb_year = tb_year.rename(columns={"month": "date"})
    ## Monthly data
    date_1 = pd.to_datetime(tb_month["month"], format="%b-%y", errors="coerce")
    date_2 = pd.to_datetime(tb_month["month"], format="%y-%b", errors="coerce")
    date_3 = pd.to_datetime("200" + tb_month["month"].astype(str), format="%Y-%b", errors="coerce")
    tb_month["date"] = date_1.fillna(date_2).fillna(date_3)
    assert tb_month["date"].notna().all(), "Some dates could not be parsed."
    tb_month = tb_month.drop(columns=["month"])

    # Harmonize country names
    tb_month = geo.harmonize_countries(df=tb_month, countries_file=paths.country_mapping_path)
    tb_year = geo.harmonize_countries(df=tb_year, countries_file=paths.country_mapping_path)

    # Add aggregates
    tb_month = add_regions(tb_month, ds_regions)
    tb_month = add_world(tb_month)
    tb_year = add_regions(tb_year, ds_regions)
    tb_year = add_world(tb_year)

    # Rename columns
    tb_year = tb_year.rename(
        columns={
            "date": "year",
            "avian_cases": "avian_cases_year",
        }
    )
    tb_month = tb_month.rename(
        columns={
            "avian_cases": "avian_cases_month",
        }
    )

    # Set dtype to numeric
    tb_year["year"] = tb_year["year"].astype(str).astype(int)

    # Sanity check
    assert tb_year["year"].max() == 2024
    assert tb_year["year"].min() == 1997

    # Set index
    tb_month = tb_month.format(["country", "date"])
    tb_year = tb_year.format(["country", "year"])

    # Set short_name
    tb_month.metadata.short_name = f"{tb_month.metadata.short_name}_month"
    tb_year.metadata.short_name = f"{tb_year.metadata.short_name}_year"

    #
    # Save outputs.
    #
    tables = [
        tb_month,
        tb_year,
    ]
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=tables, default_metadata=ds_meadow.metadata)

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
        tb_region = tb_region.groupby(["date", "country"], as_index=False, observed=True)["avian_cases"].sum()

        # Combine
        tb = pr.concat([tb, tb_region], ignore_index=True)

    return tb


def add_world(tb: Table) -> Table:
    """Add world aggregate to the table."""
    # Ignore regions
    tb_world = tb[~tb["country"].isin(REGIONS)].copy()

    # Aggregate
    tb_world = tb_world.groupby("date", as_index=False, observed=True)["avian_cases"].sum()
    tb_world = tb_world.assign(country="World")

    # Combine
    tb = pr.concat(
        [
            tb,
            tb_world,
        ],
        ignore_index=True,
    )

    return tb

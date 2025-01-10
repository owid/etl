"""WDI provides relative values. We add population data to estimate absolute values."""

from pathlib import Path

from etl.data_helpers.geo import add_population_to_table, add_regions_to_table
from etl.helpers import PathFinder, create_dataset

CURRENT_DIR = Path(__file__).parent
METADATA_PATH = CURRENT_DIR / "internet.meta.yml"

paths = PathFinder(__file__)
REGIONS = [
    "Europe",
    "Asia",
    "North America",
    "South America",
    "Africa",
    "Oceania",
    "High-income countries",
    "Low-income countries",
    "Lower-middle-income countries",
    "Upper-middle-income countries",
]


def run(dest_dir: str) -> None:
    # Load auxiliary: income groups, regions
    ds_income = paths.load_dataset("income_groups")
    ds_regions = paths.load_dataset("regions")

    # Load WDI
    ds_wdi = paths.load_dataset("wdi")
    tb_wdi = ds_wdi.read("wdi", reset_metadata="keep_origins")
    tb_wdi = tb_wdi.loc[:, ["country", "year", "it_net_user_zs"]].dropna()

    # Load population data
    ds_population = paths.load_dataset("population")

    # Add population
    tb = add_population_to_table(tb_wdi, ds_population)

    # Rename columns
    tb = tb.rename(columns={"it_net_user_zs": "share_internet_users"})

    # Estimate absolute values
    tb["num_internet_users"] = tb["share_internet_users"] * tb["population"] / 100

    # Drop population column
    tb = tb.drop(columns=["population"])

    # Add regions
    tb = add_regions_to_table(
        tb,
        ds_income_groups=ds_income,
        ds_regions=ds_regions,
        regions=REGIONS,
        aggregations={"num_internet_users": "sum"},
    )

    # Add population back (for regions was not added)
    tb = add_population_to_table(tb, ds_population)

    # Estimate relative values for regions
    msk = tb["country"].isin(REGIONS)
    tb.loc[msk, "share_internet_users"] = tb.loc[msk, "num_internet_users"] / tb.loc[msk, "population"] * 100

    # Filter pre-2020 data
    tb = tb.loc[tb["year"] < 2022]

    #
    # Save outputs.
    #
    tables = [
        tb.format(["country", "year"], short_name="internet"),
    ]

    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define release year as the year in the version
RELEASE_YEAR = int(paths.version.split("-")[0])


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_ilostat = paths.load_dataset("ilostat")

    # Read table from meadow dataset.
    tb = ds_ilostat.read("ilostat", safe_types=False)
    tb_regions = ds_ilostat.read("table_of_contents_country")
    tb_indicator = ds_ilostat.read("dictionary_indicator")

    #
    # Process data.
    #
    # Rename columns
    tb = tb.rename(
        columns={
            "ref_area": "country",
            "time": "year",
        },
        errors="raise",
    )

    tb = add_ilo_regions(tb=tb, tb_regions=tb_regions)

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    tb = add_indicator_metadata(tb=tb, tb_indicator=tb_indicator)

    print(tb)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_ilostat.metadata)

    # Save garden dataset.
    ds_garden.save()


def add_ilo_regions(tb: Table, tb_regions: Table) -> Table:
    """
    Add ILO regions and subregions to the table.
    """

    tb = tb.copy()
    tb_regions = tb_regions.copy()

    # Filter freq
    tb_regions = tb_regions[tb_regions["freq"] == "A"].reset_index(drop=True)

    # Keep relevant columns
    tb_regions = tb_regions[["ref_area", "ilo_region_label", "ilo_subregion_detailed_label"]]

    # Rename columns
    tb_regions = tb_regions.rename(
        columns={
            "ref_area": "country",
            "ilo_region_label": "ilo_region",
            "ilo_subregion_detailed_label": "ilo_subregion",
        },
        errors="raise",
    )

    # Add year column as RELEASE_YEAR
    tb_regions["year"] = RELEASE_YEAR

    # Merge with the main table
    tb = pr.merge(tb, tb_regions, on=["country", "year"], how="outer")

    return tb


def add_indicator_metadata(tb: Table, tb_indicator: Table) -> Table:
    """
    Add indicator metadata to the table.
    """

    tb = tb.copy()

    print(list(tb.indicator.unique()))

    # Assert that there is info for every indicator
    assert set(
        tb["indicator"].unique()
    ).issubset(
        set(tb_indicator["indicator"].unique())
    ), f"Some indicators are missing in the indicator metadata table: {set(tb['indicator'].unique()) - set(tb_indicator['indicator'].unique())}"

    tb = pr.merge(tb, tb_indicator, on="indicator", how="left")

    return tb

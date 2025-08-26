"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define release year as the year in the version
RELEASE_YEAR = paths.version.split("-")[0].astype(int)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_ilostat = paths.load_dataset("ilostat")
    ds_regions = paths.load_dataset("table_of_contents_country")

    # Read table from meadow dataset.
    tb = ds_ilostat.read("ilostat", safe_types=False)
    tb_regions = ds_regions.read("table_of_contents_country")

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
    tb_regions = tb_regions[["ref_area", "ilo_region.label", "ilo_subregion_detailed.label"]]

    # Rename columns
    tb_regions = tb_regions.rename(
        columns={
            "ref_area": "country",
            "ilo_region.label": "ilo_region",
            "ilo_subregion_detailed.label": "ilo_subregion",
        },
        errors="raise",
    )

    # Add year column as RELEASE_YEAR
    tb_regions["year"] = RELEASE_YEAR

    # Merge with the main table
    tb = pr.merge(tb, tb_regions, on=["country", "year"], how="outer")

    return tb

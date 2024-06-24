"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("regions")

    # Read table from meadow dataset.
    tb = ds_meadow["regions"].reset_index()
    tb = tb.rename(columns={"country_or_area": "country"})
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = create_sdg_regions(tb)
    tb = tb[["country", "sdg_region"]]
    tb = tb.format(["country"], short_name="sdg_regions")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def create_sdg_regions(tb: Table) -> Table:
    """
    The SDG regions are an aggregation of the M49 sub-regions - this function maps them to the SDG regions.
    """
    tb = tb.dropna(subset=["sub_region_name"])
    region_dict = {
        "Sub-Saharan Africa": "Sub-Saharan Africa",
        "Northern Africa": "Northern Africa and Western Asia",
        "Western Asia": "Northern Africa and Western Asia",
        "Central Asia": "Central and Southern Asia",
        "Southern Asia": "Central and Southern Asia",
        "Eastern Asia": "Eastern and South-Eastern Asia",
        "South-eastern Asia": "Eastern and South-Eastern Asia",
        "Latin America and the Caribbean": "Latin America and the Caribbean",
        "Australia and New Zealand": "Australia and New Zealand",
        "Melanesia": "Oceania",
        "Micronesia": "Oceania",
        "Polynesia": "Oceania",
        "Eastern Europe": "Europe and Northern America",
        "Western Europe": "Europe and Northern America",
        "Southern Europe": "Europe and Northern America",
        "Northern Europe": "Europe and Northern America",
        "Northern America": "Europe and Northern America",
    }

    tb["sdg_region"] = tb["sub_region_name"].map(region_dict)
    assert tb["sdg_region"].notnull().all(), "Some sub-regions are not mapped to SDG regions."

    return tb

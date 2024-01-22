"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regions are defined with three-letter codes, so we map them to their full names.
REGIONS_MAPPING = {
    "EAP": "East Asia and Pacific",
    "ECA": "Europe and Central Asia",
    "LAC": "Latin America and the Caribbean",
    "MNA": "Middle East and North Africa",
    "OHI": "Other high income countries",
    "SAS": "South Asia",
    "SSA": "Sub-Saharan Africa",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("thousand_bins_distribution")

    # Read table from meadow dataset.
    tb = ds_meadow["thousand_bins_distribution"].reset_index()

    #
    # Process data.
    # Rename columns, regions and multiply pop by 1,000,000.
    tb = rename_columns_regions_and_multiply_pop(tb, REGIONS_MAPPING)

    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year", "region", "quantile"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def rename_columns_regions_and_multiply_pop(tb: Table, regions_mapping: dict) -> Table:
    """Rename columns, regions and multiply pop by 1,000,000."""
    # Rename columns
    tb = tb.rename(columns={"code": "country", "region_code": "region", "obs": "quantile", "welf": "avg"})

    # Rename region column with REGIONS_MAPPING. Assert that all regions are mapped.
    assert set(tb["region"].unique()) == set(REGIONS_MAPPING.keys()), "There are undefined regions"
    tb["region"] = tb["region"].map(REGIONS_MAPPING)

    # Multiply pop by 1,000,000
    tb["pop"] *= 1e6

    return tb

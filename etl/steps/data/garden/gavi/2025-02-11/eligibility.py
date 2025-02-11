"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("eligibility")
    ds_regions = paths.load_dataset("regions")
    # Read table from meadow dataset.
    tb = ds_meadow.read("eligibility")
    tb_regions = ds_regions.read("regions")
    # Not completely sure we should be listing _all_ other countries as not eligible, but Gavi only lists countries which are so we must assume a little
    tb_regions = tb_regions.query("region_type == 'country' and not is_historical")
    assert len(tb["year"].unique()) == 1, "More than one year in the Gavi dataset"
    tb_regions["year"] = tb["year"].unique()[0]
    tb_all_countries = tb_regions[["name", "year"]]
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Combine countries
    tb = pr.merge(tb_all_countries, tb, left_on=["name", "year"], right_on=["country", "year"], how="left")
    tb["phase"] = tb["phase"].fillna("Not eligible")
    tb = tb.drop(columns=["country"])
    tb = tb.rename(columns={"name": "country"})
    tb = tb.format(["country", "year"], short_name="eligibility")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

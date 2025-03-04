"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("health_expenditure_1993")

    # Read table from meadow dataset.
    tb_gdp = ds_meadow.read("gdp")
    tb_health_expenditure = ds_meadow.read("health_expenditure")

    #
    # Process data.
    #
    tb_gdp = geo.harmonize_countries(
        df=tb_gdp,
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
    )
    tb_health_expenditure = geo.harmonize_countries(
        df=tb_health_expenditure,
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
    )

    # Merge both tables
    tb = pr.merge(tb_gdp, tb_health_expenditure, on=["country", "year"])

    # Create the health_expenditure_share_gdp column, dividing health_expenditure by gdp.
    tb["health_expenditure_share_gdp"] = tb["health_expenditure"] / tb["gdp"] * 100

    # Drop gdp and health_expenditure columns.
    tb = tb.drop(columns=["gdp", "health_expenditure"])

    tb = tb.format(["country", "year"], short_name="health_expenditure_1993")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

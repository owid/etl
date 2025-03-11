"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("social_expenditure_1985")

    # Read table from meadow dataset.
    tb = ds_meadow.read("social_expenditure_1985")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Calculate the share of social expenditure in GDP.
    tb["share_gdp"] = (tb["total_social_expenditure_with_education"] - tb["education"]) / tb["gdp"] * 100

    # Improve table format.
    tb = tb.format(["country", "year"])

    # Keep only share_gdp column.
    tb = tb[["share_gdp"]]

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()

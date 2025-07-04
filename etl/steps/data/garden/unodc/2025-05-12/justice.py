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
    ds_meadow = paths.load_dataset("justice")

    # Read table from meadow dataset.
    tb = ds_meadow.read("justice")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Filter for Bribery category with specific indicators
    bribery_indicators = ["Persons convicted", "Persons prosecuted", "Persons arrested/cautioned/suspected"]
    tb = tb[(tb["category"] == "Bribery") & (tb["indicator"].isin(bribery_indicators)) & (tb["sex"] == "Total")]
    tb["sex"] = tb["sex"].replace(
        {
            "Total": "all individuals",
        }
    )
    tb["age"] = tb["age"].replace(
        {
            "Total": "all ages",
        }
    )

    tb = tb.drop(
        ["category", "dimension", "age", "sex"],
        axis=1,
    )
    # Improve table format.
    tb = tb.format(["country", "year", "indicator", "unit_of_measurement"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()

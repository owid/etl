"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


AGE_GROUP_MAPPING = {
    "young (<30)": "up to 29 years",
    "lower middle age (30-44)": "30-44 years",
    "upper middle age (45-59)": "45-59 years",
    "old (60+)": "60+ years",
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("happiness_by_age")

    # Read table from meadow dataset.
    tb = ds_meadow.read("happiness_by_age")

    tb["age_group"] = tb["age_group"].map(AGE_GROUP_MAPPING)

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Improve table format.
    tb = tb.format(["country", "age_group", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()

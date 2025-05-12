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
    ds_meadow = paths.load_dataset("corruption_barometer")

    # Read table from meadow dataset.
    tb = ds_meadow.read("corruption_barometer")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Map specific terms in the 'answer' column to desired values
    answer_mapping = {
        "AGREE": "Net agree",
        "DISAGREE": "Net disagree",
        "INCREASED": "Net increased",
        "DECREASED": "Net decreased",
        "TOTAL Bribery Rate, Excluding no contact": "Bribery rate, excluding no contact",
        "TOTAL Bribery Rate, Total population": "Bribery rate, total population",
        "TOTAL Contact Rate": "Bribery, contact rate",
    }

    tb["answer"] = tb["answer"].replace(answer_mapping).str.lower()

    # Improve table format.
    tb = tb.format(["country", "year", "question", "answer"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()

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
        "AGREE": "Agree or strongly agree",
        "DISAGREE": "Disagree or strongly disagree",
        "INCREASED": "Increased somewhat or a lot",
        "DECREASED": "Decreased somewhat or a lot",
        "NET NONE/ SOME": "Some or none of them",
        "NET MOST/ALL": "Most or all of them",
        "NET BADLY": "Fairly or very badly",
        "NET WELL": "Fairly or very well",
        "None": "None of them",
        "Don't know / Refused": "Don't know or refused",
        "TOTAL Bribery Rate, Excluding no contact": "in the past 12 months had contact with a public official",
        "TOTAL Bribery Rate, Total population": "all individuals",
        "TOTAL Contact Rate": "contact rate",
    }

    tb["answer"] = tb["answer"].replace(answer_mapping).str.lower().str.capitalize()
    # Improve table format.
    tb = tb.format(["country", "year", "question", "answer"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()

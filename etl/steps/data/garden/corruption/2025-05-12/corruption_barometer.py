"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = ["Europe", "Asia", "North America", "South America", "Africa", "Oceania", "World"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("corruption_barometer")
    ds_population = paths.load_dataset("population")
    ds_regions = paths.load_dataset("regions")

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
        "TOTAL Bribery Rate, Excluding no contact": "Bribery rate among those who had contact with a public official in the past 12 months",
        "TOTAL Bribery Rate, Total population": "Bribery rate among all respondents irrespective of contact with a public official in the past 12 months",
        "TOTAL Contact Rate": "Contact rate with a public official in the past 12 months",
    }

    tb["answer"] = tb["answer"].replace(answer_mapping).str.lower().str.capitalize()

    # Add regional aggregates by adding population, multiplying the "share" by the population, adding regions and then dividing by the population.
    tb = geo.add_population_to_table(tb, ds_population)
    tb["number"] = tb["value"] * tb["population"]
    tb = geo.add_regions_to_table(
        tb=tb,
        index_columns=["country", "year", "question", "answer", "institution"],
        ds_regions=ds_regions,
        regions=REGIONS,
    )

    tb["value"] = tb["number"] / tb["population"]
    tb = tb.drop(columns=["number", "population"])

    # Improve table format.
    tb = tb.format(["country", "year", "question", "answer", "institution"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()

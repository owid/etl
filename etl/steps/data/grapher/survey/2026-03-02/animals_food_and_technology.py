"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Responses for the 7-point Likert agree scale.
LIKERT_AGREE_RESPONSES = [
    "Strongly disagree",
    "Disagree",
    "Somewhat disagree",
    "No opinion",
    "Somewhat agree",
    "Agree",
    "Strongly agree",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("animals_food_and_technology")

    # Read aggregated responses table.
    tb = ds_garden.read("animals_food_and_technology_responses")

    #
    # Process data.
    #
    # Keep only questions with the 7-point Likert agree scale.
    tb = tb[tb["response"].isin(LIKERT_AGREE_RESPONSES)].copy()

    # Pivot: one column per response category.
    tb = tb.pivot(
        index=["country", "year", "question", "question_title", "question_short"],
        columns="response",
        values="share",
    ).reset_index()

    # Use question_short as the entity (hack to adapt to grapher's country/year format).
    tb = tb.drop(columns=["country", "question", "question_title"])
    tb = tb.rename(columns={"question_short": "country"})

    # Set titles on value columns.
    for col in tb.columns:
        if col not in ["country", "year"]:
            tb[col].metadata.title = f"Share of respondents that answered '{col.replace('_', ' ').title()}'"
            tb[col].metadata.description_processing = "Survey questions have been shortened for visualization purposes."

    # Format table.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()

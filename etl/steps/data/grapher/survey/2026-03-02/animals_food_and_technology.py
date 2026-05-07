"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import VariablePresentationMeta

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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
    # Keep only grouped questions (Likert agree scale).
    tb = tb[tb["group"].notna()].copy()

    # Read survey intro from the garden table's metadata.
    survey_intro = tb["share"].metadata.description_key[0]

    # Build per-group description_key from the data's (group, question) pairs.
    group_description_keys = {}
    for group, group_df in tb.groupby("group"):
        questions = sorted(group_df["question"].unique())
        questions_sublist = f"The questions related to {group.lower()} are:\n" + "\n".join(
            f'- "{q}"' for q in questions
        )
        group_description_keys[group] = [survey_intro, questions_sublist]

    # Pivot: one column per (group, response) combination.
    tb["group_response"] = tb["group"] + " - " + tb["response"]
    tb = tb.pivot(
        index=["country", "year", "question", "question_title", "question_short", "group"],
        columns="group_response",
        values="share",
    ).reset_index()

    # Use question_short as the entity.
    tb = tb.drop(columns=["country", "question", "question_title", "group"])
    tb = tb.rename(columns={"question_short": "country"})

    # Set metadata on value columns.
    for col in tb.columns:
        if col not in ["country", "year"]:
            # Parse group and response from column name.
            group, response = col.rsplit(" - ", 1)
            title = f"{group} - Share of respondents that answered '{response}'"
            tb[col].metadata.title = title
            tb[col].metadata.description_key = group_description_keys[group]
            tb[col].metadata.display = {"name": response}
            tb[col].metadata.presentation = VariablePresentationMeta(title_public=title)

    # Format table.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()

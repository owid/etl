"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import VariablePresentationMeta

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

# Groups of Likert agree questions (must match garden step).
QUESTION_GROUPS = {
    "Bans on factory farming and slaughterhouses": [
        "ban_factory_farming",
        "ban_slaughterhouses",
        "ban_farming",
        "ban_slaughterhouses_rev",
    ],
    "Livestock treatment and animal pain": [
        "sentience",
        "important_issue",
        "humane_soc",
        "humane_self",
        "discomfort",
        "discomfort_rev",
        "sentience_rev",
    ],
    "Dietary choices and meat-eating": [
        "plant_diet_self",
        "plant_diet_soc",
        "personal_choice",
        "price_plant_diet_soc",
        "price_plant_diet_self",
        "price_cultured_soc",
        "price_cultured_self",
        "plant_diet_soc_rev",
    ],
}

# Full survey question text per variable (for description_key).
VARIABLE_QUESTIONS = {
    "ban_factory_farming": "I support a ban on the factory farming of animals.",
    "ban_slaughterhouses": "I support a ban on slaughterhouses.",
    "ban_farming": "I support a ban on animal farming.",
    "ban_slaughterhouses_rev": "I oppose a ban on slaughterhouses.",
    "sentience": "Farmed animals have roughly the same ability to feel pain and discomfort as humans.",
    "important_issue": "The factory farming of animals is one of the most important social issues in the world today.",
    "humane_soc": "Most farmed animals are treated well. For example, the animals are given enough space and kept in good health.",
    "humane_self": "The animal-based foods I purchase (meat, dairy, and/or eggs) usually come from animals that are treated humanely. For example, the animals are given enough space and kept in good health.",
    "discomfort": "I have some discomfort with the way animals are used in the food industry.",
    "discomfort_rev": "I am comfortable with the way animals are used in the food industry.",
    "sentience_rev": "Farmed animals have substantially less ability to feel pain and discomfort than humans.",
    "plant_diet_self": "I am currently trying to consume fewer animal-based foods (meat, dairy, and/or eggs) and more plant-based foods (fruits, grains, beans, and/or vegetables).",
    "plant_diet_soc": "People should consume fewer animal-based foods (meat, dairy, and/or eggs) and more plant-based foods (fruits, grains, beans, and/or vegetables).",
    "personal_choice": "Whether to eat animals or be vegetarian is a personal choice, and nobody has the right to tell me which one they think I should do.",
    "price_plant_diet_soc": "When plant-based meat, dairy, and egg foods are the same price as conventional foods, people should eat more plant-based foods and fewer conventional foods.",
    "price_plant_diet_self": "When plant-based meat, dairy, and egg foods are the same price as conventional foods, I would prefer to eat more plant-based foods and fewer conventional foods.",
    "price_cultured_soc": "When cultured meat, dairy, and egg foods are the same price as conventional foods, people should eat more cultured foods and fewer conventional foods.",
    "price_cultured_self": "When cultured meat, dairy, and egg foods are the same price as conventional foods, I would prefer to eat more cultured foods and fewer conventional foods.",
    "plant_diet_soc_rev": "People should consume fewer plant-based foods (fruits, grains, beans, and/or vegetables) and more animal-based foods (meat, dairy, and/or eggs).",
}

# Survey description for description_key.
SURVEY_INTRO = "The survey has been repeated in 2017, 2019, 2020, 2021, 2023, and 2025, with 7,165 participants across six waves. Responses are weighted to be representative of the US population."


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

    # Build per-group description_key with the relevant full question texts.
    group_description_keys = {}
    for group, var_names in QUESTION_GROUPS.items():
        questions = [VARIABLE_QUESTIONS[v] for v in var_names if v in VARIABLE_QUESTIONS]
        questions_sublist = f"The questions related to {group.lower()} are:\n" + "\n".join(
            f'- "{q}"' for q in questions
        )
        group_description_keys[group] = [SURVEY_INTRO, questions_sublist]

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

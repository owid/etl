"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Codebook mappings from:
# https://data.mendeley.com/public-files/datasets/k3xcndsswk/files/259f15a8-d72f-4356-b8c5-fedb38149873/file_downloaded

LIKERT_AGREE_7 = {
    1: "Strongly disagree",
    2: "Disagree",
    3: "Somewhat disagree",
    4: "No opinion",
    5: "Somewhat agree",
    6: "Agree",
    7: "Strongly agree",
}

LIKERT_LIKELY_7 = {
    1: "Very unlikely",
    2: "Unlikely",
    3: "Somewhat unlikely",
    4: "No opinion",
    5: "Somewhat likely",
    6: "Likely",
    7: "Very likely",
}

POLITICS_5 = {
    1: "Very liberal",
    2: "Liberal",
    3: "Moderate",
    4: "Conservative",
    5: "Very conservative",
}

YES_NO = {
    1: "Yes",
    0: "No",
}

EDUCATION = {
    1: "Grade 4 or less",
    2: "Grade 5 to 8",
    3: "Grade 9 to 11",
    4: "Grade 12 (no diploma) high school graduate",
    5: "Regular high school diploma",
    6: "GED or alternative credential",
    7: "Less than 1 year of college credit",
    8: "1 or more years college credit, no degree",
    9: "Associate's degree",
    10: "Bachelor's degree",
    11: "Master's degree",
    12: "Professional degree beyond bachelors",
    13: "Doctorate degree",
}

DONATE = {
    0: "$0",
    1: "$1",
    2: "$2",
    3: "$3",
    4: "$4",
    5: "$5",
    6: "$6",
    7: "$7",
    8: "$8",
    9: "$9",
    10: "$10",
}

FARM_CONDITIONS = {
    1: "Gotten worse",
    2: "Stayed about the same",
    3: "Improved",
}

PERCENT_FACTORY_FARMED = {
    10: "0-20%",
    30: "20-40%",
    50: "40-60%",
    70: "60-80%",
    90: "80-100%",
}

VOTE = {
    1: "Consider a candidate's position on factory farming as just one of many important factors",
    2: "Not see factory farming as a major issue",
    3: "Only vote for a candidate who shares your views on factory farming",
}

YEARS_END_FARMING = {
    10: "0-20 years",
    30: "20-40 years",
    50: "40-60 years",
    70: "60-80 years",
    90: "80-100 years",
    110: "100+ years",
}

BAN_FACTORY_FARMING_ARG = {
    1: "Ban factory farming animals",
    7: "Continue factory farming animals",
}

BAN_FARMING_ARG = {
    1: "Ban farming animals",
    7: "Continue farming animals",
}

BAN_LABELS = {
    1: "Ban meat-like terms",
    7: "Allow meat-like terms",
}

DIVEST = {
    1: "Support divestment from factory farming",
    7: "Oppose divestment from factory farming",
}

# String categorical variables to also aggregate (already text in the source data).
STRING_CATEGORICAL = [
    "gender",
    "region",
    "raceethnicity",
    "diet",
    "political_party",
    "religion",
]

# Variable titles (from codebook "Variable label" column).
VARIABLE_TITLES = {
    "education": "Education",
    "plant_diet_soc": "Society plant based",
    "plant_diet_self": "Personal plant based",
    "discomfort": "Discomfort with industry",
    "sentience": "Animal sentience",
    "personal_choice": "Personal choice",
    "important_issue": "Important issue",
    "ban_factory_farming": "Ban factory farming",
    "ban_slaughterhouses": "Ban slaughterhouses",
    "ban_farming": "Ban animal farming",
    "donate": "Donate",
    "demonstration": "Demonstration",
    "price_plant_diet_soc": "Society plant based (price)",
    "price_plant_diet_self": "Personal plant based (price)",
    "price_cultured_soc": "Society cultured meat",
    "price_cultured_self": "Personal cultured meat",
    "humane_soc": "Animal treatment",
    "humane_self": "Purchase humane",
    "ban_slaughterhouses_rev": "Ban slaughterhouses (rev)",
    "farm_conditions": "Farm conditions",
    "discomfort_rev": "Discomfort with industry (rev)",
    "plant_diet_soc_rev": "Society plant based (rev)",
    "percent_factory_farmed": "Percent factory farmed",
    "sentience_rev": "Animal sentience (rev)",
    "vote": "Vote",
    "years_end_farming": "Years end farming",
    "factory_farming_politics": "Factory farming politics",
    "politics": "Political views",
    "veg": "Vegetarian or vegan",
    "ban_factory_farming_arg": "Ban factory farming (pro/con)",
    "ban_farming_arg": "Ban animal farming (pro/con)",
    "ban_labels": "Ban labels (pro/con)",
    "divest": "Divest (pro/con)",
    "politics_econ": "Political views (economic)",
    "politics_soc": "Political views (social)",
    "college": "College degree",
    "vid_image": "Seen video or image",
    "seen_protest": "Seen protest",
    "seen_doc": "Seen documentary",
    "animal_conv": "Conversation",
    "ate_burger": "Plant-based burger",
    "ate_c_burger": "Cultured burger",
    "veg_restaurant": "Veg restaurant",
    "time_animals": "Time with animals",
    "read_book": "Read book",
    "diet_openness_veg": "Diet openness - veg",
    "diet_openness_meat": "Diet openness - meat only",
    "gender": "Gender",
    "region": "Region",
    "raceethnicity": "Race/ethnicity",
    "diet": "Diet",
    "political_party": "Political party",
    "religion": "Religion",
}

# Shortened question text for visualization purposes.
VARIABLE_SHORT_QUESTIONS = {
    "plant_diet_soc": "People should eat fewer animal-based and more plant-based foods",
    "plant_diet_self": "I am trying to eat fewer animal-based and more plant-based foods",
    "discomfort": "I have discomfort with how animals are used in the food industry",
    "sentience": "Farmed animals can feel pain and discomfort like humans",
    "personal_choice": "To eat animals or be vegetarian is a personal choice",
    "important_issue": "Factory farming is one of the most important social issues today",
    "ban_factory_farming": "I support a ban on factory farming",
    "ban_slaughterhouses": "I support a ban on slaughterhouses",
    "ban_farming": "I support a ban on animal farming",
    "donate": "How much of $10 would you donate to help farmed animals?",
    "demonstration": "How likely would you be to join a demonstration against factory farming?",
    "price_plant_diet_soc": "At equal prices, people should eat more plant-based foods",
    "price_plant_diet_self": "At equal prices, I would prefer more plant-based foods",
    "price_cultured_soc": "At equal prices, people should eat more cultured foods",
    "price_cultured_self": "At equal prices, I would prefer more cultured foods",
    "humane_soc": "Most farmed animals are treated well",
    "humane_self": "The animal products I buy come from humanely treated animals",
    "ban_slaughterhouses_rev": "I oppose a ban on slaughterhouses",
    "farm_conditions": "Have conditions for farmed animals improved in recent years?",
    "discomfort_rev": "I am comfortable with how animals are used in the food industry",
    "plant_diet_soc_rev": "People should eat fewer plant-based and more animal-based foods",
    "percent_factory_farmed": "What percentage of farmed animals live on factory farms?",
    "sentience_rev": "Farmed animals feel substantially less pain than humans",
    "vote": "How would factory farming affect your vote?",
    "years_end_farming": "When will humanity stop using animals for food?",
    "factory_farming_politics": 'Is opposition to factory farming "liberal" or "conservative"?',
    "politics": "How would you describe your political views?",
    "ban_factory_farming_arg": "Ban vs. continue factory farming",
    "ban_farming_arg": "Ban vs. continue all animal farming",
    "ban_labels": "Ban vs. allow meat-like terms for plant-based products",
    "divest": "Should universities divest from factory farming?",
    "politics_econ": "Political views on economic issues",
    "politics_soc": "Political views on social issues",
    "vid_image": "Saw graphic images or video of farmed animals",
    "seen_protest": "Saw a factory farming protest",
    "seen_doc": "Watched a documentary on animal farming",
    "animal_conv": "Had a conversation about factory farming",
    "ate_burger": "Ate a plant-based burger",
    "ate_c_burger": "Ate a cultured meat burger",
    "veg_restaurant": "Ate at a vegetarian or vegan restaurant",
    "time_animals": "Spent time with farmed animals",
    "read_book": "Read a book about factory farming or plant-based food",
    "diet_openness_veg": "Open to trying a vegetarian or vegan diet",
    "diet_openness_meat": "Open to trying a meat-only diet",
}

# Full survey question text for each variable (from codebook "Variable survey question" column).
# For demographics and generated variables, falls back to the short label.
VARIABLE_QUESTIONS = {
    "plant_diet_soc": "People should consume fewer animal-based foods (meat, dairy, and/or eggs) and more plant-based foods (fruits, grains, beans, and/or vegetables).",
    "plant_diet_self": "I am currently trying to consume fewer animal-based foods (meat, dairy, and/or eggs) and more plant-based foods (fruits, grains, beans, and/or vegetables).",
    "discomfort": "I have some discomfort with the way animals are used in the food industry.",
    "sentience": "Farmed animals have roughly the same ability to feel pain and discomfort as humans.",
    "personal_choice": "Whether to eat animals or be vegetarian is a personal choice, and nobody has the right to tell me which one they think I should do.",
    "important_issue": "The factory farming of animals is one of the most important social issues in the world today.",
    "ban_factory_farming": "I support a ban on the factory farming of animals.",
    "ban_slaughterhouses": "I support a ban on slaughterhouses.",
    "ban_farming": "I support a ban on animal farming.",
    "donate": "Suppose you were given $10 and allowed to donate any amount of it to an effective non-profit organization that works to help farmed animals, keeping the rest for yourself. How much of this $10 would you donate?",
    "demonstration": "Suppose a public demonstration against the problems of factory farming occurred near where you live and your friend asked you to come demonstrate with her. If this demonstration fit into your schedule, how likely would you be to join and help demonstrate?",
    "price_plant_diet_soc": "When plant-based meat, dairy, and egg foods are the same price as conventional foods, people should eat more plant-based foods and fewer conventional foods.",
    "price_plant_diet_self": "When plant-based meat, dairy, and egg foods are the same price as conventional foods, I would prefer to eat more plant-based foods and fewer conventional foods.",
    "price_cultured_soc": "When cultured meat, dairy, and egg foods are the same price as conventional foods, people should eat more cultured foods and fewer conventional foods.",
    "price_cultured_self": "When cultured meat, dairy, and egg foods are the same price as conventional foods, I would prefer to eat more cultured foods and fewer conventional foods.",
    "humane_soc": "Most farmed animals are treated well. For example, the animals are given enough space and kept in good health.",
    "humane_self": "The animal-based foods I purchase (meat, dairy, and/or eggs) usually come from animals that are treated humanely. For example, the animals are given enough space and kept in good health.",
    "ban_slaughterhouses_rev": "I oppose a ban on slaughterhouses.",
    "farm_conditions": "In the past few years, do you think conditions farmed animals live in have...?",
    "discomfort_rev": "I am comfortable with the way animals are used in the food industry.",
    "plant_diet_soc_rev": "People should consume fewer plant-based foods (fruits, grains, beans, and/or vegetables) and more animal-based foods (meat, dairy, and/or eggs).",
    "percent_factory_farmed": "If you had to guess, what percentage of farmed animals (e.g. cows, chickens, fish) do you think live on factory farms?",
    "sentience_rev": "Farmed animals have substantially less ability to feel pain and discomfort than humans.",
    "vote": "Thinking about how the factory farming issue might affect your vote for major offices, would you...?",
    "years_end_farming": "If you had to guess, at how many years from now do you think humanity will stop using animals for food production?",
    "factory_farming_politics": 'Do you see opposition to factory farming as a "liberal" or "conservative" viewpoint?',
    "politics": "How would you describe your political views?",
    "ban_factory_farming_arg": "Some people think that we should ban factory farming animals, to reduce harm to humans and animals. Others think that we should continue factory farming animals, to provide low-cost meat to consumers. Where would you place yourself on this scale?",
    "ban_farming_arg": "Some people think that we should ban all animal farming and transition to plant-based and cultured foods, to reduce harm to humans and animals. Others think that we should keep using animals for food, to provide the conventional meat consumers are used to eating. Where would you place yourself on this scale?",
    "ban_labels": 'Some people think that the government should ban companies from using meat-like terms such as "veggie burger" or "plant-based sausage" to refer to products made without animals. Others disagree and think that these labels are accurate descriptions of the products. Where would you place yourself on this scale?',
    "divest": "Some people think that colleges and universities should divest from factory farming. Others think that colleges and universities should continue to invest in factory farming. Where would you place yourself on this scale?",
    "politics_econ": "Thinking about economic issues, would you say your views on economic issues are...?",
    "politics_soc": "Thinking about social issues, would you say your views on social issues are...?",
    "vid_image": "Saw unpleasant or graphic images and/or video of farmed animals",
    "seen_protest": "Saw a factory farming protest, demonstration, march, or similar",
    "seen_doc": "Watched a full-length documentary related to animal farming, meat consumption, or similar",
    "animal_conv": "Had a conversation about factory farming and/or plant-based food with a friend",
    "ate_burger": "Ate a plant-based burger that was made to taste like a beef burger",
    "ate_c_burger": "Ate a cultured (i.e. cell-based, cultivated, clean) meat burger",
    "veg_restaurant": "Ate at a vegetarian or vegan restaurant",
    "time_animals": "Spent time with cows, pigs, chickens, or farmed fish",
    "read_book": "Read a book that discussed factory farming or plant-based food",
    "diet_openness_veg": "Open to trying vegetarian or vegan diet in the next few years",
    "diet_openness_meat": "Open to trying meat-only diet in the next few years",
}

# Map each coded variable to its mapping.
VARIABLE_MAPPINGS = {
    "education": EDUCATION,
    "plant_diet_soc": LIKERT_AGREE_7,
    "plant_diet_self": LIKERT_AGREE_7,
    "discomfort": LIKERT_AGREE_7,
    "sentience": LIKERT_AGREE_7,
    "personal_choice": LIKERT_AGREE_7,
    "important_issue": LIKERT_AGREE_7,
    "ban_factory_farming": LIKERT_AGREE_7,
    "ban_slaughterhouses": LIKERT_AGREE_7,
    "ban_farming": LIKERT_AGREE_7,
    "donate": DONATE,
    "demonstration": LIKERT_LIKELY_7,
    "price_plant_diet_soc": LIKERT_AGREE_7,
    "price_plant_diet_self": LIKERT_AGREE_7,
    "price_cultured_soc": LIKERT_AGREE_7,
    "price_cultured_self": LIKERT_AGREE_7,
    "humane_soc": LIKERT_AGREE_7,
    "humane_self": LIKERT_AGREE_7,
    "ban_slaughterhouses_rev": LIKERT_AGREE_7,
    "farm_conditions": FARM_CONDITIONS,
    "discomfort_rev": LIKERT_AGREE_7,
    "plant_diet_soc_rev": LIKERT_AGREE_7,
    "percent_factory_farmed": PERCENT_FACTORY_FARMED,
    "sentience_rev": LIKERT_AGREE_7,
    "vote": VOTE,
    "years_end_farming": YEARS_END_FARMING,
    "factory_farming_politics": POLITICS_5,
    "politics": POLITICS_5,
    "veg": YES_NO,
    "ban_factory_farming_arg": BAN_FACTORY_FARMING_ARG,
    "ban_farming_arg": BAN_FARMING_ARG,
    "ban_labels": BAN_LABELS,
    "divest": DIVEST,
    "politics_econ": POLITICS_5,
    "politics_soc": POLITICS_5,
    "college": YES_NO,
    "vid_image": YES_NO,
    "seen_protest": YES_NO,
    "seen_doc": YES_NO,
    "animal_conv": YES_NO,
    "ate_burger": YES_NO,
    "ate_c_burger": YES_NO,
    "veg_restaurant": YES_NO,
    "time_animals": YES_NO,
    "read_book": YES_NO,
    "diet_openness_veg": YES_NO,
    "diet_openness_meat": YES_NO,
}


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("animals_food_and_technology")

    # Read table from meadow dataset.
    tb = ds_meadow.read("animals_food_and_technology")

    #
    # Process data.
    #
    # Map numeric codes to human-readable labels.
    for col, mapping in VARIABLE_MAPPINGS.items():
        if col in tb.columns:
            tb[col] = tb[col].map(mapping)

    # This is a US-only survey; add country column.
    tb["country"] = "United States"

    #
    # Build aggregated table: weighted share of each response per question and year.
    #
    all_categorical = list(VARIABLE_MAPPINGS.keys()) + STRING_CATEGORICAL
    # Keep only categorical columns that exist in the data.
    all_categorical = [c for c in all_categorical if c in tb.columns]

    # Melt to long format: one row per (respondent, question, response).
    tb_long = tb.melt(
        id_vars=["country", "year", "weight"],
        value_vars=all_categorical,
        var_name="question",
        value_name="response",
    )

    # Drop rows where the response is missing (not all questions asked in all years).
    tb_long = tb_long.dropna(subset=["response"])

    # Add title, short question, and full question text columns.
    tb_long["question_title"] = tb_long["question"].map(VARIABLE_TITLES)
    tb_long["question_short"] = tb_long["question"].map(
        lambda v: VARIABLE_SHORT_QUESTIONS.get(v, VARIABLE_TITLES.get(v, v))
    )
    tb_long["question"] = tb_long["question"].map(lambda v: VARIABLE_QUESTIONS.get(v, VARIABLE_TITLES.get(v, v)))

    # Compute weighted share per (year, question, response).
    tb_agg = tb_long.groupby(
        ["country", "year", "question", "question_title", "question_short", "response"], observed=True, as_index=False
    ).agg({"weight": "sum"})
    # Normalize to percentages within each (year, question).
    totals = tb_agg.groupby(["country", "year", "question"], observed=True)["weight"].transform("sum")
    tb_agg["weight"] = (tb_agg["weight"] / totals * 100).round(1)
    tb_agg = tb_agg.rename(columns={"weight": "share"})

    # Set short name and format.
    tb_agg.metadata.short_name = "animals_food_and_technology_responses"
    tb_agg = tb_agg.format(["country", "year", "question", "question_title", "question_short", "response"])

    # Add metadata to the share column programmatically.
    tb_agg["share"].metadata.description_key = [
        "The survey has been repeated in 2017, 2019, 2020, 2021, 2023, and 2025, with 7,165 participants across six waves. Responses are weighted to be representative of the US population.",
    ] + list(VARIABLE_QUESTIONS.values())

    # Format individual-level table.
    tb = tb.format(["country", "year", "entry_id"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb, tb_agg], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()

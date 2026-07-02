"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from tabulate import tabulate

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Set table format when printing
TABLEFMT = "pretty"

# Set margin for checks
MARGIN = 0.5

# Define question suffixes

IMPORTANT_IN_LIFE_QUESTIONS = [
    "important_in_life_family",
    "important_in_life_friends",
    "important_in_life_leisure_time",
    "important_in_life_politics",
    "important_in_life_work",
    "important_in_life_religion",
]

INTERESTED_IN_POLITICS_QUESTIONS = ["interested_politics"]

POLITICAL_ACTION_QUESTIONS = [
    "political_action_signing_a_petition",
    "political_action_joining_in_boycotts",
    "political_action_attending_peaceful_demonstrations",
    "political_action_joining_unofficial_strikes",
]

ENVIRONMENT_VS_ECONOMY_QUESTIONS = ["env_ec"]

INCOME_EQUALITY_QUESTIONS = ["eq_ineq"]

SCHWARTZ_QUESTIONS = [
    "new_ideas",
    "rich",
    "secure",
    "good_time",
    "help_others",
    "success",
    "risks",
    "behave",
    "respect_environment",
    "tradition",
]

WORK_VS_LEISURE_QUESTIONS = ["lei_vs_wk"]

WORK_QUESTIONS = ["work_is_a_duty", "work_should_come_first"]

MOST_SERIOUS_PROBLEM_QUESTIONS = ["most_serious"]

JUSTIFIABLE_QUESTIONS = [
    "claiming_benefits",
    "stealing_property",
    "parents_beating_children",
    "violence_against_other_people",
    "avoiding_fare_on_public_transport",
    "cheating_on_taxes",
    "accepting_a_bribe",
    "homosexuality",
    "prostitution",
    "abortion",
    "divorce",
    "euthanasia",
    "suicide",
    "having_casual_sex",
    "sex_before_marriage",
    "invitro_fertilization",
    "death_penalty",
    "man_beating_wife",
    "political_violence",
]

WORRIES_QUESTIONS = ["losing_job", "not_being_able_to_provide_good_education", "war", "terrorist_attack", "civil_war"]

HAPPINESS_QUESTIONS = ["happy"]

NEIGHBORS_QUESTIONS = [
    "neighbors_different_race",
    "neighbors_heavy_drinkers",
    "neighbors_immigrant_foreign_workers",
    "neighbors_aids",
    "neighbors_drug_addicts",
    "neighbors_homosexuals",
    "neighbors_different_religion",
    "neighbors_gypsies",
    "neighbors_unmarried_couples",
    "neighbors_different_language",
]

HOMOSEXUAL_PARENTS_QUESTIONS = ["homosx_prnts"]

POLITICAL_SYSTEMS_QUESTIONS = [
    "experts_make_decisions",
    "army_rule",
    "democratic_political_system",
]

ESSENTIAL_CHARACTERISTIC_OF_DEMOCRACY_QUESTIONS = [
    "governments_tax_the_rich_and_subsidize_the_poor",
    "religious_authorities_interpret_the_laws",
    "people_choose_their_leaders_in_free_elections",
    "people_receive_state_aid_for_unemployment",
    "army_takes_over_when_government_is_incompetent",
    "civil_rights_protects_peoples_liberty_against_oppression",
    "women_have_the_same_rights_as_men",
    "the_state_makes_peoples_incomes_equal",
    "people_obey_their_rulers",
]

RELIGION_HOW_OFTEN_SERVICES_QUESTIONS = ["attend_religious_services"]
RELIGION_HOW_OFTEN_PRAY_QUESTIONS = ["pray"]

IMPORTANCE_OF_GOD_QUESTIONS = ["god"]

JOBS_SCARCE_QUESTIONS = [
    "men_more_right_to_a_job_than_women",
    "priority_to_nationals_over_immigrants",
]

GENDER_ROLES_QUESTIONS = [
    "housewife_just_as_fulfilling",
    "men_better_political_leaders",
    "university_more_important_for_a_boy",
    "pre_school_child_suffers_with_working_mother",
    "men_better_business_executives",
]

NEIGHBORHOOD_FREQUENCY_QUESTIONS = [
    "robberies",
    "police_or_military_interfere",
    "racist_behavior",
    "drug_sale_in_streets",
]

# H008_02 uses a different (often/sometimes/rarely/never) scale than the H002 frequency block
FELT_UNSAFE_QUESTIONS = ["felt_unsafe_at_home"]

SECURITY_ACTIONS_QUESTIONS = [
    "didnt_carry_much_money",
    "preferred_not_to_go_out_at_night",
    "carried_a_weapon",
    "victim_of_a_crime",
    "family_victim_of_a_crime",
]

NEIGHBORHOOD_SECURITY_QUESTIONS = ["secure_neighborhood"]

HUMAN_RIGHTS_RESPECT_QUESTIONS = ["respect_human_rights"]

# Continuous index (kept on its native 0-1 scale, so it must be excluded from the 0 -> null replacement)
WELZEL_EQUALITY_INDEX_COLUMNS = ["avg_score_welzel_equality"]

# Media information source questions (5-point frequency: daily/weekly/monthly/less than monthly/never)
MEDIA_QUESTIONS = [
    "information_source_daily_newspaper",
    "information_source_talk_with_friends_or_colleagues",
    "information_source_tv_news",
    "information_source_radio_news",
    "information_source_mobile_phone",
    "information_source_email",
    "information_source_internet",
]

# How close you feel questions (4-point: very close/close/not very close/not close at all)
CLOSENESS_QUESTIONS = [
    "how_close_you_feel_to_continent",
    "how_close_you_feel_to_world",
    "how_close_you_feel_to_village_town_or_city",
    "how_close_you_feel_to_county_region_district",
    "how_close_you_feel_to_country",
]

# Science and technology agreement questions (10-point: completely disagree to completely agree)
TECHNOLOGY_QUESTIONS = [
    "science_and_technology_make_life_healthier_and_easier",
    "science_and_technology_bring_more_opportunities_for_next_generation",
    "we_depend_too_much_on_science_and_not_enough_on_faith",
    "science_breaks_down_ideas_of_right_and_wrong",
]

# Science and technology: is the world better or worse off (10-point), single custom block
SCIENCE_WORLD_QUESTIONS = ["science_world"]

# Aims of country (multinomial: one choice out of four named goals)
AIMS_COUNTRY_QUESTIONS = ["aims_of_country_first_choice", "aims_of_country_second_choice"]

# Aims of respondent (multinomial: one choice out of four named goals)
AIMS_RESPONDENT_QUESTIONS = ["aims_of_respondent_first_choice", "aims_of_respondent_second_choice"]

# Most important goal (multinomial: one choice out of four named goals)
MOST_IMPORTANT_QUESTIONS = ["most_important_first_choice", "most_important_second_choice"]

# Feel concerned about humankind (5-point), single custom block
HUMANKIND_CONCERN_QUESTIONS = ["humankind"]

# --- World Values Survey (WVS) table ---
# WVS-only questions (asked in WVS but absent from IVS), built into a separate `world_values_survey`
# table inside this same dataset. Each entry is the column suffix produced by wvs_create_file.do.
WVS_WOMEN_INCOME_QUESTIONS = ["women_income"]  # D066_01, 5-point agree
WVS_CONFIDENCE_ELECTIONS_QUESTIONS = ["elections"]  # E069_64, 4-point confidence
WVS_TERRORISM_QUESTIONS = ["terrorism"]  # F114E, 10-point justifiable


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset, regions and population
    ds_meadow = paths.load_dataset("integrated_values_surveys")

    # Read table from meadow dataset.
    tb = ds_meadow.read("integrated_values_surveys")

    #
    # Process data.

    # Drop columns
    tb = drop_indicators_and_replace_nans(tb)

    # The IVS and WVS tables share one countries mapping file, so each leaves some entries unused;
    # silence the (expected) "unused values in mapping" warning.
    tb = paths.regions.harmonize_names(tb, warn_on_unused_countries=False)

    # Sanity checks
    tb = sanity_checks(tb)

    tb = tb.format(["country", "year"])

    #
    # Build the World Values Survey (WVS) table from its own meadow dataset (WVS-only questions).
    #
    ds_meadow_wvs = paths.load_dataset("world_values_survey")
    tb_wvs = ds_meadow_wvs.read("world_values_survey")

    tb_wvs = process_wvs(tb_wvs)

    tb_wvs = paths.regions.harmonize_names(tb_wvs, warn_on_unused_countries=False)

    # Sanity checks (recodes must sum to 100%)
    tb_wvs = sanity_checks_wvs(tb_wvs)

    tb_wvs = tb_wvs.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset. The dataset holds two
    # tables: the Integrated Values Surveys table and the World Values Survey table.
    ds_garden = paths.create_dataset(
        tables=[tb, tb_wvs], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def drop_indicators_and_replace_nans(tb: Table) -> Table:
    """
    Drop indicators/questions not useful enough for OWID's purposes (too few data points, for too few countries, etc.)
    Also, replace zero values appearing in IVS data with nulls. This did not happen in WVS data.
    """

    # Drop selected variables
    vars_to_drop = [
        "trust_first_not_very_much",
        "trust_personally_not_very_much",
        "confidence_confidence_in_cer_with_australia",  # Only New Zealand
        "confidence_free_commerce_treaty__tratado_de_libre_comercio",  # Only Mexico and Chile
        "confidence_united_american_states_organization",  # Only Peru and Dominican Republic
        "confidence_education_system",  # most of the data is in 1993
        "confidence_social_security_system",  # most of the data is in 1993
        "confidence_andean_pact",  # Only Venezuela
        "confidence_local_regional_government",  # Only Argentina and Puerto Rico
        "confidence_organization_of_american_states__oae",  # Only Peru
    ]
    tb = tb.drop(columns=vars_to_drop)

    # Define columns containing "no_answer" and "dont_know"
    no_answer_cols = [cols for cols in tb.columns if "no_answer" in cols]
    dont_know_cols = [cols for cols in tb.columns if "dont_know" in cols]

    # Replace zero values with nulls, except for columns containing "no_answer" and "dont_know"
    # The Welzel equality sub-index is a continuous 0-1 index where 0 is a valid value, so it is excluded too.
    subset_cols = tb.columns.difference(no_answer_cols + dont_know_cols + WELZEL_EQUALITY_INDEX_COLUMNS)
    tb[subset_cols] = tb[subset_cols].replace(0, float("nan"))

    # Replace nulls in Schwartz questions by 0 when the main answer is not null
    tb = solve_nulls_values_in_schwartz_questions(
        tb=tb,
        questions=SCHWARTZ_QUESTIONS,
        main_answer="like_me_agg",
        other_answers=[
            "not_like_me_agg",
            "very_much_like_me",
            "like_me",
            "somewhat_like_me",
            "a_little_like_me",
            "not_like_me",
            "not_at_all_like_me",
            "dont_know",
            "no_answer",
        ],
    )

    # Replace 0 by null for don't know columns if the rest of columns are null
    # For important in life questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=IMPORTANT_IN_LIFE_QUESTIONS,
        answers=[
            "very",
            "rather",
            "not_very",
            "notatall",
        ],
    )

    # For interested in politics question
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=INTERESTED_IN_POLITICS_QUESTIONS,
        answers=["very", "somewhat", "not_very", "not_at_all"],
    )

    # For political action questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=POLITICAL_ACTION_QUESTIONS,
        answers=["have_done", "might_do", "never"],
    )

    # For environment vs. economy question
    tb = replace_dont_know_by_null(
        tb=tb, questions=ENVIRONMENT_VS_ECONOMY_QUESTIONS, answers=["environment", "economy", "other_answer"]
    )

    # For income equality question
    tb = replace_dont_know_by_null(
        tb=tb, questions=INCOME_EQUALITY_QUESTIONS, answers=["equality", "neutral", "inequality"]
    )

    # For "Schwartz" questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=SCHWARTZ_QUESTIONS,
        answers=["very_much_like_me", "like_me", "somewhat_like_me", "a_little_like_me", "not_like_me"],
    )

    # For "Work vs. leisure" question
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=WORK_VS_LEISURE_QUESTIONS,
        answers=["work", "leisure", "neutral"],
    )

    # For work questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=WORK_QUESTIONS,
        answers=["strongly_agree", "agree", "neither", "disagree", "strongly_disagree"],
    )

    # For most serious problem of the world question
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=MOST_SERIOUS_PROBLEM_QUESTIONS,
        answers=["poverty", "women_discr", "sanitation", "education", "pollution"],
    )

    # For justifiable questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=JUSTIFIABLE_QUESTIONS,
        answers=["never_just_agg", "always_just_agg", "neutral"],
    )

    # For worries questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=WORRIES_QUESTIONS,
        answers=["very_much", "a_great_deal", "not_much", "not_at_all"],
    )

    # For happiness questions
    tb = replace_dont_know_by_null(
        tb=tb, questions=HAPPINESS_QUESTIONS, answers=["very", "quite", "not_very", "not_at_all"]
    )

    # For neighbors questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=NEIGHBORS_QUESTIONS,
        answers=["mentioned", "notmentioned"],
    )

    # For homosexual parents questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=HOMOSEXUAL_PARENTS_QUESTIONS,
        answers=["strongly_agree", "agree", "neither", "disagree", "strongly_disagree"],
    )

    # For democracy satisfaction questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=["satisfied_democracy"],
        answers=[
            "not",
            "",
            "neutral",
        ],
    )

    # For political systems questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=POLITICAL_SYSTEMS_QUESTIONS,
        answers=["very_good", "fairly_good", "fairly_bad", "very_bad"],
    )

    # For essential characteristic of democracy questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=ESSENTIAL_CHARACTERISTIC_OF_DEMOCRACY_QUESTIONS,
        answers=[
            "not_essential_dem_agg",
            "essential_dem_agg",
            "neutral_essential_dem",
        ],
    )

    # For democracy importance question
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=["important_democracy"],
        answers=["not", "", "neutral"],
    )

    # For democraticness in own country question
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=["democratic"],
        answers=["not", "yes", "neutral"],
    )

    # For honest elections making a difference question
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=["honest_elections_make_a_difference"],
        answers=[
            "very_important",
            "rather_important",
            "not_very_important",
            "not_at_all_important",
        ],
    )

    # For religion how often questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=RELIGION_HOW_OFTEN_SERVICES_QUESTIONS,
        answers=[
            "up_to_once_month",
            "special_holydays",
            "once_year",
            "less_than_once_year",
        ],
    )
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=RELIGION_HOW_OFTEN_PRAY_QUESTIONS,
        answers=[
            "up_to_sev_week",
            "only_services",
            "special_holydays",
            "once_year",
            "less_than_once_year",
        ],
    )

    # For importance of god question
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=["important_god"],
        answers=[
            "",
            "not",
            "neutral",
        ],
    )

    # For jobs scarce questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=JOBS_SCARCE_QUESTIONS,
        answers=["agree", "disagree", "neither"],
    )

    # For gender roles questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=GENDER_ROLES_QUESTIONS,
        answers=["strongly_agree", "agree", "disagree", "strongly_disagree"],
    )

    # For neighborhood frequency questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=NEIGHBORHOOD_FREQUENCY_QUESTIONS,
        answers=["very_frequently", "quite_frequently", "not_frequently", "not_at_all_frequently"],
    )

    # For security actions and crime victim questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=SECURITY_ACTIONS_QUESTIONS,
        answers=["yes", "no"],
    )

    # For secure in neighborhood question
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=NEIGHBORHOOD_SECURITY_QUESTIONS,
        answers=["very", "quite", "not_very", "not_at_all"],
    )

    # For respect for human rights question
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=HUMAN_RIGHTS_RESPECT_QUESTIONS,
        answers=["great_deal", "some", "not_much", "none_at_all"],
    )

    # For felt unsafe from crime at home question
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=FELT_UNSAFE_QUESTIONS,
        answers=["often", "sometimes", "rarely", "never"],
    )

    # For media information source questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=MEDIA_QUESTIONS,
        answers=["daily", "weekly", "monthly", "less_than_monthly", "never"],
    )

    # For how close you feel questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=CLOSENESS_QUESTIONS,
        answers=["very_close", "close", "not_very_close", "not_close_at_all"],
    )

    # For science and technology agreement questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=TECHNOLOGY_QUESTIONS,
        answers=["agree", "neutral", "disagree"],
    )

    # For science and technology world better/worse off question
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=SCIENCE_WORLD_QUESTIONS,
        answers=["better_off", "neutral", "worse_off"],
    )

    # For aims of country questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=AIMS_COUNTRY_QUESTIONS,
        answers=["economic_growth", "strong_defence", "more_say", "beautiful_cities"],
    )

    # For aims of respondent questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=AIMS_RESPONDENT_QUESTIONS,
        answers=["maintaining_order", "give_people_say", "fighting_prices", "freedom_of_speech"],
    )

    # For most important goal questions
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=MOST_IMPORTANT_QUESTIONS,
        answers=["stable_economy", "humane_society", "ideas_over_money", "fight_crime"],
    )

    # For feel concerned about humankind question
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=HUMANKIND_CONCERN_QUESTIONS,
        answers=["very_much", "much", "to_a_certain_extent", "not_so_much", "not_at_all"],
    )

    # Drop rows with all null values in columns not country and year
    tb = tb.dropna(how="all", subset=tb.columns.difference(["country", "year"]))

    return tb


def replace_dont_know_by_null(tb: Table, questions: list[str], answers: list[str]) -> Table:
    """
    Replace empty don't know answers when the rest of the answers is null
    """
    for q in questions:
        # Add q to each member of answers
        answers_by_question = [f"{a}_{q}" if a else q for a in answers]

        # Check if all columns in answers_by_question are null
        tb["null_check"] = tb[answers_by_question].notnull().all(axis=1)

        # if null_check is False and f"dont_know_{q}" is 0, set f"dont_know_{q}" as null
        tb.loc[(~tb["null_check"]) & (tb[f"dont_know_{q}"] == 0), f"dont_know_{q}"] = float("nan")

    # Remove null_check
    tb = tb.drop(columns=["null_check"])

    return tb


def solve_nulls_values_in_schwartz_questions(
    tb: Table,
    questions: list[str],
    main_answer: str,
    other_answers: list[str],
) -> Table:
    """
    Replace null values in Schwartz questions by 0 when the main answer is not null
    """

    for q in questions:
        # Add q to each member of answers
        main_answer_by_question = f"{main_answer}_{q}"
        other_answers_by_question = [f"{a}_{q}" for a in other_answers]

        # Assign 0 to each other_answers_by_question when it's null and when main_answer_by_question is not null
        for a in other_answers_by_question:
            tb.loc[(tb[main_answer_by_question].notnull()) & (tb[a].isnull()), a] = 0

    return tb


def sanity_checks(tb: Table) -> Table:
    """
    Perform sanity checks on the data
    """
    # Check if the sum of the answers is 100
    # For important in life questions
    tb = check_sum_100(
        tb=tb,
        questions=IMPORTANT_IN_LIFE_QUESTIONS,
        answers=["very", "rather", "not_very", "notatall", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For interested in politics question
    tb = check_sum_100(
        tb=tb,
        questions=INTERESTED_IN_POLITICS_QUESTIONS,
        answers=["very", "somewhat", "not_very", "not_at_all", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For political action questions
    tb = check_sum_100(
        tb=tb,
        questions=POLITICAL_ACTION_QUESTIONS,
        answers=["have_done", "might_do", "never", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For environment vs. economy question
    tb = check_sum_100(
        tb=tb,
        questions=ENVIRONMENT_VS_ECONOMY_QUESTIONS,
        answers=["environment", "economy", "other_answer", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For income equality question
    tb = check_sum_100(
        tb=tb,
        questions=INCOME_EQUALITY_QUESTIONS,
        answers=["equality", "neutral", "inequality", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For "Schwartz" questions
    tb = check_sum_100(
        tb=tb,
        questions=SCHWARTZ_QUESTIONS,
        answers=[
            "very_much_like_me",
            "like_me",
            "somewhat_like_me",
            "a_little_like_me",
            "not_like_me",
            "not_at_all_like_me",
            "dont_know",
            "no_answer",
        ],
        margin=MARGIN,
    )

    # For "Work vs. leisure" question
    tb = check_sum_100(
        tb=tb,
        questions=WORK_VS_LEISURE_QUESTIONS,
        answers=["work", "leisure", "neutral", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For work questions
    tb = check_sum_100(
        tb=tb,
        questions=WORK_QUESTIONS,
        answers=["strongly_agree", "agree", "neither", "disagree", "strongly_disagree", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For most serious problem of the world question
    tb = check_sum_100(
        tb=tb,
        questions=MOST_SERIOUS_PROBLEM_QUESTIONS,
        answers=["poverty", "women_discr", "sanitation", "education", "pollution", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For justifiable questions
    tb = check_sum_100(
        tb=tb,
        questions=JUSTIFIABLE_QUESTIONS,
        answers=["never_just_agg", "always_just_agg", "neutral", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For worries questions
    tb = check_sum_100(
        tb=tb,
        questions=WORRIES_QUESTIONS,
        answers=["very_much", "a_great_deal", "not_much", "not_at_all", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For happiness questions
    tb = check_sum_100(
        tb=tb,
        questions=HAPPINESS_QUESTIONS,
        answers=["very", "quite", "not_very", "not_at_all", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For neighbors questions
    tb = check_sum_100(
        tb=tb,
        questions=NEIGHBORS_QUESTIONS,
        answers=["mentioned", "notmentioned", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For homosexual parents questions
    tb = check_sum_100(
        tb=tb,
        questions=HOMOSEXUAL_PARENTS_QUESTIONS,
        answers=[
            "strongly_agree",
            "agree",
            "neither",
            "disagree",
            "strongly_disagree",
            "dont_know",
            "no_answer",
        ],
        margin=MARGIN,
    )

    # For democracy satisfaction questions
    tb = check_sum_100(
        tb=tb,
        questions=["democracy"],
        answers=["not_satisfied", "satisfied", "neutral_satisfied", "dont_know_satisfied", "no_answer_satisfied"],
        margin=MARGIN,
    )

    # For political systems questions
    tb = check_sum_100(
        tb=tb,
        questions=POLITICAL_SYSTEMS_QUESTIONS,
        answers=["very_good", "fairly_good", "fairly_bad", "very_bad", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For essential characteristic of democracy questions
    tb = check_sum_100(
        tb=tb,
        questions=ESSENTIAL_CHARACTERISTIC_OF_DEMOCRACY_QUESTIONS,
        answers=[
            "not_essential_dem_agg",
            "essential_dem_agg",
            "neutral_essential_dem",
            "dont_know",
            "no_answer",
        ],
        margin=MARGIN,
    )

    # For democracy importance question
    tb = check_sum_100(
        tb=tb,
        questions=["democracy"],
        answers=["not_important", "important", "neutral_important", "dont_know_important", "no_answer_important"],
        margin=MARGIN,
    )

    # For democraticness in own country question
    tb = check_sum_100(
        tb=tb,
        questions=["democratic"],
        answers=["not", "yes", "neutral", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For honest elections making a difference question
    tb = check_sum_100(
        tb=tb,
        questions=["honest_elections_make_a_difference"],
        answers=[
            "very_important",
            "rather_important",
            "not_very_important",
            "not_at_all_important",
            "dont_know",
            "no_answer",
        ],
        margin=MARGIN,
    )

    # For religion how often questions
    tb = check_sum_100(
        tb=tb,
        questions=RELIGION_HOW_OFTEN_SERVICES_QUESTIONS,
        answers=[
            "up_to_once_month",
            "special_holydays",
            "once_year",
            "less_than_once_year",
            "dont_know",
            "no_answer",
        ],
        margin=MARGIN,
    )
    tb = check_sum_100(
        tb=tb,
        questions=RELIGION_HOW_OFTEN_PRAY_QUESTIONS,
        answers=[
            "up_to_sev_week",
            "only_services",
            "special_holydays",
            "once_year",
            "less_than_once_year",
            "dont_know",
            "no_answer",
        ],
        margin=MARGIN,
    )

    # For importance of god question
    tb = check_sum_100(
        tb=tb,
        questions=IMPORTANCE_OF_GOD_QUESTIONS,
        answers=[
            "important",
            "not_important",
            "neutral_important",
            "dont_know_important",
            "no_answer_important",
        ],
        margin=MARGIN,
    )

    # For jobs scarce questions
    tb = check_sum_100(
        tb=tb,
        questions=JOBS_SCARCE_QUESTIONS,
        answers=["agree", "disagree", "neither", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For gender roles questions
    tb = check_sum_100(
        tb=tb,
        questions=GENDER_ROLES_QUESTIONS,
        answers=["strongly_agree", "agree", "disagree", "strongly_disagree", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For neighborhood frequency questions
    tb = check_sum_100(
        tb=tb,
        questions=NEIGHBORHOOD_FREQUENCY_QUESTIONS,
        answers=[
            "very_frequently",
            "quite_frequently",
            "not_frequently",
            "not_at_all_frequently",
            "dont_know",
            "no_answer",
        ],
        margin=MARGIN,
    )

    # For security actions and crime victim questions
    tb = check_sum_100(
        tb=tb,
        questions=SECURITY_ACTIONS_QUESTIONS,
        answers=["yes", "no", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For secure in neighborhood question
    tb = check_sum_100(
        tb=tb,
        questions=NEIGHBORHOOD_SECURITY_QUESTIONS,
        answers=["very", "quite", "not_very", "not_at_all", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For respect for human rights question
    tb = check_sum_100(
        tb=tb,
        questions=HUMAN_RIGHTS_RESPECT_QUESTIONS,
        answers=["great_deal", "some", "not_much", "none_at_all", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For felt unsafe from crime at home question
    tb = check_sum_100(
        tb=tb,
        questions=FELT_UNSAFE_QUESTIONS,
        answers=["often", "sometimes", "rarely", "never", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For media information source questions
    tb = check_sum_100(
        tb=tb,
        questions=MEDIA_QUESTIONS,
        answers=["daily", "weekly", "monthly", "less_than_monthly", "never", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For how close you feel questions
    tb = check_sum_100(
        tb=tb,
        questions=CLOSENESS_QUESTIONS,
        answers=["very_close", "close", "not_very_close", "not_close_at_all", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For science and technology agreement questions
    tb = check_sum_100(
        tb=tb,
        questions=TECHNOLOGY_QUESTIONS,
        answers=["agree", "neutral", "disagree", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For science and technology world better/worse off question
    tb = check_sum_100(
        tb=tb,
        questions=SCIENCE_WORLD_QUESTIONS,
        answers=["better_off", "neutral", "worse_off", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For aims of country questions
    tb = check_sum_100(
        tb=tb,
        questions=AIMS_COUNTRY_QUESTIONS,
        answers=["economic_growth", "strong_defence", "more_say", "beautiful_cities", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For aims of respondent questions
    tb = check_sum_100(
        tb=tb,
        questions=AIMS_RESPONDENT_QUESTIONS,
        answers=[
            "maintaining_order",
            "give_people_say",
            "fighting_prices",
            "freedom_of_speech",
            "dont_know",
            "no_answer",
        ],
        margin=MARGIN,
    )

    # For most important goal questions
    tb = check_sum_100(
        tb=tb,
        questions=MOST_IMPORTANT_QUESTIONS,
        answers=["stable_economy", "humane_society", "ideas_over_money", "fight_crime", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    # For feel concerned about humankind question
    tb = check_sum_100(
        tb=tb,
        questions=HUMANKIND_CONCERN_QUESTIONS,
        answers=[
            "very_much",
            "much",
            "to_a_certain_extent",
            "not_so_much",
            "not_at_all",
            "dont_know",
            "no_answer",
        ],
        margin=MARGIN,
    )

    return tb


def check_sum_100(tb: Table, questions: list[str], answers: list[str], margin: float) -> Table:
    """
    Check if the sum of the answers is 100
    """
    for q in questions:
        # Add q to each member of answers
        answers_by_question = [f"{a}_{q}" for a in answers]

        # Check if all columns in answers_by_question are null
        tb["sum_check"] = tb[answers_by_question].sum(axis=1)

        # Create mask to check if sum is 100
        mask = ((tb["sum_check"] <= 100 - margin) | (tb["sum_check"] >= 100 + margin)) & tb[
            answers_by_question
        ].notnull().all(axis=1)
        tb_error = tb[mask].reset_index(drop=True).copy()

        assert tb_error.empty, (
            f"{len(tb_error)} answers for {q} are not adding up to 100%:\n"
            f"{tabulate(tb_error[['country', 'year'] + answers_by_question + ['sum_check']], headers='keys', tablefmt=TABLEFMT, floatfmt='.1f')}"
        )

    # Remove sum_check
    tb = tb.drop(columns=["sum_check"])

    return tb


def process_wvs(tb: Table) -> Table:
    """
    Process the World Values Survey table (WVS-only questions).

    Unlike IVS, WVS shares contain no spurious zeros: country-years where a question was not asked are
    missing after the Stata merge (not zero), so no 0 -> null replacement is applied here. We only null out
    "don't know" where the rest of the answers are null (parity with IVS), then drop all-null rows.
    """
    # Replace 0 by null for "don't know" columns if the rest of the columns are null.
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=WVS_WOMEN_INCOME_QUESTIONS,
        answers=["strongly_agree", "agree", "neither", "disagree", "strongly_disagree"],
    )
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=WVS_CONFIDENCE_ELECTIONS_QUESTIONS,
        answers=["great_deal", "quite_a_lot", "not_very_much", "none_at_all"],
    )
    tb = replace_dont_know_by_null(
        tb=tb,
        questions=WVS_TERRORISM_QUESTIONS,
        answers=["never_just_agg", "always_just_agg", "neutral"],
    )

    # Drop rows with all null values in columns other than country and year.
    tb = tb.dropna(how="all", subset=tb.columns.difference(["country", "year"]))

    return tb


def sanity_checks_wvs(tb: Table) -> Table:
    """
    Perform sanity checks on the World Values Survey table (the mutually-exclusive categories plus
    "don't know" and "no answer" must add up to 100%).
    """
    tb = check_sum_100(
        tb=tb,
        questions=WVS_WOMEN_INCOME_QUESTIONS,
        answers=["strongly_agree", "agree", "neither", "disagree", "strongly_disagree", "dont_know", "no_answer"],
        margin=MARGIN,
    )
    tb = check_sum_100(
        tb=tb,
        questions=WVS_CONFIDENCE_ELECTIONS_QUESTIONS,
        answers=["great_deal", "quite_a_lot", "not_very_much", "none_at_all", "dont_know", "no_answer"],
        margin=MARGIN,
    )
    tb = check_sum_100(
        tb=tb,
        questions=WVS_TERRORISM_QUESTIONS,
        answers=["never_just_agg", "always_just_agg", "neutral", "dont_know", "no_answer"],
        margin=MARGIN,
    )

    return tb

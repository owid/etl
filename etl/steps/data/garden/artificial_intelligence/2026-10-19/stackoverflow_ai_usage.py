"""Garden step for Stack Overflow Developer Survey – AI tool usage.

Computes, for each survey year, the share of professional developers who use AI tools, from individual responses.

The survey changed over time, so both the definition of a professional developer and of an AI tool user depend on the year:
- 2023–2025: professional developers answered "I am a developer by profession" (MainBranch). AI tool users answered "Yes"
  (2023–2024) or "Yes, I use AI tools daily / weekly / monthly or infrequently" (2025) to "Do you currently use AI tools in
  your development process?" (AISelect).
- 2026: MainBranch was dropped, so professional developers are those who answered "I create code as a primary part of
  my job" (CodeMain). AISelect became "Do you currently use AI tools or AI agents in your role at work?", where
  respondents tick the kinds of AI tools they use, or "I don’t use AI tools". AI tool users are those who ticked at least
  one kind of tool, which is also how the survey itself decides who gets the follow-up questions on AI.

These definitions reproduce the shares Stack Overflow published for professional developers in 2023–2025.
"""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Survey years expected in the data.
YEARS = [2023, 2024, 2025, 2026]

# Answer that identifies professional developers, for each year: (column, answer).
PROFESSIONAL_DEVELOPER = {
    2023: ("main_branch", "I am a developer by profession"),
    2024: ("main_branch", "I am a developer by profession"),
    2025: ("main_branch", "I am a developer by profession"),
    2026: ("code_main", "I create code as a primary part of my job"),
}

# Answers to the AI tool usage question that count as using AI tools, for each year.
YES_2023_2024 = {"Yes"}
YES_2025 = {"Yes, I use AI tools daily", "Yes, I use AI tools weekly", "Yes, I use AI tools monthly or infrequently"}
NO_2023_2025 = {"No, but I plan to soon", "No, and I don't plan to"}
# In 2026, the question is multiple choice, and answers are the ticked options separated by ";".
TOOLS_2026 = {
    "AI coding assistants or coding agents",
    "General-purpose AI chat tools",
    "AI agents or automated workflows",
    "Internal AI tools built by my company",
}
NO_2026 = "I don’t use AI tools"


def run() -> None:
    """Create garden dataset."""
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("stackoverflow_ai_usage")
    tb = ds_meadow.read("stackoverflow_ai_usage")

    sanity_check_inputs(tb)

    #
    # Process data.
    #
    # Step 1: Keep professional developers who answered the AI tool usage question.
    is_professional = tb["year"] < 0
    for year, (column, answer) in PROFESSIONAL_DEVELOPER.items():
        is_professional |= (tb["year"] == year) & tb[column].isin([answer])
    tb = tb[is_professional & tb["ai_select"].notna()].reset_index(drop=True)

    # Step 2: Flag respondents who use AI tools, following each year's version of the question.
    answers = tb["ai_select"].astype(str)
    tb["uses_ai_tools"] = answers.isin(YES_2023_2024 | YES_2025)
    is_2026 = tb["year"] == 2026
    tb.loc[is_2026, "uses_ai_tools"] = (
        answers[is_2026].str.split(";").apply(lambda ticked: bool(set(ticked) & TOOLS_2026))
    )

    # Step 3: Compute the share of AI tool users, and the number of respondents, per year.
    tb = tb.groupby("year", as_index=False).agg(
        yes_overall=("uses_ai_tools", "mean"), n_respondents=("uses_ai_tools", "size")
    )
    tb["yes_overall"] = (tb["yes_overall"] * 100).round(2)

    # Step 4: Describe the survey years and the range of respondents per year in the metadata, then drop the counts.
    survey_years = f"{tb['year'].min()}–{tb['year'].max()}"
    response_range = f"{tb['n_respondents'].min():,.0f} to {tb['n_respondents'].max():,.0f}"
    tb = tb.drop(columns=["n_respondents"])

    tb["country"] = "World"
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    sanity_check_outputs(tb)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[tb], yaml_params={"response_range": response_range, "survey_years": survey_years}
    )
    ds_garden.save()


def sanity_check_inputs(tb: Table) -> None:
    assert sorted(tb["year"].unique()) == YEARS, f"Unexpected survey years: {sorted(tb['year'].unique())}"

    # Each year's professional-developer answer must exist, so that a relabeled answer doesn't silently empty a year.
    for year, (column, answer) in PROFESSIONAL_DEVELOPER.items():
        assert tb.loc[tb["year"] == year, column].isin([answer]).sum() > 5_000, (
            f"Too few professional developers in {year}."
        )

    # Every answer to the AI tool usage question must be a known one, so that new options are not silently counted
    # as non-users.
    answers = {year: set(tb.loc[tb["year"] == year, "ai_select"].dropna().astype(str)) for year in YEARS}
    for year in [2023, 2024]:
        assert answers[year] == YES_2023_2024 | NO_2023_2025, f"Unexpected answers in {year}: {answers[year]}"
    assert answers[2025] == YES_2025 | NO_2023_2025, f"Unexpected answers in 2025: {answers[2025]}"
    ticked_2026 = {option for answer in answers[2026] for option in answer.split(";")}
    assert ticked_2026 == TOOLS_2026 | {NO_2026}, f"Unexpected answers in 2026: {ticked_2026}"


def sanity_check_outputs(tb: Table) -> None:
    years = tb.index.get_level_values("year")
    assert sorted(years) == YEARS, f"Unexpected years in output: {sorted(years)}"
    assert tb["yes_overall"].notna().all(), "Missing share of AI tool users."
    assert tb["yes_overall"].between(0, 100).all(), "Share of AI tool users outside [0, 100]."

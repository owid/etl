"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)


QUESTIONS_EXPECTED = {
    "Count On to Help",
    "Enjoy the Work You Do Every Day",
    "Exciting Life or Calm Life",
    "Experience Calmness Yesterday",
    "Feel at Peace With Life",
    "Focus on Taking Care of Themselves or Taking Care of Others",
    "Health Problems",
    "Main Purpose in Life",
    "Many Choices in Type of Work",
    "Struggling Index",
    "Suffering Index",
    "Thriving Index",
    "Various Aspects of Life in Balance",
    "Work Significantly Improves the Lives of Others Outside of Household",
}


def run() -> None:
    ds_meadow = paths.load_dataset("global_wellbeing")
    tb = ds_meadow.read("global_wellbeing")

    # Fix a typo introduced upstream.
    tb["dimension"] = tb["dimension"].str.replace("villAge", "village")

    tb = paths.regions.harmonize_names(tb, country_col="country", countries_file=paths.country_mapping_path)
    tb["year"] = 2020

    questions = set(tb["question"])
    unknown = questions.difference(QUESTIONS_EXPECTED)
    assert not unknown, f"Unknown questions! {unknown}"

    # Pivot questions to columns.
    tb_pivot = tb.pivot(index=["country", "year", "dimension", "answer"], columns="question", values="share")

    # Split into Gallup indices (no "answer" dimension) and survey questions.
    index_cols = [c for c in tb_pivot.columns if c.endswith("Index")]

    tb_index = tb_pivot[index_cols].copy()
    tb_index = tb_index.droplevel("answer").dropna(how="all")
    tb_index = tb_index * 100

    tb_questions: Table = tb_pivot.drop(columns=index_cols).copy()
    tb_questions = tb_questions * 100

    # Preserve table-level short_name on both outputs.
    tb_questions.metadata.short_name = paths.short_name
    tb_index.metadata.short_name = f"{paths.short_name}_index"

    ds_garden = paths.create_dataset(
        tables=[tb_questions, tb_index], default_metadata=ds_meadow.metadata
    )
    ds_garden.save()

"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Expected questions in the dataset.
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


def run(dest_dir: str) -> None:
    log.info("global_wellbeing.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("global_wellbeing")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["global_wellbeing"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    # Reset index
    df = df.reset_index()

    #
    # Process data.
    #
    log.info("global_wellbeing: harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)

    # Add year
    df["year"] = 2020

    # Sanity check on questions
    log.info("global_wellbeing: check questions are as expected")
    questions = set(df["question"])
    assert not (
        questions_unknown := questions.difference(QUESTIONS_EXPECTED)
    ), f"Unknown questions! {questions_unknown}"

    # Pivot to have questions as columnns
    log.info("global_wellbeing: harmonize_countries")
    df = df.pivot(index=["country", "year", "dimension", "answer"], columns="question", values="share")

    # Get table with the Gallup indices: suffering, struggling, thriving indices
    # We define an additional table since these columns don't have the dimension "answer"
    log.info("global_wellbeing: build df with gallup indices")
    df_index = df.filter(regex=r".* Index")
    df_index = df_index.droplevel(3).dropna(how="all")
    df_index = 100 * df_index
    # Get table with the survey questions
    log.info("global_wellbeing: build df with survey questions")
    df_questions = df[[col for col in df.columns if col not in df_index.columns]]
    # Get percent from rates
    df_questions = 100 * df_questions

    # Create a new table with the processed data.
    log.info("global_wellbeing: create tables")
    tb_questions = Table(df_questions, short_name=paths.short_name)
    tb_index = Table(df_index, short_name=f"{paths.short_name}_index")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    log.info("global_wellbeing: create dictionary")
    ds_garden = create_dataset(dest_dir, tables=[tb_questions, tb_index], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("global_wellbeing.end")

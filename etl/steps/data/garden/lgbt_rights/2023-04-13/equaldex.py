"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process data from Equaldex."""

    # Make the dataframe wide by pivoting on the issue column.
    df = df.pivot(index=["country", "year"], columns="issue", values=["value_formatted"]).reset_index(drop=False)

    # Flatten the multi-index column names, but keep country and year as separate columns.
    df.columns = ["_".join(col).strip() for col in df.columns.values]
    df = df.rename(columns={"country_": "country", "year_": "year"})

    # replace all column names with - with _
    df.columns = df.columns.str.replace("-", "_")

    # Create a new list of id for each issue, ordered from more liberal to more restrictive (useful for grapher charts)
    id_homosexuality = {
        "Legal": "01. Legal",
        "Varies by Region": "02. Varies by region",
        "Ambiguous": "03. Ambiguous",
        "Male illegal, female legal": "04. Male illegal, female legal or uncertain",
        "Male illegal, female uncertain": "04. Male illegal, female legal or uncertain",
        "Illegal (other penalty)": "05. Illegal, prison or other penalty",
        "Illegal (imprisonment as punishment)": "05. Illegal, prison or other penalty",
        "Illegal (up to life in prison as punishment)": "05. Illegal, prison or other penalty",
        "Illegal (death penalty as punishment)": "06. Illegal (death penalty as punishment)",
    }
    id_changing_gender = {
        "Legal, surgery not required": "01. Legal, surgery not required",
        "Legal, but requires surgery": "02. Legal, but requires surgery",
        "Varies by Region": "03. Varies by region",
        "Ambiguous": "04. Ambiguous",
        "Illegal": "05. Illegal",
    }
    id_marriage = {
        "Legal": "01. Legal",
        "Civil unions": "02. Civil union or other partnership",
        "Other type of partnership": "02. Civil union or other partnership",
        "Foreign same-sex marriages recognized only": "03. Foreign same-sex marriages recognized only",
        "Unregistered cohabitation": "04. Unregistered cohabitation",
        "Varies by Region": "05. Varies by region",
        "Ambiguous": "06. Ambiguous",
        "Unrecognized": "07. Unrecognized",
        "Unrecognized, same-sex marriage and civil unions banned": "08. Unrecognized, same-sex marriage and civil unions banned",
        "Not legal": "09. Not legal",
    }

    id_adoption = {
        "Legal": "01. Legal",
        "Married couples only": "02. Married couples only",
        "Step-child adoption only": "03. Step-child adoption only",
        "Single only": "04. Single only",
        "Ambiguous": "05. Ambiguous",
        "Illegal": "06. Illegal",
    }

    id_age_of_consent = {
        "Equal": "01. Equal",
        "Varies by Region": "02. Varies by region",
        "Ambiguous": "03. Ambiguous",
        "Unequal": "04. Unequal",
    }

    id_blood = {
        "Legal": "01. Legal",
        "Varies by Region": "02. Varies by region",
        "Ambiguous": "03. Ambiguous",
        "Banned (3-month deferral)": "04. Banned (3-month deferral)",
        "Banned (6-month deferral)": "05. Banned (6-month deferral)",
        "Banned (1-year deferral)": "06. Banned (1-year deferral)",
        "Banned (5-year deferral)": "07. Banned (5-year deferral)",
        "Banned (indefinite deferral)": "08. Banned (indefinite deferral)",
    }

    id_censorship = {
        "No censorship": "01. No censorship",
        "Varies by Region": "02. Varies by region",
        "Ambiguous": "03. Ambiguous",
        "Other punishment": "04. Other punishment",
        "Fine as punishment": "05. Fine as punishment",
        "State-enforced": "06. State-enforced",
        "Imprisonment as punishment": "07. Imprisonment as punishment",
    }

    id_conversion_therapy = {
        "Banned": "01. Banned",
        "Varies by Region": "02. Varies by region",
        "Ambiguous": "03. Ambiguous",
        "Not banned": "04. Not banned",
    }

    id_discrimination = {
        "Illegal": "01. Illegal",
        "Illegal in some contexts": "02. Illegal in some contexts",
        "Varies by Region": "03. Varies by region",
        "No protections": "04. No protections",
    }

    id_employment_discrimination = {
        "Sexual orientation and gender identity": "01. Sexual orientation and gender identity",
        "Sexual orientation only": "02. Sexual orientation only",
        "Varies by Region": "03. Varies by region",
        "Ambiguous": "04. Ambiguous",
        "No protections": "05. No protections",
    }

    id_housing_discrimination = {
        "Sexual orientation and gender identity": "01. Sexual orientation and gender identity",
        "Sexual orientation only": "02. Sexual orientation only",
        "Varies by Region": "03. Varies by region",
        "Ambiguous": "04. Ambiguous",
        "No protections": "05. No protections",
    }

    id_military = {
        "Legal": "01. Legal",
        "Don't Ask, Don't Tell": "02. Don't Ask, Don't Tell",
        "Lesbians, gays, bisexuals permitted, transgender people banned": "03. Lesbians, gays, bisexuals permitted, transgender people banned",
        "Ambiguous": "04. Ambiguous",
        "Illegal": "05. Illegal",
    }

    id_non_binary_gender_recognition = {
        "Recognized": "01. Recognized",
        "Intersex only": "02. Intersex only",
        "Varies by Region": "03. Varies by region",
        "Ambiguous": "04. Ambiguous",
        "Not legally recognized": "05. Not legally recognized",
    }

    # Match id variables with the new ids in new columns
    # TODO: Consider updating the map function to datautils.dataframes.map_series (when there's no match it copies the original value)
    df["homosexuality"] = df["value_formatted_homosexuality"].map(id_homosexuality)
    df["changing_gender"] = df["value_formatted_changing_gender"].map(id_changing_gender)
    df["marriage"] = df["value_formatted_marriage"].map(id_marriage)
    df["adoption"] = df["value_formatted_adoption"].map(id_adoption)
    df["age_of_consent"] = df["value_formatted_age_of_consent"].map(id_age_of_consent)
    df["blood"] = df["value_formatted_blood"].map(id_blood)
    df["censorship"] = df["value_formatted_censorship"].map(id_censorship)
    df["conversion_therapy"] = df["value_formatted_conversion_therapy"].map(id_conversion_therapy)
    df["discrimination"] = df["value_formatted_discrimination"].map(id_discrimination)
    df["employment_discrimination"] = df["value_formatted_employment_discrimination"].map(id_employment_discrimination)
    df["housing_discrimination"] = df["value_formatted_housing_discrimination"].map(id_housing_discrimination)
    df["military"] = df["value_formatted_military"].map(id_military)
    df["non_binary_gender_recognition"] = df["value_formatted_non_binary_gender_recognition"].map(
        id_non_binary_gender_recognition
    )

    # Keep only the columns we need
    df = df[
        [
            "country",
            "year",
            "homosexuality",
            "changing_gender",
            "marriage",
            "adoption",
            "age_of_consent",
            "blood",
            "censorship",
            "conversion_therapy",
            "discrimination",
            "employment_discrimination",
            "housing_discrimination",
            "military",
            "non_binary_gender_recognition",
        ]
    ]

    return df


def run(dest_dir: str) -> None:
    log.info("equaldex.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("equaldex")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["equaldex"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow).reset_index(drop=False)

    # Read table from meadow dataset (current dataset).
    tb_meadow = ds_meadow["equaldex_current"]

    # Create a dataframe with data from the table.
    df_current = pd.DataFrame(tb_meadow).reset_index(drop=False)

    #
    # Process data.

    df = process_data(df)
    df_current = process_data(df_current)

    # Merge both datasets and include the suffix _current to the columns of the current dataset
    df = pd.merge(df, df_current, on=["country", "year"], how="outer", suffixes=("", "_current"))

    log.info("equaldex.harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)

    # Verify index and order them
    df = df.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Create a new table with the processed data.
    tb_garden = Table(
        df,
        short_name=paths.short_name,
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden])

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("equaldex.end")

"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def load_countries_regions() -> Table:
    """Load countries-regions table from reference dataset (e.g. to map from iso codes to country names)."""
    ds_reference: Dataset = paths.load_dependency("reference")
    tb_countries_regions = ds_reference["countries_regions"]

    return tb_countries_regions


def load_population() -> Table:
    """Load population table from population OMM dataset."""
    ds_indicators: Dataset = paths.load_dependency(channel="garden", namespace="demography", short_name="population")
    tb_population = ds_indicators["population"]

    return tb_population


def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process data from Equaldex."""

    # Make the dataframe wide by pivoting on the issue column.
    df = df.pivot(
        index=["country", "year"], columns="issue", values=["id", "value", "value_formatted", "description"]
    ).reset_index(drop=False)

    # Flatten the multi-index column names, but keep country and year as separate columns.
    df.columns = ["_".join(col).strip() for col in df.columns.values]
    df = df.rename(columns={"country_": "country", "year_": "year"})

    # replace all column names with - with _
    df.columns = df.columns.str.replace("-", "_")

    # Create a new list of id for each issue, ordered from more liberal to more restrictive (useful for grapher charts)
    id_homosexuality = {
        "Legal": "01. Legal",
        "Male illegal, female legal": "02. Male illegal, female legal",
        "Male illegal, female uncertain": "03. Male illegal, female uncertain",
        "Illegal (other penalty)": "04. Illegal (other penalty)",
        "Ambiguous": "05. Ambiguous",
        "Varies by Region": "06. Varies by Region",
        "Illegal (imprisonment as punishment)": "07. Illegal (imprisonment as punishment)",
        "Illegal (up to life in prison as punishment)": "08. Illegal (up to life in prison as punishment)",
        "Illegal (death penalty as punishment)": "09. Illegal (death penalty as punishment)",
    }
    id_changing_gender = {
        "Legal, surgery not required": "01. Legal, surgery not required",
        "Legal, but requires surgery": "02. Legal, but requires surgery",
        "Ambiguous": "03. Ambiguous",
        "Varies by Region": "04. Varies by Region",
        "Illegal": "05. Illegal",
    }
    id_marriage = {
        "Legal": "01. Legal",
        "Civil unions": "02. Civil unions",
        "Other type of partnership": "03. Other type of partnership",
        "Foreign same-sex marriages recognized only": "04. Foreign same-sex marriages recognized only",
        "Unregistered cohabitation": "05. Unregistered cohabitation",
        "Ambiguous": "06. Ambiguous",
        "Varies by Region": "07. Varies by Region",
        "Unrecognized": "08. Unrecognized",
        "Unrecognized, same-sex marriage and civil unions banned": "09. Unrecognized, same-sex marriage and civil unions banned",
        "Not legal": "10. Not legal",
    }

    # Match id variables with the new ids in new columns
    df["homosexuality"] = df["value_formatted_homosexuality"].map(id_homosexuality)
    df["changing_gender"] = df["value_formatted_changing_gender"].map(id_changing_gender)
    df["marriage"] = df["value_formatted_marriage"].map(id_marriage)

    # Keep only the columns we need
    df = df[
        [
            "country",
            "year",
            "homosexuality",
            "changing_gender",
            "marriage",
            "description_homosexuality",
            "description_changing_gender",
            "description_marriage",
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

    #
    # Process data.

    df = process_data(df)

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

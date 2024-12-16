"""Load a snapshot and create a meadow dataset."""

import re

import owid.catalog.processing as pr
import pandas as pd
import PyPDF2

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def extract_text_from_pdf(pdf_path: str) -> str:
    text = ""
    with open(pdf_path, "rb") as file:
        pdf_reader = PyPDF2.PdfReader(file)
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            text += page.extract_text()
    return text


def run(dest_dir: str) -> None:
    #
    # Load and process inputs.
    #
    # Load snapshots for global and country-level data.
    snap_global = paths.load_snapshot("number_of_wild_fish_killed_for_food_global.pdf")
    snap_by_country = paths.load_snapshot("number_of_wild_fish_killed_for_food_by_country.pdf")

    #
    # Process data.
    #
    # Extract text from PDF on global numbers.
    text_global = extract_text_from_pdf(pdf_path=snap_global.path)
    df_global = pd.DataFrame(
        [row.split() for row in text_global.split("\n")[3:24]],
        columns=[
            "year",
            "Capture production (landings) (million tonnes)",
            "Estimated numbers in millions (lower)",
            "Estimated numbers in millions (upper)",
            "Estimated numbers in millions (midpoint)",
            "% of estimate based on specific species/genus weight data (by tonnage)",
            "Mean individual fish weight for year (g) (lower)",
            "Mean individual fish weight for year (g) (upper)",
        ],
    )
    tb_global = pr.read_from_df(df_global, metadata=snap_global.to_table_metadata(), origin=snap_global.metadata.origin)

    # Extract text from PDF on country-level numbers.
    text_by_country = extract_text_from_pdf(snap_by_country.path)

    # Extract relevant rows from each page.
    # Fix poor extractions at ends of pages.
    text_by_country_rows = (
        [row for row in text_by_country.split("\n")[3:77]]
        + [text_by_country.split("\n")[77].split("Top")[0]]
        + [text_by_country.split("\n")[92].split("(by tonnage)")[1]]
        + [row for row in text_by_country.split("\n")[93:174]]
        + [text_by_country.split("\n")[174].split("Belgium")[0]]
        + [text_by_country.split("\n")[174].split("Cyprinids nei")[1]]
        + [row for row in text_by_country.split("\n")[175:254]]
    )

    # Remove spurious spaces.
    text_by_country_rows = [re.sub(r"\s+", " ", row) for row in text_by_country_rows]

    # Split the data into three groupings: First, country name, second, the data, third, the top species.
    data_columns = []
    for row in text_by_country_rows:
        indices = [i for i, char in enumerate(row) if char.isdigit()]
        data_columns.append(
            [row[0 : indices[0]].strip()]
            + row[indices[0] : indices[-1] + 1].split(" ")
            + [row[indices[-1] + 1 :].strip()]
        )
    df_by_country = pd.DataFrame(
        data_columns,
        columns=[
            "Country",
            "Average annual capture production (landings) 2000-2019 (tonnes)",
            "Estimated numbers (lower)",
            "Estimated numbers (upper)",
            "Estimated numbers (midpoint)",
            "% of estimate based on specific species/genus weight data (by tonnage)",
            "Mean individual fish weight for country (g) (lower)",
            "Mean individual fish weight for country (g) (upper)",
            "Top species for country by estimated numbers (midpoint)",
        ],
    )
    tb_by_country = pr.read_from_df(
        df_by_country, metadata=snap_by_country.to_table_metadata(), origin=snap_by_country.metadata.origin
    )

    # Improve table formats.
    tb_global = tb_global.format(keys=["year"])
    tb_by_country = tb_by_country.format(keys=["country"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb_global, tb_by_country], check_variables_metadata=True)
    ds_meadow.save()

"""Load a meadow dataset and create a garden dataset."""

import os
import zipfile
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset
from etl.snapshot import Snapshot

PRE_PRIMARY_EDUCATION_VARIABLES = [
    "Gross enrolment ratio, pre-primary, both sexes (%)",
    "Official entrance age to pre-primary education (years)",
    "School life expectancy, pre-primary, gender parity index (GPI)",
    "School life expectancy, pre-primary, both sexes (years)",
    "School life expectancy, pre-primary, female (years)",
    "School life expectancy, pre-primary, male (years)",
    "Pupil/qualified teacher ratio in pre-primary education (headcount basis)",
    "Pupil/trained teacher ratio in pre-primary education (headcount basis)",
    "Percentage of teachers in pre-primary education who are qualified, both sexes (%)",
    "Percentage of teachers in pre-primary education who are trained, both sexes (%)",
    "Annual statutory teacher salaries in public institutions in USD. Pre-Primary. Starting salary",
    "Government expenditure on pre-primary education as % of GDP (%)",
    "Percentage of enrolment in early childhood education programmes in private institutions (%)",
    "Percentage of enrolment in early childhood educational development programmes in private institutions (%)",
    "Total net enrolment rate, primary, male (%)",
    "Total net enrolment rate, primary, female (%)",
    "Total net enrolment rate, primary, both sexes (%)",
    "Total net enrolment rate, primary, gender parity index (GPI)",
    "Percentage of enrolment in primary education in private institutions (%)",
    "Gross enrolment ratio, primary, both sexes (%)",
    "DHS: Proportion of out-of-school. Primary",
    "DHS: Proportion of out-of-school. Primary. Female",
    "DHS: Proportion of out-of-school. Primary. Male",
    MICS: Primary completion rate
MICS: Primary completion rate. Female
MICS: Primary completion rate. Male
    "Rate of out-of-school children of primary school age, both sexes (%)",
    "Rate of out-of-school children of primary school age, female (%)",
    "Rate of out-of-school children of primary school age, male (%)",
    "UIS: Rate of out-of-school children of primary school age, both sexes (household survey data) (%)",
    "UIS: Rate of out-of-school children of primary school age, female (household survey data) (%)",
    "UIS: Rate of out-of-school children of primary school age, male (household survey data) (%)",
    "Primary completion rate, female (%)",
    "Primary completion rate, male (%)",
    "Primary completion rate, both sexes (%)",
    "Primary completion rate, gender parity index (GPI)",
    "Percentage of enrolment in primary education in private institutions (%)",
    "PASEC: Average performance gap between 2nd grade students in private and public education. Mathematics",
    "PASEC: Average performance gap between 2nd grade students in private and public education. Language",
    "School life expectancy, primary, gender parity index (GPI)",
    "School life expectancy, primary, both sexes (years)",
    "School life expectancy, primary, female (years)",
    "School life expectancy, primary, male (years)",
    "Pupil/qualified teacher ratio in primary education (headcount basis)",
    "Pupil/trained teacher ratio in primary education (headcount basis)",
    "Percentage of teachers in primary education who are qualified, both sexes (%)",
    "Percentage of teachers in primary education who are trained, both sexes (%)",
    "Annual statutory teacher salaries in public institutions in USD. Primary. Starting salary",
    "Government expenditure on primary education as % of GDP (%)",

    "DHS: Secondary completion rate"
    "DHS: Secondary completion rate. Female"
    "DHS: Secondary completion rate. Male"
    MICS: Secondary completion rate
    MICS: Secondary completion rate. Female
    MICS: Secondary completion rate. Male
    Lower secondary completion rate, female (%)
    Lower secondary completion rate, male (%)
    Lower secondary completion rate, both sexes (%)
    Lower secondary completion rate, gender parity index (GPI)
    ]


def add_metadata(tb):
    snap: Snapshot = paths.load_dependency("education.zip")

    # Step 1: Unzip the file
    with zipfile.ZipFile(snap.path, "r") as zip_ref:
        # Replace 'data.csv' with the name of your CSV file in the zip archive
        csv_file_name = "EdStatsSeries.csv"
        destination_directory = os.path.dirname(snap.path)
        zip_ref.extract(csv_file_name, destination_directory)

    # Now, use pandas to read the CSV file into a DataFrame
    df_metadata = pd.read_csv(os.path.join(destination_directory, csv_file_name))
    df_metadata = df_metadata[["Indicator Name", "Source", "Long definition"]]

    for column in tb.columns:
        title_to_find = tb[column].metadata.title
        tb[column].metadata.description = df_metadata["Long definition"][df_metadata["Indicator Name"] == title_to_find]
        tb[column].metadata.display = {}
        tb[column].metadata.display["numDecimalPlaces"] = 0
        if "%" or "percent" in title_to_find:
            tb[column].metadata.short_unit = "%"

    return tb


# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = [
    "North America",
    "South America",
    "Europe",
    "European Union (27)",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
]


def add_data_for_regions(tb: Table, regions: List[str], ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    tb_with_regions = tb.copy()

    aggregations = {column: "median" for column in tb_with_regions.columns if column not in ["country", "year"]}

    for region in REGIONS:
        # Find members of current region.
        members = geo.list_members_of_region(
            region=region,
            ds_regions=ds_regions,
            ds_income_groups=ds_income_groups,
        )
        tb_with_regions = geo.add_region_aggregates(
            df=tb_with_regions,
            region=region,
            countries_in_region=members,
            countries_that_must_have_data=[],
            num_allowed_nans_per_year=None,
            frac_allowed_nans_per_year=0.99999,
            aggregations=aggregations,
        )
    tb_with_regions = tb_with_regions.copy_metadata(from_table=tb)

    return tb_with_regions


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = cast(Dataset, paths.load_dependency("education"))

    # Load regions dataset.
    ds_regions: Dataset = paths.load_dependency("regions")

    # Load income groups dataset.
    ds_income_groups: Dataset = paths.load_dependency("income_groups")

    # Read table from meadow dataset.
    tb = ds_meadow["education"]
    tb.reset_index(inplace=True)

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    for i in PRE_PRIMARY_EDUCATION_VARIABLES:
        if i not in tb["indicator_name"].unique():
            print(i)

    tb = tb[tb["indicator_name"].isin(PRE_PRIMARY_EDUCATION_VARIABLES)]
    tb.reset_index(inplace=True)

    # Pivot the dataframe so that each indicator is a separate column
    tb = tb.pivot(index=["country", "year"], columns="indicator_name", values="value")
    tb.reset_index(inplace=True)
    # Add region aggregates.
    tb = add_data_for_regions(tb=tb, regions=REGIONS, ds_regions=ds_regions, ds_income_groups=ds_income_groups)

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    tb = Table(tb, short_name=paths.short_name, underscore=True)
    tb = add_metadata(tb)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)
    # Save changes in the new garden dataset.
    ds_garden.save()

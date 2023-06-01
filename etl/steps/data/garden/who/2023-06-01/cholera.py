"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("cholera.start")

    #
    # Load inputs.
    #
    # Load backport dataset.
    short_name = "dataset_5676_global_health_observatory__world_health_organization__2022_08"
    who_gh_dataset: Dataset = paths.load_dependency(short_name)
    who_gh = who_gh_dataset[short_name].reset_index()

    # Process backport dataset
    cholera_cols = who_gh.columns[who_gh.columns.str.contains("cholera")].to_list()
    cholera_bp = who_gh[["year", "entity_name"] + cholera_cols]
    cholera_bp[cholera_cols] = cholera_bp[cholera_cols].apply(pd.to_numeric, errors="coerce")
    cholera_bp = cholera_bp.dropna(how="all", axis=0, subset=cholera_cols).rename(
        columns={
            "entity_name": "country",
            "indicator__cholera_case_fatality_rate": "cholera_case_fatality_rate",
            "indicator__number_of_reported_cases_of_cholera": "cholera_reported_cases",
            "indicator__number_of_reported_deaths_from_cholera": "cholera_deaths",
        }
    )
    cholera_bp = geo.harmonize_countries(
        df=cholera_bp, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Add global aggregate
    cholera_bp = add_global_total(cholera_bp)

    # Load fast track dataset
    snap: Dataset = paths.load_dependency("cholera.csv")
    cholera_ft = pd.read_csv(snap.path)

    # Combine backport and fast track datasets
    cholera_combined = pd.concat([cholera_bp, cholera_ft]).set_index(["country", "year"])
    tb_garden = Table(cholera_combined, short_name="cholera")

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden])

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("cholera.end")


def add_global_total(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate global total of cholera cases and add it to the existing dataset
    """
    countries_regions = geo._load_countries_regions()
    countries = countries_regions[countries_regions["region_type"] == "country"]["name"].to_list()
    manual_countries_to_allow = ["The former state union Serbia and Montenegro"]
    countries = countries + manual_countries_to_allow
    assert all(
        df["country"].isin(countries)
    ), f"{df['country'][~df['country'].isin(countries)].drop_duplicates()}, is not a country"
    df_glob = df.groupby(["year"]).agg({"cholera_reported_cases": "sum", "cholera_deaths": "sum"}).reset_index()
    df_glob["country"] = "World"
    df_glob["cholera_case_fatality_rate"] = (df_glob["cholera_deaths"] / df_glob["cholera_reported_cases"]) * 100
    df = pd.concat([df, df_glob])
    return df

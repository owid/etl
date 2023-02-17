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
    log.info("fluid.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("fluid")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["fluid"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    log.info("fluid.harmonize_countries")
    df = geo.harmonize_countries(
        df=df, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Subset the data
    df = subset_and_clean_data(df)
    df = pivot_fluid(df)
    df = df.reset_index(drop=True)
    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=paths.short_name)
    tb_garden.update_metadata_from_yaml(paths.metadata_path, paths.short_name)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("fluid.end")


def subset_and_clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    * Select out the 'All' age groups
    * Select the case info types we are interested in: SARI, ILI, ARI, SARI_DEATHS
    * Format the date
    * Drop unwanted columns
    * Drop rows where reported cases = 0
    """

    df = df.query("agegroup_code == 'All' & case_info.isin(['ILI', 'SARI', 'ARI','SARI_ICU' ,'SARI_DEATHS'])")
    df["date"] = pd.to_datetime(df["iso_weekstartdate"], format="%Y-%m-%d", utc=True).dt.date.astype(str)

    # At time of creation the source type was not used in FluID, but it is used in FluNET to identify Sentinel sites
    assert df.origin_source.unique() == ["NOTDEFINED"]

    df = df.drop(
        columns=[
            "whoregion",
            "fluseason",
            "hemisphere",
            "itz",
            "country_code",
            "iso_weekstartdate",
            "iso_year",
            "iso_week",
            "mmwr_weekstartdate",
            "mmwr_year",
            "mmwr_week",
            "agegroup_code",
            "outpatients",
            "inpatients",
            "pop_cov",
            "inf_tested",
            "inf_pos",
            "inf_neg",
            "inf_a",
            "inf_b",
            "inf_mixed",
            "ah1",
            "ah1n12009",
            "ah3",
            "ah5",
            "ah7",
            "anotsubtyped",
            "aothertypes",
            "byamagata",
            "bvictoria",
            "bnotdetermined",
            "other",
            "rsv",
            "adeno",
            "parainfluenza",
            "inf_risk",
            "geospread",
            "impact",
            "intensity",
            "trend",
            "nb_sites",
            "mortality_all",
            "mortality_pni",
            "iso2",
            "isoyw",
            "mmwryw",
            "iso2",
            "isoyw",
            "mmwryw",
            "origin_source",
        ]
    )

    df = df.dropna(subset=["reported_cases"])
    return df


def pivot_fluid(df: pd.DataFrame) -> pd.DataFrame:

    df_piv = df.pivot(
        index=[
            "country",
            "date",
        ],
        columns="case_info",
        values=[
            "reported_cases",
        ],
    ).reset_index()

    df_piv.columns = list(map("".join, df_piv.columns))

    df_piv = df_piv.dropna(axis=1, how="all")

    df_piv = df_piv.rename(
        columns={
            "reported_casesSARI": "reported_sari_cases",
            "reported_casesARI": "reported_ari_cases",
            "reported_casesILI": "reported_ili_cases",
            "reported_casesSARI_DEATHS": "reported_sari_deaths",
            "reported_casesSARI_ICU": "reported_sari_icu",
        }
    )

    df_piv = df_piv.dropna(axis=0, how="all")

    return df_piv

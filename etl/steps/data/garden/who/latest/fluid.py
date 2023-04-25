"""Load a meadow dataset and create a garden dataset."""


from datetime import datetime

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
MIN_DATA_POINTS_PER_YEAR = 10


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
    # Remove years with fewer than 10 datapoints
    df = remove_sparse_years(df, min_datapoints_per_year=MIN_DATA_POINTS_PER_YEAR)

    df = calculate_patient_rates(df)

    df = df.reset_index(drop=True)
    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=paths.short_name)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("fluid.end")


def clean_sari_inpatient(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removing rows where the number of SARI cases is below the number of inpatients, I don't think this should be possible.
    """
    remove_inpatients = df["inpatients"][(df["case_info"] == "SARI") & (df["reported_cases"] > df["inpatients"])].shape[
        0
    ]
    log.info(f"Removing {remove_inpatients} rows where the number of inpatients is below the number of SARI cases...")
    df["inpatients"][(df["case_info"] == "SARI") & (df["reported_cases"] > df["inpatients"])] = np.NaN

    return df


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
            "itz",
            "country_code",
            "iso_week",
            "iso_weekstartdate",
            "iso_year",
            "mmwr_weekstartdate",
            "mmwr_year",
            "mmwr_week",
            "agegroup_code",
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
    df = clean_sari_inpatient(df)
    return df


def pivot_fluid(df: pd.DataFrame) -> pd.DataFrame:

    df_piv = df.pivot(
        index=["country", "hemisphere", "date"],
        columns=["case_info"],
        values=["reported_cases", "outpatients", "inpatients"],
    ).reset_index()

    df_piv.columns = list(map("".join, df_piv.columns))

    df_piv = df_piv.rename(
        columns={
            "reported_casesSARI": "reported_sari_cases",
            "reported_casesARI": "reported_ari_cases",
            "reported_casesILI": "reported_ili_cases",
            "reported_casesSARI_DEATHS": "reported_sari_deaths",
            "reported_casesSARI_ICU": "reported_sari_icu",
            "outpatientsARI": "outpatients_ari",
            "outpatientsILI": "outpatients_ili",
            "inpatientsSARI": "inpatients_sari",
            "inpatientsSARI_ICU": "inpatients_sari_icu",
        }
    )

    return df_piv


def calculate_patient_rates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculating the rates of reported cases per outpatients/inpatients as also used by WHO
    """

    df[["reported_ili_cases", "reported_ari_cases", "reported_sari_cases"]] = df[
        ["reported_ili_cases", "reported_ari_cases", "reported_sari_cases"]
    ].astype(float)

    # Replace 0s and NAs with NaNs
    df.loc[:, ["outpatients_ari", "outpatients_ili", "inpatients_sari", "inpatients_sari_icu"]] = df.loc[
        :, ["outpatients_ari", "outpatients_ili", "inpatients_sari", "inpatients_sari_icu"]
    ].replace({0: np.nan, pd.NA: np.nan})

    df["ili_cases_per_thousand_outpatients"] = (df["reported_ili_cases"] / df["outpatients_ili"]) * 1000
    df["ari_cases_per_thousand_outpatients"] = (df["reported_ari_cases"] / df["outpatients_ari"]) * 1000
    df["sari_cases_per_hundred_inpatients"] = (df["reported_sari_cases"] / df["inpatients_sari"]) * 100

    df = clean_patient_rates(df)

    return df


def clean_patient_rates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleaning the patient rates by:
    * Removing values over the top limit of the rate e.g. values over 100 for SARI cases per 100 inpatients
    * Removing time-series where there are only values of either 0 or the top limit of the variable.
    """
    df[
        [
            "ili_cases_per_thousand_outpatients",
            "ari_cases_per_thousand_outpatients",
            "sari_cases_per_hundred_inpatients",
        ]
    ] = (
        df[
            [
                "ili_cases_per_thousand_outpatients",
                "ari_cases_per_thousand_outpatients",
                "sari_cases_per_hundred_inpatients",
            ]
        ]
        .astype(float)
        .replace({pd.NA: np.nan})
    )

    over_1000_ili = df[df["ili_cases_per_thousand_outpatients"] > 1000].shape[0]
    over_1000_ari = df[df["ari_cases_per_thousand_outpatients"] > 1000].shape[0]
    over_100_sari = df[df["sari_cases_per_hundred_inpatients"] > 100].shape[0]

    log.info(
        f"{over_1000_ili} rows with ili_cases_per_thousand_outpatients greater than or equal to 1000. We'll set these to NA."
    )
    log.info(
        f"{over_1000_ari} rows with ari_cases_per_thousand_outpatients greater than or equal to 1000. We'll set these to NA."
    )
    log.info(
        f"{over_100_sari} rows with sari_cases_per_hundred_inpatients greater than or equal to 100. We'll set these to NA."
    )

    df.loc[df["ili_cases_per_thousand_outpatients"] >= 1000, "ili_cases_per_thousand_outpatients"] = np.NaN
    df.loc[df["ari_cases_per_thousand_outpatients"] >= 1000, "ari_cases_per_thousand_outpatients"] = np.NaN
    df.loc[df["sari_cases_per_hundred_inpatients"] >= 100, "sari_cases_per_hundred_inpatients"] = np.NaN

    df["ili_cases_per_thousand_outpatients"] = df.groupby("country", group_keys=False)[
        "ili_cases_per_thousand_outpatients"
    ].apply(remove_values_with_only_extremes, min=1, max=999)

    df["ari_cases_per_thousand_outpatients"] = df.groupby("country", group_keys=False)[
        "ari_cases_per_thousand_outpatients"
    ].apply(remove_values_with_only_extremes, min=1, max=999)

    df["sari_cases_per_hundred_inpatients"] = df.groupby("country", group_keys=False)[
        "sari_cases_per_hundred_inpatients"
    ].apply(remove_values_with_only_extremes, min=1, max=99)

    return df


def remove_values_with_only_extremes(group: pd.Series, min: int, max: int) -> pd.Series:
    """
    If all values in the group are less than {min} or greater than {max}, or NA then replace all values for that group with NA.
    """
    if all((x <= min) | (x >= max) | (np.isnan(x)) for x in group):
        return pd.Series([np.NaN if x <= min or x >= max else x for x in group], index=group.index, dtype="float64")
    else:
        return group


def remove_sparse_years(df: pd.DataFrame, min_datapoints_per_year: int) -> pd.DataFrame:
    """
    If a year has fewer than {min_data_points_per_year} then we should remove all the data for that year -> set it to NA
    For the current year then if all the values are 0 or NA then we remove all values for the year so far
    """

    df["year"] = pd.to_datetime(df["date"]).dt.year
    constant_cols = ["country", "date", "hemisphere", "year"]
    cols = df.columns.drop(constant_cols)
    current_year = datetime.today().year
    for col in cols:
        df_col = df.loc[:, ["country", "year", col]]
        df_col[col] = pd.to_numeric(df_col[col])
        df_col_bool = (
            df_col.groupby(["country", "year"]).agg(weeks_gt_zero=(col, lambda x: x.gt(0).sum())).reset_index()
        )
        df = pd.merge(df, df_col_bool, on=["country", "year"])

        df_current = df[df["year"] == current_year]
        # Dropping rows if all the weeks data from this year are 0 or NA
        df_current.loc[
            (df_current["weeks_gt_zero"] == 0),
            col,
        ] = np.nan

        df_hist = df[df["year"] < current_year]

        df_hist.loc[
            (df_hist["weeks_gt_zero"] < min_datapoints_per_year),
            col,
        ] = np.nan

        df = pd.concat([df_current, df_hist])
        df[col] = pd.to_numeric(df[col])
        df = df.drop(columns=["weeks_gt_zero"])

    return df

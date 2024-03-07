from pathlib import Path

import pandas as pd
from owid.catalog import Dataset
from structlog import get_logger

from etl.paths import DATA_DIR

# Initialize logger.
log = get_logger()

PARENT_DIR = Path(__file__).parent.absolute()

ds = Dataset(DATA_DIR / "explorers" / "poverty_inequality" / "2023-08-24" / "poverty_inequality_export")

df = ds["keyvars"].reset_index()
df = pd.DataFrame(df)

df_percentiles = ds["percentiles"].reset_index()
df_percentiles = pd.DataFrame(df_percentiles)

df_wdi = ds["wdi"].reset_index()


#############################################
# FUNCTION SETTINGS

# `series` is a list of series codes, e.g. ["average_lis_disposable_equivalized_2017ppp2017", "threshold_lis_market_perCapita_2017ppp2017", "threshold_widExtrapolated_pretaxNational_perAdult_2011ppp2022", "threshold_pip_disposable_perCapita_2017ppp2017"]
series = [
    "average_lis_disposable_equivalized_2017ppp2017",
    "threshold_lis_market_perCapita_2017ppp2017",
    "threshold_lis_disposable_perAdult_2017ppp2017",
    "threshold_wid_pretaxNational_perAdult_2011ppp2022",
]

# reference_years is the list of years to match the series to. Each year is a dictionary with the year as key and a dictionary of parameters as value. The parameters are:
# - maximum_distance: the maximum distance between the series and the reference year
# - tie_break_strategy: the strategy to use to break ties when there are multiple series that match the reference year. The options are "lower" (select the series with the lowest distance to the reference year) or "higher" (select the series with the highest distance to the reference year)
# - min_interval: the minimum distance between reference years. The value of min_interval for the last reference year is ignored.
reference_years = {
    # 1980: {"maximum_distance": 0, "tie_break_strategy": "lower", "min_interval": 5},
    1990: {"maximum_distance": 3, "tie_break_strategy": "higher", "min_interval": 7},
    2000: {"maximum_distance": 0, "tie_break_strategy": "higher", "min_interval": 8},
    2010: {"maximum_distance": 3, "tie_break_strategy": "lower", "min_interval": 5},
    2018: {"maximum_distance": 5, "tie_break_strategy": "higher", "min_interval": 2},
}

# Activate lining up function
lining_up = True

# scale_to_na defines if the numbers are scaled up to national accounts (somewhat similar to what WID does)
scale_to_na = True

# gdp_type defines if we use GDP in international dollars ("ppp") or in constant USD "constant"
gdp_type = "ppp"

# In case of PIP series, we need to define three parameters:
# - constant_reporting_level: if True (no quotes), the series will be matched only to series with the same pipreportinglevel (only national, only rural or only rural per country). Different countries can have different constant pipreportinglevels. If False, series mixing national, urban or rural are created.
# - constant_welfare_type: if True (no quotes), the series will be matched only to series with the pipwelfare defined in the income_or_consumpion parameter. If False, income or consumption can be selected.
# - income_or_consumption: if constant_welfare_type is False, this parameter defines whether to match the series to income or consumption data. The options are "income" or "consumption".
constant_reporting_level = True
constant_welfare_type = False
income_or_consumption = "income"


#############################################


def lining_up(
    df: pd.DataFrame, series: list, reference_years: dict, lining_up: bool, scale_to_na: bool, gdp_type: str
) -> pd.DataFrame:
    """
    Main function to lining up the data
    """

    # Assert if the series belong to the df
    assert set(series).issubset(set(df["series_code"])), log.error(
        f"The series {set(series) - set(df['series_code'])} is not in the dataset."
    )
    df_match = pd.DataFrame()
    df_series = df[df["series_code"].isin(series)].reset_index(drop=True)

    reference_years_list = []
    for y in reference_years:
        # keep reference year in a list
        reference_years_list.append(y)
        # Filter df_series according to reference year and maximum distance from it
        df_year = df_series[
            (df_series["year"] <= y + reference_years[y]["maximum_distance"])
            & (df_series["year"] >= y - reference_years[y]["maximum_distance"])
        ].reset_index(drop=True)

        assert not df_year.empty, log.error(
            f"No data found for reference year {y}. Please check `maximum_distance` ({reference_years[y]['maximum_distance']})."
        )

        df_year["distance"] = abs(df_year["year"] - y)

        # If source is PIP, filter df_year according to constant_reporting_level and constant_welfare_type
        # Filter df_year according to constant_reporting_level, constant_welfare_type and income_or_consumption

        # Check if constant_welfare_type is boolean
        assert isinstance(constant_welfare_type, bool), log.error(
            "`constant_welfare_type` must be boolean: True or False (without quotes)."
        )
        # Check if income_or_consumption is income or consumption
        assert income_or_consumption in ["income", "consumption"], log.error(
            "`income_or_consumption` must be either 'income' or 'consumption'."
        )
        if constant_welfare_type:
            df_year = df_year[
                (df_year["pipwelfare"] == income_or_consumption) | (df_year["pipwelfare"].isnull())
            ].reset_index(drop=True)

        # Merge the different reference years into a single dataframe
        if df_match.empty:
            df_match = df_year
        else:
            df_match = pd.merge(df_match, df_year, how="outer", on=["country", "series_code"], suffixes=("", f"_{y}"))
            if len(reference_years_list) == 2:
                df_match[f"distance_{reference_years_list[-2]}_{y}"] = abs(df_match["year"] - df_match[f"year_{y}"])
            else:
                df_match[f"distance_{reference_years_list[-2]}_{y}"] = abs(
                    df_match[f"year_{reference_years_list[-2]}"] - df_match[f"year_{y}"]
                )
            # Filter df_match according to min_interval
            df_match = df_match[
                df_match[f"distance_{reference_years_list[-2]}_{y}"]
                >= reference_years[reference_years_list[-2]]["min_interval"]
            ].reset_index(drop=True)

            assert not df_match.empty, log.error(
                f"No matching data found for reference years {reference_years_list[-2]} and {y}. Please check `min_interval` ({reference_years[reference_years_list[-2]]['min_interval']})."
            )

    # Rename columns related to the first reference year
    df_match = df_match.rename(
        columns={
            "year": f"year_{reference_years_list[0]}",
            "distance": f"distance_{reference_years_list[0]}",
            "value": f"value_{reference_years_list[0]}",
            "source": f"source_{reference_years_list[0]}",
            "pipreportinglevel": f"pipreportinglevel_{reference_years_list[0]}",
            "pipwelfare": f"pipwelfare_{reference_years_list[0]}",
        }
    )
    # Create a score for PIP data
    # Create a list of pipreportinglevel_x, with x being each of the reference years
    pipwelfare_list = []
    pipreportinglevel_list = []
    distance_list = []
    for y in reference_years_list:
        pipwelfare_list.append(f"pipwelfare_{y}")
        pipreportinglevel_list.append(f"pipreportinglevel_{y}")
        distance_list.append(f"distance_{y}")

    # If all the columns in pipwelfare_list are "income", assign a score of 10 to the column df_match["pip_welfare_score"]
    df_match.loc[df_match[pipwelfare_list].eq("income").all(axis=1), "pip_welfare_score"] = 10
    # If all the columns in pipwelfare_list are "consumption", assign a score of 5 to the column df_match["pip_welfare_score"]
    df_match.loc[df_match[pipwelfare_list].eq("consumption").all(axis=1), "pip_welfare_score"] = 5
    # Assign a score of 0 to the column df_match["pip_welfare_score"] for the rest of the rows
    df_match["pip_welfare_score"] = df_match["pip_welfare_score"].fillna(0)

    # If all the columns in pipreportinglevel_list are "national", assign a score of 10 to the column df_match["pip_reporting_level_score"]
    df_match.loc[df_match[pipreportinglevel_list].eq("national").all(axis=1), "pip_reporting_level_score"] = 10
    # If all the columns in pipreportinglevel_list are "urban", assign a score of 5 to the column df_match["pip_reporting_level_score"]
    df_match.loc[df_match[pipreportinglevel_list].eq("urban").all(axis=1), "pip_reporting_level_score"] = 5
    # If all the columns in pipreportinglevel_list are "rural", assign a score of 1 to the column df_match["pip_reporting_level_score"]
    df_match.loc[df_match[pipreportinglevel_list].eq("rural").all(axis=1), "pip_reporting_level_score"] = 1
    # Assign a score of 0 to the column df_match["pip_reporting_level_score"] for the rest of the rows
    df_match["pip_reporting_level_score"] = df_match["pip_reporting_level_score"].fillna(0)

    # Assign pip_reporting_level_equal to True if all the columns in pipreportinglevel_list are equal or all null
    df_match["pip_reporting_level_equal"] = (
        df_match[pipreportinglevel_list].eq(df_match[pipreportinglevel_list].iloc[:, 0], axis=0).all(axis=1)
    ) | (df_match[pipreportinglevel_list].isnull().all(axis=1))

    # Create a score for the cummulative distnace between the reference years
    # NOTE: I am not using it for now
    df_match["total_distance"] = df_match[distance_list].sum(axis=1)

    # Check if constant_reporting_level is boolean
    assert isinstance(constant_reporting_level, bool), log.error(
        "`constant_reporting_level` must be boolean: True or False (without quotes)."
    )

    # If constant_reporting_level = True, filter df_match according to pip_reporting_level_equal
    if constant_reporting_level:
        df_match = df_match[df_match["pip_reporting_level_equal"]].reset_index(drop=True)

        assert not df_match.empty, log.error(
            f"No matching data found for reference years {reference_years_list}. Please check `constant_reporting_level` ({constant_reporting_level})."
        )

    # Export raw combinations of series and reference years
    df_match.to_csv(f"{PARENT_DIR}/df_match_raw.csv", index=False)

    # Filter df_match according to tie_break_strategy
    for y in reference_years_list:
        if reference_years[y]["tie_break_strategy"] == "lower":
            # Remove duplicates and keep the row with the minimum distance
            df_match = df_match.sort_values(
                by=["pip_welfare_score", "pip_reporting_level_score", f"distance_{y}"], ascending=[False, False, True]
            ).drop_duplicates(subset=["country", "series_code"], keep="first")
        elif reference_years[y]["tie_break_strategy"] == "higher":
            # Remove duplicates and keep the row with the maximum distance
            df_match = df_match.sort_values(
                by=["pip_welfare_score", "pip_reporting_level_score", f"distance_{y}"], ascending=[False, False, False]
            ).drop_duplicates(subset=["country", "series_code"], keep="first")
        else:
            raise ValueError("tie_break_strategy must be either 'lower' or 'higher'")

        assert not df_match.empty, log.error(
            f"No matching data data found for reference year {y}. Please check `tie_break_strategy` ({reference_years[y]['tie_break_strategy']})."
        )

    # Create a list with the variables year_y and value_y for each reference year
    year_y_list = []
    value_y_list = []
    year_value_y_list = []
    for y in reference_years_list:
        year_y_list.append(f"year_{y}")
        value_y_list.append(f"value_{y}")
        year_value_y_list.append(f"year_{y}")
        year_value_y_list.append(f"value_{y}")

    # Make columns in year_y_list integer
    df_match[year_y_list] = df_match[year_y_list].astype(int)

    # Keep the columns I need
    df_match = df_match[["country", "series_code", "indicator_name"] + year_value_y_list].reset_index(drop=True)

    # Sort by country and year_y
    df_match = df_match.sort_values(by=["series_code", "country"] + year_y_list).reset_index(drop=True)

    # Export matched series
    df_match.to_csv(f"{PARENT_DIR}/df_match.csv", index=False)

    return df_match


# df_match = match_ref_years(
#     df, series, reference_years, constant_reporting_level, constant_welfare_type, income_or_consumption
# )

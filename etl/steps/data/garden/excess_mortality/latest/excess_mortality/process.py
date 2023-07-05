"""Processing tools."""
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from structlog import get_logger

from etl.data_helpers import geo

log = get_logger()
# Maximum year
YEAR_MAX = 2023


def process_df(df: pd.DataFrame) -> pd.DataFrame:
    """Process data"""
    # Add baseline average
    log.info("\texcess_mortality: add `baseline_avg`")
    df = add_baseline_avg(df)
    # Add metrics
    log.info("\texcess_mortality: add metrics `excess_avg*`, `excess_proj*`, `p_avg*` and `p_proj*`")
    df = add_xm_and_p_score(df)
    # Make df long
    log.info("\texcess_mortality: make dataframe long (unpivot and pivot year data)")
    df = make_df_long(df)
    # Minor tweaks
    log.info("\texcess_mortality: minor tweaks")
    df = minor_tweaks(df)
    # Add cumulative metrics
    log.info("\texcess_mortality: add cumulative metrics")
    df = add_cum_metrics(df)
    # Add population
    log.info("\texcess_mortality: add population")
    df = add_population(df)
    # Final formatting
    log.info("\texcess_mortality: final formatting of dataframe")
    df = final_formatting(df)
    return df


def add_baseline_avg(df: pd.DataFrame) -> pd.DataFrame:
    """Add field `baseline_avg` to `df`"""
    column_new_metric = "baseline_avg"
    # calculate baseline average and standard error -----
    df[column_new_metric] = df[["2015", "2016", "2017", "2018", "2019"]].mean(axis=1)
    # because there's only one Week 53 in the past 5 years, use the baseline average from Week 52 instead. ONS does this.
    cols_link = ["entity", "time_unit", "age"]
    # Get rows with time == 53
    df_53 = df[df["time"] == 53].copy().drop(columns=[column_new_metric])
    df_52 = df[df["time"] == 52].copy()[cols_link + [column_new_metric]]
    df_53 = df_53.merge(df_52, on=cols_link)
    df = df[df.time != 53]
    df = pd.concat([df, df_53], ignore_index=True)
    return df


def add_xm_and_p_score(df: pd.DataFrame) -> pd.DataFrame:
    """Add fields `xm` and `p_score` to `df`"""
    COLUMNS_IDX = ["entity", "time", "time_unit"]

    # calculate both excess deaths and p-scores above both baselines: 5-yr avg and projected
    def _add_metrics(df: pd.DataFrame, year: str):
        if year == "2020":
            suffix = ""
        elif year in [str(year) for year in range(2021, YEAR_MAX + 1)]:
            digits = str(year)[-2:]
            suffix = f"_{digits}"
        else:
            raise ValueError(f"Unknown year {year}")
        return df.assign(
            **{
                f"excess_avg{suffix}": df[year] - df["baseline_avg"],
                f"p_avg{suffix}": (100 * (df[year] - df["baseline_avg"]) / df["baseline_avg"]).replace(
                    [np.inf, -np.inf], np.nan
                ),
                f"excess_proj{suffix}": df[year] - df[f"baseline_proj{suffix}"],
                f"p_proj{suffix}": (
                    100 * (df[year] - df[f"baseline_proj{suffix}"]) / df[f"baseline_proj{suffix}"]
                ).replace([np.inf, -np.inf], np.nan),
            }
        )
        return df

    def _get_p_scores(df: pd.DataFrame) -> pd.DataFrame:
        # get p-scores and make wide
        cols = ["baseline_proj", "excess_avg", "p_avg", "excess_proj", "p_proj"]
        metrics = ["baseline_avg"]
        for col in cols:
            metrics.append(col)
            for year in [str(year) for year in range(2021, YEAR_MAX + 1)]:
                metrics.append(f"{col}_{year[-2:]}")
        df_ = df[COLUMNS_IDX + ["age"] + metrics].copy()
        # Pivot
        df_ = df_.pivot(index=COLUMNS_IDX, columns="age", values=metrics)
        # Set column names
        df_.columns = ["_".join(col) for col in df_.columns.values]
        # Reset index
        df_ = df_.reset_index()
        return df_

    def _get_deaths(df: pd.DataFrame) -> pd.DataFrame:
        # Get deaths
        columns_idx = COLUMNS_IDX
        columns_years = [str(year) for year in range(2010, YEAR_MAX + 1)]
        df_ = df.loc[
            df["age"] == "all_ages",
            columns_idx + columns_years,
        ].copy()

        def _rename_columnn(col: str):
            if col in columns_years:
                col = f"deaths_{col}_all_ages"
            return col

        df_.columns = list(map(_rename_columnn, df_.columns))
        return df_

    # Add metrics for each year
    for year in [str(year) for year in range(2020, YEAR_MAX + 1)]:
        df = _add_metrics(df, year)
    # Get p-scores and deaths dataframes

    df_p = _get_p_scores(df)
    df_d = _get_deaths(df)
    # Merge
    df = df_p.merge(df_d, on=COLUMNS_IDX)
    # Round metric values
    columns = [col for col in df.columns if col not in COLUMNS_IDX]
    df[columns] = df[columns].round(2)
    return df


def make_df_long(df: pd.DataFrame) -> pd.DataFrame:
    """Make `df` long"""

    def _build_yearly_data(df, year, remove_w53):
        year = str(year)
        year_end = year[-2:]
        df_ = df[
            [
                "entity",
                "time",
                "time_unit",
                f"p_avg_{year_end}_0_14",
                f"p_avg_{year_end}_15_64",
                f"p_avg_{year_end}_65_74",
                f"p_avg_{year_end}_75_84",
                f"p_avg_{year_end}_85p",
                f"p_avg_{year_end}_all_ages",
                f"p_proj_{year_end}_0_14",
                f"p_proj_{year_end}_15_64",
                f"p_proj_{year_end}_65_74",
                f"p_proj_{year_end}_75_84",
                f"p_proj_{year_end}_85p",
                f"p_proj_{year_end}_all_ages",
                f"excess_proj_{year_end}_all_ages",
                f"baseline_proj_{year_end}_all_ages",
                f"deaths_{year}_all_ages",
            ]
        ].copy()
        if remove_w53:
            # remove week 53 from YYYY, which is only there because of the 2020 projection
            df_ = df_[df_["time"] != 53]
        # drop the "_yy" part of the column names and rename a column so I can bind them together with the p_score dataframe
        df_.columns = df_.columns.str.replace(f"_{year_end}", "")
        df_ = df_.rename(columns={f"deaths_{year}_all_ages": "deaths_since_2020_all_ages"})
        return df_

    def _get_date(year, time, time_unit):
        if time_unit == "monthly":
            if time == 12:
                date = datetime(year, 12, 31)
            else:
                date = datetime(year, time + 1, 1) - timedelta(days=1)
        elif time_unit == "weekly":
            # Use ISO 8601 week (use last sunday)
            date = datetime.strptime(f"{year}-{time}-0", "%G-%V-%w")
        else:
            raise ValueError(f"Unknown time unit {time_unit}")
        return date.strftime("%Y-%m-%d")

    # Get yearly data
    dfs = []
    for year in [year for year in range(2021, YEAR_MAX + 1)]:
        # Unpivot by year (only relevant columns)
        df_ = _build_yearly_data(df, year, True)
        # Add date
        df_["date"] = df_.apply(lambda x: _get_date(year, x["time"], x["time_unit"]), axis=1)
        dfs.append(df_)

    # Add date
    df["date"] = df.apply(lambda x: _get_date(2020, x["time"], x["time_unit"]), axis=1)
    # Rename column
    df["deaths_since_2020_all_ages"] = df["deaths_2020_all_ages"]
    # Merge
    df = pd.concat([df] + dfs, ignore_index=True)
    # Format date
    df["date"] = pd.to_datetime(df["date"])
    return df


def minor_tweaks(df: pd.DataFrame) -> pd.DataFrame:
    """Minor df changes"""
    # create a new dataframe and get rid of columns I don't need
    columns = [
        "entity",
        "date",
        "time",
        "time_unit",
        "baseline_avg_all_ages",
        "p_avg_0_14",
        "p_avg_15_64",
        "p_avg_65_74",
        "p_avg_75_84",
        "p_avg_85p",
        "p_avg_all_ages",
        "baseline_proj_all_ages",
        "p_proj_0_14",
        "p_proj_15_64",
        "p_proj_65_74",
        "p_proj_75_84",
        "p_proj_85p",
        "p_proj_all_ages",
        "excess_proj_all_ages",
        "deaths_since_2020_all_ages",
    ]
    cols_deaths = [f"deaths_{year}_all_ages" for year in range(2010, YEAR_MAX + 1)]
    df = df[columns + cols_deaths]

    # Question:
    # Some columns will have NaNs for 2021 and 2022 bc we did not propagate these there.

    # Rename columns
    df = df.rename(
        columns={
            "baseline_avg_all_ages": "average_deaths_2015_2019_all_ages",  # plenty of NaNs for >2020
            "baseline_proj_all_ages": "projected_deaths_since_2020_all_ages",
        },
    )

    # only keep Week 53 data from 2021 (did I mean 2020, then 2020-plus?)
    df.loc[
        df["time"] == 53,
        [
            "deaths_2015_all_ages",
            "deaths_2016_all_ages",
            "deaths_2017_all_ages",
            "deaths_2018_all_ages",
            "deaths_2019_all_ages",
        ],
    ] = np.nan
    return df


def add_cum_metrics(df: pd.DataFrame) -> pd.DataFrame:
    # calculate cumulative excess deaths and p-scores for each country
    df = (
        df.sort_values("date")
        .groupby("entity", as_index=False, group_keys=True)
        .apply(
            lambda x: x.assign(
                cum_excess_proj_all_ages=x["excess_proj_all_ages"].cumsum(),
                cum_proj_deaths_all_ages=x["projected_deaths_since_2020_all_ages"].cumsum(),
            )
        )
        .reset_index(drop=True)
    )
    # Estimate cum_p
    df = df.assign(
        cum_p_proj_all_ages=(df["cum_excess_proj_all_ages"] / df["cum_proj_deaths_all_ages"]) * 100,
    )

    # round the cumulative p-scores to 2 digits
    df["cum_p_proj_all_ages"] = df["cum_p_proj_all_ages"].round(2)
    return df


def add_population(df: pd.DataFrame) -> pd.DataFrame:
    # Load population data
    df["year"] = df["date"].dt.year
    df = geo.add_population_to_dataframe(df, country_col="entity", year_col="year")
    df = df.drop(columns=["year"])
    # Get per million metrics
    df["excess_per_million_proj_all_ages"] = df["excess_proj_all_ages"] / (df["population"] / 1e6)
    df["cum_excess_per_million_proj_all_ages"] = df["cum_excess_proj_all_ages"] / (df["population"] / 1e6)
    # Drop column population
    df = df.drop(columns=["population"])
    return df


def final_formatting(df: pd.DataFrame) -> pd.DataFrame:
    # Keep columns just in case they were in use on github.com/owid/owid-datasets
    # df = df.drop(
    #     columns=[
    #         "deaths_2010_all_ages",
    #         "deaths_2011_all_ages",
    #         "deaths_2012_all_ages",
    #         "deaths_2013_all_ages",
    #         "deaths_2014_all_ages",
    #     ]
    # )
    return df

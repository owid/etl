"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLS_WITH_DATA = [
    "manufactured_cigarettes",
    "manufactured_cigarettes_per_adult_per_day",
    "handrolled_cigarettes",
    "handrolled_cigarettes_per_adult_per_day",
    "total_cigarettes",
    "total_cigarettes_per_adult_per_day",
    "all_tobacco_products_tonnes",
    "all_tobacco_products_grams_per_adult_per_day",
]

MAN_KEY = "manufactured_cigarettes_per_adult_per_day"
HAND_KEY = "handrolled_cigarettes_per_adult_per_day"
TOTAL_KEY = "total_cigarettes_per_adult_per_day"
ALL_KEY = "all_tobacco_products_grams_per_adult_per_day"


def standardise_years(df):
    new_df = []
    for __, row in df.iterrows():
        year = row["year"]
        year_int = -1
        dict_from_row = row.to_dict()
        try:
            year_int = int(year)
            dict_from_row["year"] = year_int
            new_df.append(dict_from_row)
            continue
        except ValueError:
            if "." in year:
                year_int = int(year.split(".")[0])
            elif "/" in year:
                year_int = int(year.split("/")[0])
            if year_int > 0:
                dict_from_row["year"] = year_int
                new_df.append(dict_from_row)
                continue
            elif "-" in year:  # timeframe given in excel
                timeframe = year.split("-")
                start_year = int(timeframe[0])
                end_year = int(timeframe[1])
                if end_year < 100:
                    if end_year > (start_year % 100):
                        end_year = int(np.floor(start_year / 100) * 100 + end_year)
                    elif end_year < (start_year % 100):
                        end_year = int(np.floor(start_year / 100) * 100 + end_year + 100)
                elif end_year > 10000:
                    end_year = int(np.floor(end_year / 10))
                for year_in_timeframe in range(start_year, end_year + 1):
                    dict_from_row = row.to_dict()
                    dict_from_row.update({"year": year_in_timeframe})
                    new_df.append(dict_from_row)
    return pd.DataFrame(new_df)


def include_split_germany(tb, ds_population):
    """Include data for Germany 1945-1990 in the table by taking weighted average of East and West Germany data"""
    germany_tb = tb[tb["country"].isin(["West Germany", "East Germany"])]
    germany_tb = geo.add_population_to_table(germany_tb, ds_population, interpolate_missing_population=True)

    # calculate share of population for each year
    added_pop = germany_tb[["year", "population"]].groupby("year").sum().reset_index()

    for idx, row in germany_tb.iterrows():
        germany_tb.loc[idx, "share_of_population"] = (
            row["population"] / added_pop[added_pop["year"] == row["year"]]["population"].values[0]
        )
    # calculate share of cigarettes per adult for weighted average
    germany_tb[MAN_KEY] = germany_tb[MAN_KEY] * germany_tb["share_of_population"]
    germany_tb[HAND_KEY] = germany_tb[HAND_KEY] * germany_tb["share_of_population"]
    germany_tb[TOTAL_KEY] = germany_tb[TOTAL_KEY] * germany_tb["share_of_population"]
    germany_tb[ALL_KEY] = germany_tb[ALL_KEY] * germany_tb["share_of_population"]

    # sum up values for weighted average
    germany_tb = germany_tb[COLS_WITH_DATA + ["year"]].groupby("year").sum(min_count=1).reset_index()
    germany_tb["country"] = "Germany"

    return germany_tb


def cast_to_float(df, col):
    df[col] = df[col].astype("Float64")
    return df


def run(dest_dir: str) -> None:
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("cigarette_sales")
    # load population data
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["cigarette_sales"].reset_index()

    # Process data.

    # Fix years column (change dtype to integer and expand timeframes)
    df_years_fixed = Table(standardise_years(tb), metadata=tb.metadata)
    # replace table with dataframe with fixed years, concat with empty df to keep metadata
    tb = pr.concat([tb[0:0], df_years_fixed])

    # convert million to actual values
    tb["manufactured_cigarettes"] = tb["manufactured_cigarettes_millions"] * 1000000
    tb["handrolled_cigarettes"] = tb["handrolled_cigarettes_millions"] * 1000000
    tb["total_cigarettes"] = tb["total_cigarettes_millions"] * 1000000

    tb = tb.drop(
        columns=[
            "manufactured_cigarettes_millions",
            "handrolled_cigarettes_millions",
            "total_cigarettes_millions",
        ]
    )

    # Calculate weighted average for Germany 1950-1990
    germany_tb = include_split_germany(tb, ds_population)

    # include data for Germany 1950-1990
    tb = pr.concat([tb, germany_tb])

    # reorder columns
    tb = tb[["year", "country"] + COLS_WITH_DATA]

    # drop rows with no data (nan values)
    tb = tb.dropna(how="all", subset=COLS_WITH_DATA)

    # drop rows with zeros and NaNs
    tb_temp = tb.fillna(0)
    rows_to_drop = tb_temp[(tb_temp[COLS_WITH_DATA] == 0).all(axis=1)].index
    tb = tb.drop(rows_to_drop)

    # cast columns to float
    for col in COLS_WITH_DATA:
        tb = cast_to_float(tb, col)

    # harmonize countries (also exclude east and west germany)
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # remove duplicate data (from hidden rows in excel sheet)
    tb = tb.drop_duplicates(subset=["country", "year"])
    tb = tb.format(["country", "year"])

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

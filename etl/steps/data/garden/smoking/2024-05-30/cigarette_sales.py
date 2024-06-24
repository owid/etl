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


def standardize_years(df):
    """Standardise years column in the table by expanding timeframes and converting to integer.
    - If year is a single year (e.g. 1980), directly convert to integer.
    - If year is year with a footnote (e.g. 1980/1), convert to integer by removing the footnote.
    - If year is a year with a decimal (e.g. 1980.1), convert to integer by removing the decimal.
    - If year is a timeframe (e.g. 1980-1990), expand to individual years with the same data."""
    list_rows = []
    for __, row in df.iterrows():
        year = row["year"]
        year_int = -1
        dict_from_row = row.to_dict()
        try:
            year_int = int(year)
            dict_from_row["year"] = year_int
            list_rows.append(dict_from_row)
            continue
        except ValueError:
            if "." in year:  # year with decimal
                year_int = int(year.split(".")[0])
            elif "/" in year:  # year with footnote
                year_int = int(year.split("/")[0])
            if year_int > 0:  # year with decimal or footnote
                dict_from_row["year"] = year_int
                list_rows.append(dict_from_row)
                continue
            elif "-" in year:  # timeframe given in excel (e.g. 1980-1990)
                timeframe = year.split("-")
                start_year = int(timeframe[0])
                end_year = int(timeframe[1])
                if end_year < 100:  # if end year is given as two digits (e.g. 1980-90)
                    if end_year > (start_year % 100):
                        end_year = int(np.floor(start_year / 100) * 100 + end_year)
                    elif end_year < (start_year % 100):
                        end_year = int(np.floor(start_year / 100) * 100 + end_year + 100)
                elif (
                    end_year > 10000
                ):  # special case: some end years have a footnote that shows up as an extra digit (e.g. 1980-19901)
                    end_year = int(np.floor(end_year / 10))
                for year_in_timeframe in range(start_year, end_year + 1):  # expand timeframe to individual years
                    dict_from_row = row.to_dict()
                    dict_from_row.update({"year": year_in_timeframe})
                    list_rows.append(dict_from_row)
    return pd.DataFrame(list_rows)


def include_split_germany(tb, ds_population):
    """Include data for Germany 1945-1990 in the table by taking weighted average of East and West Germany data"""

    col_manufactured = "manufactured_cigarettes_per_adult_per_day"
    col_handrolled = "handrolled_cigarettes_per_adult_per_day"
    col_total_cigarettes = "total_cigarettes_per_adult_per_day"
    col_all_tobacco = "all_tobacco_products_grams_per_adult_per_day"

    germany_tb = tb[tb["country"].isin(["West Germany", "East Germany"])]
    germany_tb = geo.add_population_to_table(germany_tb, ds_population, interpolate_missing_population=True)

    # calculate share of population for each year
    added_pop = germany_tb[["year", "population"]].groupby("year").sum().reset_index()

    for idx, row in germany_tb.iterrows():
        germany_tb.loc[idx, "share_of_population"] = (
            row["population"] / added_pop[added_pop["year"] == row["year"]]["population"].values[0]
        )
    # calculate share of cigarettes per adult for weighted average
    germany_tb[col_manufactured] = germany_tb[col_manufactured] * germany_tb["share_of_population"]
    germany_tb[col_handrolled] = germany_tb[col_handrolled] * germany_tb["share_of_population"]
    germany_tb[col_total_cigarettes] = germany_tb[col_total_cigarettes] * germany_tb["share_of_population"]
    germany_tb[col_all_tobacco] = germany_tb[col_all_tobacco] * germany_tb["share_of_population"]

    # sum up values for weighted average
    germany_tb = germany_tb[COLS_WITH_DATA + ["year"]].groupby("year").sum(min_count=1).reset_index()
    germany_tb["country"] = "Germany"

    return germany_tb


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
    tb = Table(standardize_years(tb)).copy_metadata(tb)

    # convert million to actual values
    tb["manufactured_cigarettes"] = tb["manufactured_cigarettes_millions"] * 1e6
    tb["handrolled_cigarettes"] = tb["handrolled_cigarettes_millions"] * 1e6
    tb["total_cigarettes"] = tb["total_cigarettes_millions"] * 1e6

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

    # exclude East/ West Germany
    tb = tb[(tb["country"] != "East Germany") & (tb["country"] != "West Germany")]

    # reorder columns
    tb = tb[["year", "country"] + COLS_WITH_DATA]

    # drop rows with no data (rows with only NaN values)
    tb = tb.dropna(how="all", subset=COLS_WITH_DATA)

    # drop rows with no data (rows where some entries are 0 and some entries are NaN or all entries are zeros)
    tb_temp = tb.fillna(0)
    rows_to_drop = tb_temp[(tb_temp[COLS_WITH_DATA] == 0).all(axis=1)].index
    tb = tb.drop(rows_to_drop)

    # cast columns to float
    tb = tb.astype({column: "Float64" for column in COLS_WITH_DATA})

    # harmonize countries (also exclude east and west germany)
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # remove USSR data after 1991 (overlaps with data on country level)
    tb = tb[~((tb["country"] == "USSR") & (tb["year"] > 1991))]

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

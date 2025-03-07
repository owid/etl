"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SINGLE_YEARS = False


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("atus_0323")

    # Read tables from meadow dataset.
    act_data = ds_meadow.read("atus_act")
    who_data = ds_meadow.read("atus_who")
    sum_data = ds_meadow.read("atus_sum")

    # Merge table with activity and who data
    tb = pr.merge(act_data, who_data, on=["case_id", "activity_number"], how="left")

    # aggregate by category:
    # result: table with duration of each activity for each case_id
    tb_agg = (
        tb.groupby(["who_category", "case_id", "activity_number"]).agg({"activity_duration_24": "first"}).reset_index()
    )

    # merge with summary data (to get age and weights)
    tb_agg = tb_agg.merge(sum_data, on="case_id", how="left")

    # fill weights for 2020 in final weights column:
    tb_agg.loc[tb_agg["year"] == 2020, "final_weight"] = tb_agg.loc[tb_agg["year"] == 2020, "final_weight_2020"]

    # summarize data (first by case_id and category)
    tb_agg = (
        tb_agg.groupby(["who_category", "age", "case_id"])
        .agg({"activity_duration_24": "sum", "final_weight": "first", "year": "first", "gender": "first"})
        .reset_index()
    )

    ## Fill missing "who-categories" with 0 duration for each case id
    tb_agg = fill_missing_values(tb_agg)

    # Aggregate data over age and time period
    # data collection only started including work in 2010, so we start with 2010
    tb_agg_full = aggregate_over_age(tb_agg.copy(), start_year=2010, end_year=2023)

    # tables for gender, full:
    tb_agg_female = aggregate_over_age(tb_agg, start_year=2010, end_year=2023, gender="female")
    tb_agg_male = aggregate_over_age(tb_agg, start_year=2010, end_year=2023, gender="male")

    # concat all tables
    tb_who_final = pr.concat([tb_agg_full, tb_agg_female, tb_agg_male])

    # Create table for development over time for age groups
    tb_years = development_over_time_for_age_groups(tb_agg)
    tb_years_female = development_over_time_for_age_groups(tb_agg, gender="female")
    tb_years_male = development_over_time_for_age_groups(tb_agg, gender="male")

    tb_years = pr.concat([tb_years, tb_years_female, tb_years_male])

    # convert time to hours
    tb_who_final["t"] = tb_who_final["t"] / 60
    tb_years["t"] = tb_years["t"] / 60

    #
    # Process data.
    tb = tb_who_final.format(["timeframe", "age", "who_category", "gender"], short_name="atus_who")
    tb_years = tb_years.format(["age_bracket", "who_category", "year", "gender"], short_name="atus_who_years")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_years], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def fill_missing_values(tb_agg):
    """Fill values which do not appear for participant with 0 to ensure accurate averages"""

    # save age, weight and year information in dictionary
    age_dict = tb_agg.set_index("case_id")["age"].to_dict()
    weight_dict = tb_agg.set_index("case_id")["final_weight"].to_dict()
    year_dict = tb_agg.set_index("case_id")["year"].to_dict()
    gender_dict = tb_agg.set_index("case_id")["gender"].to_dict()

    # create new index by combining all categories and case_ids
    categories = tb_agg["who_category"].unique()
    case_ids = tb_agg["case_id"].unique()
    new_index = pd.MultiIndex.from_product([categories, case_ids], names=["who_category", "case_id"]).to_frame(
        index=False
    )

    # add age, weight and year information back to new index
    new_index["age"] = new_index["case_id"].map(age_dict)
    new_index["final_weight"] = new_index["case_id"].map(weight_dict)
    new_index["year"] = new_index["case_id"].map(year_dict)
    new_index["gender"] = new_index["case_id"].map(gender_dict)
    new_index = new_index.set_index(["who_category", "case_id", "age", "final_weight", "year", "gender"])

    # fill missing values with 0
    tb_agg = (
        tb_agg.set_index(["who_category", "case_id", "age", "final_weight", "year", "gender"])
        .reindex(new_index.index)
        .fillna({"activity_duration_24": 0})
    ).reset_index()

    return tb_agg


def development_over_time_for_age_groups(tb_agg, gender="all"):
    """Capture changes in time spent with different categories over time for different age groups
    Result: table with average time spend with each category for each age group for each year. This is not separated by gender."""
    # age starts with 15, 80/85 is topcoded
    age_brackets = [(15, 29), (30, 44), (45, 59), (60, 85)]


    tbs = []

    if gender == "female":
        tb_agg = tb_agg[tb_agg["gender"] == 2]
    elif gender == "male":
        tb_agg = tb_agg[tb_agg["gender"] == 1]

    # aggregate over year and age
    for age_b in age_brackets:
        tb_age = tb_agg[(tb_agg["age"] >= age_b[0]) & (tb_agg["age"] <= age_b[1])]
        tb_age = (
            tb_age.groupby(["who_category", "year"])
            .apply(
                lambda x: pd.Series(
                    {"t": np.sum(x["activity_duration_24"] * x["final_weight"]) / np.sum(x["final_weight"])}
                )
            )
            .reset_index()
        )
        tb_age["age_bracket"] = f"{age_b[0]}-{age_b[1]}"
        tb_age["gender"] = gender

        tbs.append(tb_age.copy())

    tb_years = (
        tb_agg.groupby(["who_category", "year"])
        .apply(
            lambda x: pd.Series(
                {"t": np.sum(x["activity_duration_24"] * x["final_weight"]) / np.sum(x["final_weight"])}
            )
        )
        .reset_index()
    )

    tb_years["age_bracket"] = "all"
    tb_years["gender"] = gender
    tbs.append(tb_years)
    tb_years = pr.concat(tbs)

    # add metadata:
    tb_years["t"] = tb_years["t"].copy_metadata(tb_years["year"])

    tb_years["country"] = "United States of America"
    return tb_years


def aggregate_over_age(tb_agg, start_year, end_year, gender="all"):
    """Aggregate data over age and time period. Start and end year are inclusive."""
    # remove co-worker category before 2010, since it gets recorded differently
    # this is not relevant since we use 2010-2023 timeframe, but kept for posterity
    tb_agg = tb_agg[~((tb_agg["year"] < 2010) & (tb_agg["who_category"] == "Co-worker"))]

    # remove 2020 data since it does not include march-may data
    tb_agg = tb_agg[tb_agg["year"] != 2020]

    # filter data to timeframe
    tb_agg = tb_agg[(tb_agg["year"] >= start_year) & (tb_agg["year"] <= end_year)]

    if gender == "female":
        tb_agg = tb_agg[tb_agg["gender"] == 2]
    elif gender == "male":
        tb_agg = tb_agg[tb_agg["gender"] == 1]

    # Now aggregate by category and age
    # This gives final result: average duration spent with each who_category for each age
    tb_agg = (
        tb_agg.groupby(["who_category", "age"])
        .apply(
            lambda x: pd.Series(
                {"t": np.sum(x["activity_duration_24"] * x["final_weight"]) / np.sum(x["final_weight"])}
            )
        )
        .reset_index()
    )

    # add metadata:
    tb_agg["t"] = tb_agg["t"].copy_metadata(tb_agg["age"])

    # Add country and gender information
    tb_agg["country"] = "United States of America"
    tb_agg["gender"] = gender

    # Add information about time period recorded
    tb_agg["timeframe"] = f"{start_year}-{end_year}"

    return tb_agg

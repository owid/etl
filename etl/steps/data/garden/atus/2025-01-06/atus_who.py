"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


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
    tb_agg = tb_agg[(tb_agg["year"] >= 2009) & (tb_agg["year"] <= 2019)]
    tb_agg = (
        tb_agg.groupby(["who_category", "age", "case_id"])
        .agg({"activity_duration_24": "sum", "final_weight": "first", "year": "first"})
        .reset_index()
    )

    ## Fill missing "who-categories" with 0 duration for each case id
    tb_agg = fill_missing_values(tb_agg)

    # remove co-worker category before 2010, since it gets recorded differently
    tb_agg = tb_agg[~((tb_agg["year"] < 2010) & (tb_agg["who_category"] == "Co-worker"))]

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

    # Add country information
    tb_agg["country"] = "United States of America"

    # Add information about time period recorded
    tb_agg["year_start"] = 2009
    tb_agg["year_end"] = 2019

    #
    # Process data.
    tb = tb_agg.format(["country", "age", "who_category"], short_name="atus_who")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def fill_missing_values(tb_agg):
    """Fill values which do not appear for participant with 0 to ensure accurate averages"""

    # save age, weight and year information in dictionaries
    age_dict = tb_agg.set_index("case_id")["age"].to_dict()
    weight_dict = tb_agg.set_index("case_id")["final_weight"].to_dict()
    year_dict = tb_agg.set_index("case_id")["year"].to_dict()

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
    new_index = new_index.set_index(["who_category", "case_id", "age", "final_weight", "year"])

    # fill missing values with 0
    tb_agg = (
        tb_agg.set_index(["who_category", "case_id", "age", "final_weight", "year"])
        .reindex(new_index.index)
        .fillna({"activity_duration_24": 0})
    ).reset_index()

    return tb_agg

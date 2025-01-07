"""Load a meadow dataset and create a garden dataset."""

import itertools

import numpy as np
import pandas as pd

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
    tb = act_data.merge(who_data, on=["case_id", "activity_number"], how="left")

    # aggregate by category:
    # result: table with duration of each activity for each case_id
    tb_agg = (
        tb.groupby(["who_category", "case_id", "activity_number"]).agg({"activity_duration_24": "first"}).reset_index()
    )

    # merge with summary data (to get age and weights)
    tb_agg = tb_agg.merge(sum_data, on="case_id", how="left")

    # summarize data (by category and age) and normalize with weights
    tb_agg = tb_agg[(tb_agg["year"] >= 2009) & (tb_agg["year"] <= 2019)]
    tb_agg = (
        tb_agg.groupby(["who_category", "age", "case_id"])
        .agg({"activity_duration_24": "sum", "final_weight": "first"})
        .reset_index()
    )

    age_dict = tb_agg.set_index("case_id")["age"].to_dict()
    weight_dict = tb_agg.set_index("case_id")["final_weight"].to_dict()

    categories = tb_agg["who_category"].unique()
    case_ids = tb_agg["case_id"].unique()
    new_index = pd.DataFrame(itertools.product(categories, case_ids), columns=["who_category", "case_id"])
    new_index["age"] = new_index["case_id"].map(age_dict)
    new_index["final_weight"] = new_index["case_id"].map(weight_dict)
    new_index = new_index.set_index(["who_category", "case_id", "age", "final_weight"])

    # remove co-worker category before 2010, since it gets recorded differently
    # TODO: figure out how to do this
    # tb_agg = tb_agg[~((tb_agg["year"] < 2010) & (tb_agg["who_category"] == "Co-worker"))]

    tb_agg = (
        tb_agg.set_index(["who_category", "case_id", "age", "final_weight"])
        .reindex(new_index.index)
        .fillna({"activity_duration_24": 0})
    ).reset_index()

    tb_agg = (
        tb_agg.groupby(["who_category", "age"])
        .apply(
            lambda x: pd.Series(
                {"t": np.sum(x["activity_duration_24"] * x["final_weight"]) / np.sum(x["final_weight"])}
            )
        )
        .reset_index()
    )

    tb_agg["country"] = "United States of America"

    #
    # Process data.
    tb = tb_agg.format(["country", "age", "who_category"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

"""Load a garden dataset and create a grapher dataset."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


REL_COL_VAR = [
    "close_to",
    "content",
    "days_exercise",
    "depressed",
    "discriminated",
    "donated",
    "expenses",
    "feel_anxious",
    "freedom",
    "happy",
    "hope_future",
    "life_sat",
    "lonely",
    "mental_health",
    "physical_hlth",
    "rel8",
    "say_in_govt",
    "trust_people",
    "volunteered",
    "wb_fiveyrs",
    "wb_today",
    "wb_improvement",
    "worry_safety",
]


def rm_sparse_countries(tb, var, var_cols, threshold=0.1):
    """Remove countries with 10% or more missing values."""
    tb_var = tb[var_cols].copy()
    tb_var = tb_var[tb_var[f"{var}_na_share"] < threshold].drop(columns=[f"{var}_na_share"])
    return tb_var


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("gfs_wave_one")

    # Read table from garden dataset.
    tb = ds_garden["gfs_summary"]

    tbs = []
    for var in REL_COL_VAR:
        var_cols = [col for col in tb.columns if var in col]
        tb_var = rm_sparse_countries(tb, var, var_cols).reset_index()
        tbs.append(tb_var)

    tb_res = pr.multi_merge(tbs, on="country", how="outer")

    tb_res["discriminated_often"] = tb_res["discriminated_ans_1_share"] + tb_res["discriminated_ans_2_share"]
    tb_res["trust_most"] = tb_res["trust_people_ans_1_share"] + tb_res["trust_people_ans_2_share"]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb_res], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()

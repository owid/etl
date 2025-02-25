"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COUNTRY_MAPPING = {
    1: "Argentina",
    2: "Australia",
    3: "Brazil",
    4: "Egypt",
    5: "Germany",
    6: "India",
    7: "Indonesia",
    8: "Israel",
    9: "Japan",
    10: "Kenya",
    11: "Mexico",
    12: "Nigeria",
    13: "Philippines",
    14: "Poland",
    16: "South Africa",
    17: "Spain",
    18: "Tanzania",
    19: "Turkey",
    20: "United Kingdom",
    22: "United States",
    23: "Sweden",
    24: "Hong Kong",
}

# -98: Saw, skipped
# 98: Don't know
# 99: Refused

# TODO: Think about categorical estimates for "scored" variables with less than 10 options
# binary values are given as 1/2
COLUMNS_MAPPING = {
    "id": "system",
    "country": "system",
    "wave": "system",  # wave number
    "mode_recruit": "",  # ?
    "mode_annual": "",  # ?
    "recruit_type": "system",  # type of recruit survey (seperate from annual (1), combined w annual (2))
    "doi_recruit": "system",  # end date of interview (recruit survey)
    "doi_annual": "system",  # end date of interview (annual survey)
    "abused": "binary",
    "after_death": "cat_3",
    "age": "demographic",  # age in years
    "approve_govt": "scored_5",
    "attend_svcs": "scored_5",
    "believe_god": "cat_5",
    "belonging": "scored_10",
    "bodily_pain": "scored_4",
    "born_country": "demographic",
    "capable": "scored_4",
    "cigarettes": "scored_97",
    "close_to": "binary",
    # cntry_rel questions are coded as categorical, but are scale of 1-10
    "cntry_rel_bud": "scored_10",
    "cntry_rel_chi": "scored_10",
    "cntry_rel_chr": "scored_10",
    "cntry_rel_hin": "scored_10",
    "cntry_rel_isl": "scored_10",
    "cntry_rel_jud": "scored_10",
    "cntry_rel_shi": "scored_10",
    "comfort_rel": "cat_4",
    "connected_rel": "scored_4",
    "content": "scored_10",
    "control_worry": "scored_4",
    "covid_death": "binary",
    "critical": "cat_4",
    "days_exercise": "scored_7",
    "depressed": "scored_4",
    "discriminated": "scored_4",
    "donated": "binary",
    "drinks": "scored_97",
    "education": "special_demographic",
    "education_3": "demographic",  # highest level of education (1-3)
    "employment": "demographic",  # employment status (1-7)
    "expect_good": "scored_10",
    "expenses": "scored_10",
    "father_loved": "binary",  # extra option 97 - does not apply
    "father_relatn": "scored_4",
    "feel_anxious": "scored_4",
    "forgive": "scored_4",
    "freedom": "scored_10",
    "gender": "demographic",  # (1-4)
    "give_up": "scored_10",
    "god_punish": "cat_4",
    "grateful": "scored_10",
    "group_not_rel": "scored_5",
    "happy": "scored_10",
    "health_growup": "scored_5",
    "health_prob": "binary",
    "help_stranger": "binary",
    "hope_future": "scored_10",
    "income": "special_demographic",
    "income_12yrs": "scored_4",
    "income_diff": "scored_5",
    "income_feelings": "demographic",  # feelings about income, also scored_4
    "interest": "scored_4",
    "life_approach": "cat_4",
    "life_balance": "scored_4",
    "life_purpose": "scored_10",
    "life_sat": "scored_10",
    "lonely": "scored_10",
    "loved_by_god": "scored_4",
    "marital_status": "demographic",  # (1-6),
    "mental_health": "scored_10",
    "mother_loved": "binary",  # extra option 97 - does not apply
    "mother_relatn": "scored_4",
    "num_children": "demographic",  # number of children, up to 97
    "num_household": "demographic",  # number of people (18+) in household, up to 96
    "obey_law": "scored_5",
    "outsider": "binary",  # extra option 97 - does not apply
    "own_rent_home": "demographic",  # 1-4
    "parents_12yrs": "cat_5",
    "peace": "scored_4",
    "people_help": "scored_10",
    "physical_hlth": "scored_10",
    "political_id": "special_demographic",
    "pray_meditate": "scored_4",
    "promote_good": "scored_10",
    "region1": "special_demographic",
    "region2": "special_demographic",
    "region3": "special_demographic",
    "rel_experienc": "binary",
    "rel_important": "binary",
    # religion questions probably need to be handled separately
    # all include extra options 96/ 97 - other/ no particular
    "rel1": "cat_15",
    "rel2": "cat_15",
    "rel3": "cat_13",
    "rel4": "cat_8",
    "rel5": "cat_4",
    "rel6": "cat_10",
    "rel7": "cat_3",
    "rel8": "cat_4",
    "rel9": "cat_6",
    "sacred_texts": "scored_4",
    "sat_live": "cat_3",
    "sat_relatnshp": "scored_10",
    "say_in_govt": "cat_3",
    "selfid1": "special_demographic",
    "selfid2": "special_demographic",
    "show_love": "scored_10",
    "suffering": "scored_4",
    "svcs_12yrs": "scored_4",
    "svcs_father": "scored_4",
    "svcs_mother": "scored_4",
    # teachings are coded as categorical, but are scale of 1-10
    "teachings_1": "scored_10",
    "teachings_2": "scored_10",
    "teachings_3": "scored_10",
    "teachings_4": "scored_10",
    "teachings_5": "scored_10",
    "teachings_6": "scored_10",
    "teachings_7": "scored_10",
    "teachings_8": "scored_10",
    "teachings_9": "scored_10",
    "teachings_10": "scored_10",
    "teachings_11": "scored_10",
    "teachings_12": "scored_10",
    "teachings_13": "scored_10",
    "teachings_14": "scored_10",
    "teachings_15": "scored_10",
    "tell_beliefs": "cat_4",
    "threat_life": "cat_4",
    "traits1": "scored_7",  # extroverted, enthusiastic
    "traits2": "scored_7",  # critical, quarrelsome
    "traits3": "scored_7",  # dependable, self-disciplined
    "traits4": "scored_7",  # anxious, easily upset
    "traits5": "scored_7",  # open to new experiences, complex
    "traits6": "scored_7",  # reserved, quiet
    "traits7": "scored_7",  # sympathetic, warm
    "traits8": "scored_7",  # disorganized, careless
    "traits9": "scored_7",  # calm, emotionally stable
    "traits10": "scored_7",  # conventional, uncreative
    "trust_people": "scored_5",
    "urban_rural": "demographic",  # 1-4
    "volunteered": "binary",
    "wb_fiveyrs": "scored_10",
    "wb_today": "scored_10",
    "worry_safety": "scored_10",
    "worthwhile": "scored_10",
    "annual_weight1": "system",  # annual weight year 1
    "strata": "system",
    "psu": "system",
    "full_partial": "binary",  # full (1) or partial (2) completed interview
}

# Define column groups
SYSTEM_COLS = [key for key, value in COLUMNS_MAPPING.items() if value == "system"]
DEMO_COLS = [key for key, value in COLUMNS_MAPPING.items() if value == "demographic"]
SCORED_10_COLS = [key for key, value in COLUMNS_MAPPING.items() if value == "scored_10"]
BINARY_COLS = [key for key, value in COLUMNS_MAPPING.items() if value == "binary"]
CAT_COLS = [key for key, value in COLUMNS_MAPPING.items() if value.startswith("cat")]
LOW_SCORED_COLS = [key for key, value in COLUMNS_MAPPING.items() if value in ["scored_4", "scored_5", "scored_7"]]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gfs_wave_one")

    # Read table from meadow dataset.
    tb = ds_meadow["gfs_wave_one"].reset_index()

    tb["country"] = tb["country"].map(COUNTRY_MAPPING)

    # cleaning nan values
    for val in ["-98", "98", "99", -98, 98, 99, " ", ""]:
        tb = tb.replace(val, np.nan)

    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Custom column: people who think their life will get better in the next 5 years
    tb["wb_improvement"] = tb.apply(get_ineq_nan, axis=1)  # 1 if yes, 2 if no

    for col in ["expenses", "lonely", "worry_safety"]:
        tb[col] = reverse_score(tb, col)

    # Calculate average scores/ shares of answers for all variables
    tb_scored_10 = average_scored(tb, cols=SCORED_10_COLS)
    tb_share_10 = share_categorical(tb, cols=SCORED_10_COLS)
    tb_binary = share_binary(tb, cols=BINARY_COLS + ["wb_improvement"])
    tb_cat = share_categorical(tb, cols=CAT_COLS)

    tb_scored_other = average_scored(tb, cols=LOW_SCORED_COLS)
    tb_cat_other = share_categorical(tb, cols=LOW_SCORED_COLS)

    # 97 is treated as 97 rather than 97+ (topcoded)
    # drinks: 116 rows
    # cigarettes: 45 rows
    tb_scored_97 = average_scored(tb, cols=["cigarettes", "drinks"])

    # Merge all tables and remove duplicate columns (na shares are calculated twice for some variables)
    tbs_full = pr.multi_merge(
        [tb_scored_10, tb_share_10, tb_binary, tb_cat, tb_scored_other, tb_cat_other, tb_scored_97],
        on=["country"],
        suffixes=["", "_DROP_COLUMN"],
    )
    tbs_full = tbs_full.drop(columns=[col for col in tbs_full.columns if col.endswith("_DROP_COLUMN")])

    # setting the year to 2023 for all data points (>84% of data)
    # for reference value counts: doi_annual
    # 2023    170562
    # 2022     32334
    # 2024         2
    tbs_full["year"] = 2023  # alternative would be: pd.to_datetime(tb["doi_annual"]).dt.year

    tbs_full.metadata = tb.metadata
    tbs_full.m.short_name = "gfs_wave_one"

    for col in tbs_full.columns:
        tbs_full[col].metadata = tb["country"].metadata

    tbs_full = tbs_full.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tbs_full], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def reverse_score(tb, col):
    """Reverse the score of a column with a numerical scale"""
    scale_ls = COLUMNS_MAPPING[col].split("_")
    if scale_ls[0] != "scored":
        raise ValueError(f"Column {col} is not a scored variable")
    else:
        num_scale = int(scale_ls[1])
        tb[col] = num_scale - tb[col]
        return tb[col]


def average_scored(tb, groups=["country"], cols=SCORED_10_COLS):
    tb_avg = tb[groups + cols + ["annual_weight1"]].copy()
    for col in cols:
        tb_avg[col] = tb_avg[col].astype("Int64")
        tb_avg[col] = tb_avg[col] * tb_avg["annual_weight1"]

    tb_na = get_na_share(tb, groups, cols)

    tb_avg = tb_avg.groupby(groups).apply(lambda x: x[cols].sum() / x["annual_weight1"].sum())

    tb_avg.columns = [f"{col}_mean" for col in cols]
    tb_avg = tb_avg.reset_index()

    tb_avg = pr.merge(tb_avg, tb_na, on=groups)

    return tb_avg


def share_binary(tb, groups=["country"], cols=BINARY_COLS):
    tb_binary = tb[groups + cols + ["annual_weight1"]].copy()
    # some of these have the extra option 97 - does not apply
    tb_binary = tb_binary.replace(97, np.nan)
    for col in cols:
        tb_binary[col] = tb_binary[col].astype("Int64")

    # group by variable (e.g. country) and calculate share of yes/no/na answers
    # tb_na = get_na_share(tb, groups, cols)
    col_ans = {1: "yes", 2: "no"}
    res_tbs = []
    for col in cols:
        res = tb_binary.groupby(groups + [col], dropna=False)["annual_weight1"].sum()
        denom = res.groupby(groups).sum()
        res = (res / denom).unstack()

        col_names = [col_ans[x] if x in col_ans.keys() else "na" for x in res.columns]
        res.columns = [f"{col}_{x}_share" for x in col_names]
        res_tbs.append(res.reset_index())

    return pr.multi_merge(res_tbs, on=groups)


def share_categorical(tb, groups: list = ["country"], cols: list = CAT_COLS):
    tb_cat = tb[groups + cols + ["annual_weight1"]].copy()
    res_tbs = []
    for col in cols:
        # res = tb_cat.groupby(groups + [col], dropna=False)[col].value_counts(normalize=True, dropna=False).unstack()
        res = tb_cat.groupby(groups + [col], dropna=False)["annual_weight1"].sum()
        denom = res.groupby(groups).sum()
        res = (res / denom).unstack()

        col_names = ["na" if pd.isna(x) else f"ans_{int(x)}" for x in res.columns]
        res.columns = [f"{col}_{x}_share" for x in col_names]
        res = res.fillna(0)  # if one category does not appear for country, fill with 0
        res = check_for_missing_answer(res, col)
        res_tbs.append(res.reset_index())

    return pr.multi_merge(res_tbs, on=groups)


def check_for_missing_answer(tb_cat, var: str):
    """Checks if all possible answers of a categorical variable are present in share table, if not adds missing columns
    tb_cat: "share table" for one categorical variable.
    The index is countries and the columns are the share of answers for each option (named with the number of the option)
    var: the variable name (question)

    returns: tb_cat with missing columns (if needed) added"""
    p_ans = COLUMNS_MAPPING[var].split("_")[1]  # number of possible answers
    p_ans_ls = [f"{var}_ans_{int(x)}_share" for x in range(1, int(p_ans) + 1)]
    missing_ans = [p_ans for p_ans in p_ans_ls if p_ans not in tb_cat.columns]
    if missing_ans:
        print(f"Missing answers for {var}: {missing_ans}")
        for ans in missing_ans:
            tb_cat[ans] = 0
    return tb_cat


def get_na_share(tb, groups: list, cols: list):
    """Calculates the share of missing values for each group for each variable in cols
    tb: Table with variables in each column
    groups: list of columns to group by (e.g. country)
    cols: list of columns to calculate missing value share for

    returns: Table tb_na with missing value share for each group and variable
    index"""
    tb_na = tb[groups + cols + ["annual_weight1"]].copy()
    for col in cols:
        tb_na[col] = tb_na[col].isna().astype("Int64") * tb_na["annual_weight1"]

    tb_na = tb_na.groupby(groups).apply(lambda x: x[cols].sum() / x["annual_weight1"].sum())
    tb_na.columns = [f"{col}_na_share" for col in cols]
    return tb_na.reset_index()


def get_ineq_nan(tb_row):
    """Return truth value of tb["wb_fiveyrs"] < tb["wb_today"] if both are not nan, else return nan"""
    if pd.isna(tb_row["wb_fiveyrs"]) or pd.isna(tb_row["wb_today"]):
        return np.nan
    else:
        return float(tb_row["wb_fiveyrs"] < tb_row["wb_today"]) + 1  # 1 if yes, 2 if no

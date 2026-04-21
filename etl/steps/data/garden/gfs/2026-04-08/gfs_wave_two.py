"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import processing as pr
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

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

# columns_mapping:
# - "system" columns are system variables, such as id, country, wave, etc.
# - "demographic" columns are demographic variables, such as age
# - "special_demographic" columns are demographic variables which are different for each region
# target varibles:
# - "binary" columns are binary variables, where 1 is yes and 2 is no
# - "cat_X" columns are categorical variables with X possible answers
# - "scored_X" columns are numerical variables with a scale of X (e.g. scored_10 means scale of 0-10)
# - "scored_97" columns are numerical variables with a scale up to 97 (these are topcoded to 97)
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


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("gfs_wave_two")

    # Read table from meadow dataset.
    tb = ds_meadow["gfs_wave_two"].reset_index()

    origins = tb["after_death_y1"].m.origins

    log.info("gfs_wave_two.run", rows=len(tb), cols=len(tb.columns))
    tb["country"] = tb["country"].map(COUNTRY_MAPPING)

    # split tables into year 1 and year 2, based on column names (columns ending in _y1 or _y2)

    common_cols = [
        col for col in tb.columns if not (col.endswith("_y1") or col.endswith("_y2") or col.startswith("annual_weight"))
    ]

    # year 1 columns:
    y1_cols = [col for col in tb.columns if col.endswith("_y1")]
    tb_y1 = tb[common_cols + y1_cols + ["annual_weight_c1"]].copy()
    tb_y1["annual_weight"] = tb_y1["annual_weight_c1"]
    tb_y1 = tb_y1.drop(columns=["annual_weight_c1"])

    # year 2 columns:
    y2_cols = [col for col in tb.columns if col.endswith("_y2")]
    tb_y2 = tb[common_cols + y2_cols + ["annual_weight_c2"]].copy()
    tb_y2["annual_weight"] = tb_y2["annual_weight_c2"]
    tb_y2 = tb_y2.drop(columns=["annual_weight_c2"])

    # rename columns to remove _y1 and _y2 suffixes
    tb_y1 = tb_y1.rename(columns=lambda x: x.removesuffix("_y1"))
    tb_y2 = tb_y2.rename(columns=lambda x: x.removesuffix("_y2"))

    # filter y2 table to only include rows where annual_weight is not null (these are the rows that were surveyed in year 2, which is a subset of year 1)
    tb_y2 = tb_y2[tb_y2["annual_weight"] != " "]
    log.info("gfs_wave_two.split", rows_y1=len(tb_y1), rows_y2=len(tb_y2))

    # set survey year:
    tb_y1["year"] = check_year(tb_y1)
    tb_y2["year"] = check_year(tb_y2)
    log.info("gfs_wave_two.years", year_y1=tb_y1["year"].iloc[0], year_y2=tb_y2["year"].iloc[0])

    # concatenate year 1 and year 2 tables
    tb = pd.concat([tb_y1, tb_y2], ignore_index=True)
    log.info("gfs_wave_two.concat", rows=len(tb), cols=len(tb.columns))

    # cleaning nan values
    # -98: Saw, skipped
    # 98: Don't know
    # 99: Refused

    for col in tb.select_dtypes(include="category").columns:
        tb[col] = tb[col].astype(str)

    for val in ["-98", "98", "99", -98, 98, 99, " ", "", "nan"]:
        tb = tb.replace(val, np.nan)  # type: ignore

    log.info("gfs_wave_two.replace", rows_total=len(tb))
    tb = paths.regions.harmonize_names(tb, countries_file=paths.country_mapping_path)

    # Custom column: people who think their life will get better in the next 5 years
    tb["wb_improvement"] = tb.apply(get_ineq_nan, axis=1)  # 1 if yes, 2 if no
    log.info("gfs_wave_two.wb_improvement", rows_total=len(tb), rows_non_null=tb["wb_improvement"].notnull().sum())

    # force all scored columns to be numeric
    for col in SCORED_10_COLS + LOW_SCORED_COLS + CAT_COLS:
        tb[col] = pd.to_numeric(tb[col], errors="coerce").astype("Int64")

    # force annual weight to be numeric
    tb["annual_weight"] = pd.to_numeric(tb["annual_weight"], errors="coerce")

    # reverse scoring for some variables, to make them more intuitive
    for col in ["expenses", "lonely", "worry_safety"]:
        tb[col] = reverse_score(tb, col)
    log.info("gfs_wave_two.reverse_score", rows=len(tb), cols=3)

    # Calculate average scores/ shares of answers for all variables
    GROUPS = ["country", "year"]
    tb_scored_10 = average_scored(tb, groups=GROUPS, cols=SCORED_10_COLS)
    log.info("gfs_wave_two.scored_10", rows=len(tb_scored_10), cols=len(tb_scored_10.columns))
    tb_share_10 = share_categorical(tb, groups=GROUPS, cols=SCORED_10_COLS)
    log.info("gfs_wave_two.share_10", rows=len(tb_share_10), cols=len(tb_share_10.columns))
    tb_binary = share_binary(tb, groups=GROUPS, cols=BINARY_COLS + ["wb_improvement"])
    log.info("gfs_wave_two.binary", rows=len(tb_binary), cols=len(tb_binary.columns))
    tb_cat = share_categorical(tb, groups=GROUPS, cols=CAT_COLS)
    log.info("gfs_wave_two.cat", rows=len(tb_cat), cols=len(tb_cat.columns))

    tb_scored_other = average_scored(tb, groups=GROUPS, cols=LOW_SCORED_COLS)
    tb_cat_other = share_categorical(tb, groups=GROUPS, cols=LOW_SCORED_COLS)

    # drinks and cigarettes are topcoded to 97, we treat them as 97 rather than 97+
    # drinks: 116 rows with maximum value of 97
    # cigarettes: 45 rows with maximum value of 97
    # drinks has one entry which is 4.5. We treat this as 5 (the scale is in whole numbers, so this is likely a data error).
    tb["drinks"] = tb["drinks"].astype(float).replace(4.5, 5)

    tb_scored_97 = average_scored(tb, groups=GROUPS, cols=["cigarettes", "drinks"])

    # Merge all tables and remove duplicate columns (na shares are calculated twice for some variables)
    tbs_full = pr.multi_merge(
        [tb_scored_10, tb_share_10, tb_binary, tb_cat, tb_scored_other, tb_cat_other, tb_scored_97],
        on=GROUPS,
        suffixes=["", "_DROP_COLUMN"],
    ).copy()
    tbs_full = tbs_full.drop(columns=[col for col in tbs_full.columns if col.endswith("_DROP_COLUMN")])
    log.info("gfs_wave_two.merge", rows=len(tbs_full), cols=len(tbs_full.columns))

    tbs_full.metadata = tb.metadata
    tbs_full.m.short_name = "gfs_wave_two"

    for col in tbs_full.columns:
        tbs_full[col].metadata = tb["country"].metadata
        tbs_full[col].metadata.origins = origins
        # if column ends in na_share, replace 1 with N/A
        # if na_share is 100%, that means no data is available
        if col.endswith("_na_share"):
            tbs_full[col] = tbs_full[col].replace(1, pd.NA)
    log.info("gfs_wave_two.metadata", metadata_copied=len(tbs_full.columns))

    tbs_full = tbs_full.format(["country", "year"])

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tbs_full], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    log.info("gfs_wave_two.save", rows=len(tbs_full), cols=len(tbs_full.columns))
    # Save changes in the new garden dataset.
    ds_garden.save()


def check_year(tb):
    tb["year"] = pd.to_datetime(tb["doi_annual"], format="%m/%d/%Y").dt.year
    # check that most common year appears in >82% of rows
    year_counts = tb["year"].value_counts(normalize=True)
    assert year_counts.max() > 0.82, "Most common year does not appear in >82% of rows"
    # return most common year
    return year_counts.idxmax()


def reverse_score(tb, col):
    """Reverse the score of a column with a numerical scale"""
    scale_ls = COLUMNS_MAPPING[col].split("_")
    if scale_ls[0] != "scored":
        raise ValueError(f"Column {col} is not a scored variable")
    else:
        num_scale = int(scale_ls[1])
        tb[col] = num_scale - tb[col]
        return tb[col]


def wmean(x, wtd_variable_col, weight_col):
    idx = ~x[wtd_variable_col].isna()
    weight_sum = x[idx][weight_col].sum()
    if weight_sum == 0:
        return np.nan  # Return NaN if the sum of weights is zero
    return x[idx][wtd_variable_col].sum() / weight_sum


def average_scored(tb, groups=["country"], cols=SCORED_10_COLS):
    tb_avg = tb[groups + cols + ["annual_weight"]].copy()
    means = []
    for col in cols:
        tb_avg[col] = tb_avg[col].astype("Int64")
        tb_avg[col] = tb_avg[col] * tb_avg["annual_weight"]
        g_by = tb_avg.groupby(groups).apply(wmean, wtd_variable_col=col, weight_col="annual_weight").reset_index()
        g_by.columns = groups + [f"{col}_mean"]
        means.append(g_by)

    tb_avg = pr.multi_merge(tables=means, on=groups)
    tb_avg = tb_avg.reset_index()

    tb_na = get_na_share(tb, groups, cols)

    tb_avg = pr.merge(tb_avg, tb_na, on=groups)

    return tb_avg


def share_binary(tb, groups=["country"], cols=BINARY_COLS, weight_col="annual_weight"):
    tb_binary = tb[groups + cols + [weight_col]].copy()
    # some of these have the extra option 97 - does not apply
    tb_binary = tb_binary.replace(97, np.nan)
    for col in cols:
        tb_binary[col] = pd.to_numeric(tb_binary[col], errors="coerce").astype("Int64")

    # group by variable (e.g. country) and calculate share of yes/no/na answers
    # tb_na = get_na_share(tb, groups, cols)
    col_ans = {1: "yes", 2: "no"}
    res_tbs = []
    for col in cols:
        res = tb_binary.groupby(groups + [col], dropna=False)[weight_col].sum().reset_index()
        denom = res.groupby(groups).sum().reset_index()
        # res_1 = (res / denom).unstack()

        # alternative approach:
        res = pr.merge(res, denom, on=groups, suffixes=("", "_country"), how="left")
        res["share"] = res[weight_col] / res[f"{weight_col}_country"]
        res = res.pivot(
            index=groups,
            columns=col,
            values="share",
        )
        col_names = [col_ans[x] if x in col_ans.keys() else "na" for x in res.columns]
        res.columns = [f"{col}_{x}_share" for x in col_names]
        res_tbs.append(res.reset_index())

    return pr.multi_merge(res_tbs, on=groups)


def share_categorical(tb, groups: list = ["country"], cols: list = CAT_COLS, weight_col="annual_weight"):
    tb_cat = tb[groups + cols + [weight_col]].copy()
    res_tbs = []
    for col in cols:
        res = tb_cat.groupby(groups + [col], dropna=False)[weight_col].sum().reset_index()
        denom = res.groupby(groups).sum().reset_index()
        # res = (res / denom).unstack()

        res = pr.merge(res, denom, on=groups, suffixes=("", "_country"), how="left")
        res["share"] = res[weight_col] / res[f"{weight_col}_country"]
        res = res.pivot(
            index=groups,
            columns=col,
            values="share",
        )

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

    # check actual number for possible cat answers
    p_ans = COLUMNS_MAPPING[var].split("_")[1]  # number of possible answers
    p_ans_ls = [f"{var}_ans_{int(x)}_share" for x in range(1, int(p_ans) + 1)]
    missing_ans = [p_ans for p_ans in p_ans_ls if p_ans not in tb_cat.columns]
    if missing_ans:
        log.info("gfs_wave_two.missing_answers", variable=var, missing_answers=missing_ans)
        for ans in missing_ans:
            tb_cat[ans] = 0
    return tb_cat


def get_na_share(tb, groups: list, cols: list, weight_col="annual_weight"):
    """Calculates the share of missing values for each group for each variable in cols
    tb: Table with variables in each column
    groups: list of columns to group by (e.g. country)
    cols: list of columns to calculate missing value share for

    returns: Table tb_na with missing value share for each group and variable
    index"""
    tb_na = tb[groups + cols + [weight_col]].copy()
    for col in cols:
        tb_na[col] = tb_na[col].isna().astype("Int64") * tb_na[weight_col]

    tb_na = tb_na.groupby(groups).apply(lambda x: x[cols].sum() / x[weight_col].sum())
    tb_na.columns = [f"{col}_na_share" for col in cols]
    return tb_na.reset_index()


def get_ineq_nan(tb_row):
    """Return truth value of tb["wb_fiveyrs"] < tb["wb_today"] if both are not nan, else return nan"""
    if pd.isna(tb_row["wb_fiveyrs"]) or pd.isna(tb_row["wb_today"]):
        return np.nan
    else:
        return float(tb_row["wb_fiveyrs"] < tb_row["wb_today"]) + 1  # 1 if yes, 2 if no

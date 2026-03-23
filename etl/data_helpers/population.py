"""Tools to load population data."""

from typing import Any

import pandas as pd
from owid.catalog import Dataset
from owid.datautils.dataframes import map_series
from structlog import get_logger

# Initialize logger.
log = get_logger()


def _parse_age_str(age_str: str) -> tuple[int, float]:
    """Parse an age bucket string into an inclusive (min, max) range.

    Open-ended buckets (e.g. '100+', 'all') get max=inf.
    """
    s = str(age_str).strip()
    if s == "all":
        return (0, float("inf"))
    elif s.endswith("+"):
        return (int(s[:-1]), float("inf"))
    elif "-" in s:
        lo, hi = s.split("-", 1)
        return (int(lo), int(hi))
    else:
        v = int(s)
        return (v, v)


def _select_age_buckets(available_ages: list[str], req_min: int, req_max: float) -> list[str]:
    """Select the minimal non-overlapping age buckets that cover [req_min, req_max].

    Strategy:
    1. Try an exact-match bucket first (handles 'all', '15+', '18+', '65+', '0-4', etc.)
    2. Otherwise collect every bucket whose range falls entirely within the request, then
       keep only "atomic" ones — buckets that contain no smaller candidate inside them.
    """
    parsed: dict[str, tuple[int, float]] = {}
    for a in available_ages:
        try:
            parsed[a] = _parse_age_str(a)
        except (ValueError, AttributeError):
            pass

    # 1. Exact match
    for age_str, (b_min, b_max) in parsed.items():
        if b_min == req_min and b_max == req_max:
            return [age_str]

    # 2. Atomic sub-buckets
    candidates = {a: r for a, r in parsed.items() if r[0] >= req_min and r[1] <= req_max}

    atomic = []
    for age_str, (b_min, b_max) in candidates.items():
        has_sub = any(
            other != age_str and other_r[0] >= b_min and other_r[1] <= b_max for other, other_r in candidates.items()
        )
        if not has_sub:
            atomic.append(age_str)

    return atomic


def add_population(
    df: pd.DataFrame,
    country_col: str,
    year_col: str,
    ds_un_wpp: Dataset | None = None,
    sex_col: str | None = None,
    sex_group_all: str | None = None,
    sex_group_female: str | None = None,
    sex_group_male: str | None = None,
    age_col: str | None = None,
    age_group_mapping: dict[str, Any | None] | None = None,
) -> pd.DataFrame:
    """Add population to dataframe using UN WPP 2024 (data://garden/un/2024-07-12/un_wpp).

    This function can be used to add population to a dataframe with the following dimensions: sex, age.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe. Must have a column for country and year.
    country_col : str
        Name of column with country names.
    year_col : str
        Name of column with years.
    ds_un_wpp : Dataset
        Population dataset from UN WPP (data://garden/un/2024-07-12/un_wpp).
        Must be provided explicitly; ensure it is listed in the step's DAG dependencies.
    sex_col: str, optional
        Name of the column with sex group dimension.
    sex_group_all: str, optional
        Value in `sex_col` for "all sexes" sex group.
    sex_group_female: str, optional
        Value in `sex_col` for "female" sex group.
    sex_group_male: str, optional
        Value in `sex_col` for "male" sex group.
    age_col: str, optional
        Name of the column with age group dimension.
    age_group_mapping: Dict[str, Optional[List[Optional[int]]]], optional
        Mapping from age group names to age ranges:
        {
            "age_group_name_in_input_dataframe": [min_age, max_age],
            ...
        }
        max_age=None means open-ended (no upper bound).

    Returns
    -------
    pd.DataFrame
        Dataframe with extra column `population`.
    """
    if sex_col:
        assert df[sex_col].notnull().all(), f"Column {sex_col} contains missing values!"
    if age_col:
        assert df[age_col].notnull().all(), f"Column {age_col} contains missing values!"

    if ds_un_wpp is None:
        raise ValueError(
            "ds_un_wpp must be provided. Load it explicitly with paths.load_dataset('un_wpp') and ensure "
            "data://garden/un/2024-07-12/un_wpp is in the step's DAG dependencies."
        )

    # Read population table from the 2024 dataset.
    # Use 'estimates' for historical years (1950-2023) and 'medium' for projections (2024+).
    pop = ds_un_wpp.read("population", safe_types=False)  # type: ignore
    pop = pop[pop["variant"].isin(["estimates", "medium"])].drop(columns=["variant"])
    # Keep only the population count; drop density/change columns.
    pop = pop.drop(columns=["population_change", "population_density"], errors="ignore")
    # Rename 'country' → 'location' to keep the rest of the logic uniform.
    pop = pop.rename(columns={"country": "location"})

    # Main merge columns
    columns_merge_df = [country_col, year_col]
    columns_merge_pop = ["location", "year"]

    # SEX GROUP
    if sex_col:
        sex_group_mapping = {}
        if sex_group_all:
            sex_group_mapping["all"] = sex_group_all
        if sex_group_female:
            sex_group_mapping["female"] = sex_group_female
        if sex_group_male:
            sex_group_mapping["male"] = sex_group_male
        if not sex_group_mapping:
            raise ValueError("Need to specify at least one argument of `sex_group_*`!")
        pop["sex"] = map_series(pop["sex"], sex_group_mapping)
        pop = pop.dropna(subset=["sex"])

        columns_merge_df.append(sex_col)
        columns_merge_pop.append("sex")
    else:
        pop = pop[pop["sex"] == "all"]

    # AGE GROUP
    if age_col:
        if not age_group_mapping:
            raise ValueError("Must specify a value for `age_group_mapping`!")

        columns_merge_df.append(age_col)
        columns_merge_pop.append("age")

        available_ages = pop["age"].unique().tolist()

        # Build each requested age group by selecting the appropriate atomic buckets.
        # NOTE: done in a loop because ranges can overlap across groups.
        df_pop = []
        for age_group_name, age_ranges in age_group_mapping.items():
            if not age_ranges:
                age_ranges = [None, None]
            req_min = age_ranges[0] if age_ranges[0] is not None else 0
            req_max = float("inf") if age_ranges[1] is None else age_ranges[1]

            buckets = _select_age_buckets(available_ages, req_min, req_max)
            if not buckets:
                log.warning(
                    f"No population age buckets found for range [{req_min}, {req_max}] "
                    f"(age group '{age_group_name}'). Population will be NaN for this group."
                )
                continue

            pop_g = (
                pop[pop["age"].isin(buckets)]
                .drop(columns=["age"])
                .groupby(["location", "year", "sex"], as_index=False, observed=True)
                .sum()
                .assign(age=age_group_name)
            )
            df_pop.append(pop_g)

        df_pop = pd.concat(df_pop, ignore_index=True).astype({"age": "category"})
    else:
        df_pop = pop.groupby(["location", "year", "sex"], as_index=False).sum().drop(columns=["age"], errors="ignore")

    # Merge
    columns_input = list(df.columns)
    df = df.merge(df_pop, how="left", left_on=columns_merge_df, right_on=columns_merge_pop, suffixes=("", "_extra"))
    df = df[columns_input + ["population"]]

    return df

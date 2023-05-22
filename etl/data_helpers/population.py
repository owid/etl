"""Tools to load population data."""
from typing import Any, Dict, Optional

import pandas as pd
from owid.catalog import Dataset

from etl.paths import DATA_DIR


def add_population(
    df: pd.DataFrame,
    country_col: str,
    year_col: str,
    sex_col: Optional[str] = None,
    sex_group_all: Optional[str] = None,
    sex_group_female: Optional[str] = None,
    sex_group_male: Optional[str] = None,
    age_col: Optional[str] = None,
    age_group_mapping: Optional[Dict[str, Optional[Any]]] = None,
) -> pd.DataFrame:

    """Add population to dataframe.

    Currently uses population from UN WPP 2022, as this dataset contains dissagregated data by age and sex groups.

    This function can be used to add population to a dataframe with the following dimensions: sex, age.

    Parameters
    ----------
    df : pd.DataFrame
        Input dataframe. Must have a column for country and year.
    country_col : str
        Name of column with country names.
    year_col : str
        Name of column with years.
    sex_col: str, optional
        Name of the column with sex group dimension.
    sex_group_all: str, optional
        Value in `sex_col` for "all sexes" sex group.
    sex_group_female: str, optional
        Value in `sex_col` for "female" sex group.
    sex_group_male: str, optional
        value in `sex_col` for "male" sex group.
    age_col: str, optional
        Name of the column with age group dimension.
    age_group_mapping: Dict[str, Optional[List[Optional[int]]]], optional
        Mapping from age group names to age ranges. The format of this argument is:
        {
            "age_group_name_in_input_dataframe": [min_age, max_age],
            ...
        }

        Population within the range [min_age, max_age] will be assigned to the age group `age_group_name_in_input_dataframe`.
        To get single-year values, use max_age = min_age + 1.

    Returns
    -------
    pd.DataFrame
        Dataframe with extra column `population`.
    """
    # Load granular population dataset
    ds = Dataset(DATA_DIR / "garden" / "un" / "2022-07-11" / "un_wpp")
    pop = ds["population_granular"]
    # Keep only variant='medium'
    pop = pop[pop["variant"] == "medium"].drop(columns=["variant"])
    # Keep only metric='population'
    pop = pop[pop["metric"] == "population"].drop(columns=["metric"]).rename(columns={"value": "population"})

    # Main index columns
    columns_merge_df = [country_col, year_col]
    columns_merge_pop = ["location", "year"]

    # SEX GROUP
    if sex_col:
        # Rename sex groups
        sex_group_mapping = {}
        if sex_group_all:
            sex_group_mapping["all"] = sex_group_all
        if sex_group_female:
            sex_group_mapping["female"] = sex_group_female
        if sex_group_male:
            sex_group_mapping["male"] = sex_group_male
        if not sex_group_mapping:
            raise ValueError("Need to specify at least of argument of `sex_group_*`!")
        pop["sex"] = pop["sex"].map(sex_group_mapping)
        pop = pop.dropna(subset=["sex"])

        # Add additional index columns
        columns_merge_df.append(sex_col)
        columns_merge_pop.append("sex")
    else:
        pop = pop[pop["sex"] == "all"]

    # AGE GROUP
    if age_col:
        if not age_group_mapping:
            raise ValueError("Must specify a value for `age_group_mapping`!")
        # Add additional index columns
        columns_merge_df.append(age_col)
        columns_merge_pop.append("age")

        # Build age groups
        df_pop = []
        pop["age"] = pop["age"].replace({"100+": 100}).astype("uint")
        for age_group_name, age_ranges in age_group_mapping.items():
            if not age_ranges:
                age_ranges = [None, None]
            # Define min and max age range in group
            age_min = age_ranges[0] if age_ranges[0] is not None else -1
            age_max = age_ranges[1] if age_ranges[1] is not None else 1000
            # Keep ages in group - allows for selection of single years in group
            pop_g = pop[
                (pop["age"] >= age_min) & (pop["age"] <= age_max)
            ].copy()  # Group by dimensions, replace age group name
            pop_g = (
                pop_g.drop(columns=["age"])
                .groupby(["location", "year", "sex"], as_index=False, observed=True)
                .sum()
                .assign(age=age_group_name)
            )
            # Add dataframe to list
            df_pop.append(pop_g)
        df_pop = pd.concat(df_pop, ignore_index=True)
    else:
        df_pop = pop.groupby(["location", "year", "sex"], as_index=False).sum().drop(columns=["age"])

    # Merge
    columns_input = list(df.columns)
    df = df.merge(df_pop, how="left", left_on=columns_merge_df, right_on=columns_merge_pop, suffixes=("", "_extra"))
    df = df[columns_input + ["population"]]

    return df

import json
from typing import List, cast

import numpy as np
import pandas as pd
from owid.catalog import Table, VariableMeta
from owid.catalog.utils import underscore
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("unodc.start")

    # read dataset from meadow
    ds_meadow = paths.meadow_dataset
    tb_meadow = ds_meadow["unodc"]

    df = pd.DataFrame(tb_meadow)
    df = df.reset_index()
    log.info("unodc.exclude_countries")
    df = exclude_countries(df)

    log.info("unodc.harmonize_countries")
    df = harmonize_countries(df)

    df = clean_up_categories(df)
    tb_garden_list = clean_data(df)

    # create new dataset with the same metadata as meadow
    ds_garden = create_dataset(dest_dir, tables=tb_garden_list, default_metadata=ds_meadow.metadata)
    ds_garden.save()

    log.info("unodc.end")


def load_excluded_countries() -> List[str]:
    with open(paths.excluded_countries_path, "r") as f:
        data = json.load(f)
        assert isinstance(data, list)
    return data


def exclude_countries(df: pd.DataFrame) -> pd.DataFrame:
    excluded_countries = load_excluded_countries()
    return cast(pd.DataFrame, df.loc[~df.country.isin(excluded_countries)])


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    unharmonized_countries = df["country"]
    df = geo.harmonize_countries(df=df, countries_file=str(paths.country_mapping_path))

    missing_countries = set(unharmonized_countries[df.country.isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {paths.country_mapping_path} to include these country "
            f"names; or (b) add them to {paths.excluded_countries_path}."
            f"Raw country names: {missing_countries}"
        )

    return df


def clean_data(df: pd.DataFrame) -> list[Table]:
    """
    Splitting the data into four dataframes/tables based on the dimension columns:
    * Total
    * by mechanism
    * by relationship to perpertrator
    * by situational context
    """
    df["age"] = df["age"].map({"Total": "All ages"}, na_action="ignore").fillna(df["age"])
    df["sex"] = df["sex"].map({"Total": "Both sexes"}, na_action="ignore").fillna(df["sex"])

    tb_mech = create_table(df, table_name="by mechanisms")
    tb_perp = create_table(df, table_name="by relationship to perpetrator")
    tb_situ = create_table(df, table_name="by situational context")
    tb_tot = create_total_table(df)

    tb_share = calculate_share_of_homicides(tb_tot, tb_perp)
    tb_share.update_metadata_from_yaml(paths.metadata_path, "share")
    tb_garden_list = [tb_mech, tb_tot, tb_perp, tb_situ, tb_share]

    return tb_garden_list


def create_table(df: pd.DataFrame, table_name: str) -> Table:
    """
    Create the homicides by mechanism dataframe where we will have  homicides/homicide rate
    disaggregated by mechanism (e.g. weapon)

    """
    assert any(df["dimension"] == table_name), "table_name must be a dimension in df"
    df_filter = df[df["dimension"] == table_name]

    # Make the table wider so we have a column for each mechanism
    tb_filter = pivot_and_format_table(
        df_filter,
        drop_columns=["region", "subregion", "indicator", "dimension", "source"],
        pivot_index=["country", "year"],
        pivot_values=["value"],
        pivot_columns=["unit_of_measurement", "category", "sex", "age"],
        table_name=table_name,
    )

    return tb_filter


def create_total_table(df: pd.DataFrame) -> Table:
    """
    Create the total homicides dataframe where we will have total homicides/homicide rate
    disaggregated by age and sex
    """
    df_tot = df[df["dimension"] == "Total"]
    # To escape the dataframe slice warnings
    df_tot = df_tot.copy(deep=True)
    # There are some duplicates when sex is unknown so let's remove those rows
    df_tot = df_tot[df_tot["sex"] != "Unknown"]

    # Make it more obvious what total age and total sex means

    df_tot = pivot_and_format_table(
        df_tot,
        drop_columns=["region", "subregion", "indicator", "dimension", "category", "source"],
        pivot_index=["country", "year"],
        pivot_values=["value"],
        pivot_columns=["unit_of_measurement", "sex", "age"],
        table_name="Total",
    )
    df_tot = df_tot.dropna(how="all", axis=1)

    return df_tot


def pivot_and_format_table(df_piv, drop_columns, pivot_index, pivot_values, pivot_columns, table_name) -> Table:
    """
    - Dropping a selection of columns
    - Pivoting by the desired disaggregations e.g. category, unit of measurement
    - Tidying the column names
    """
    df_piv = df_piv.drop(columns=drop_columns)
    df_piv = df_piv.pivot(index=pivot_index, columns=pivot_columns, values=pivot_values)

    df_piv.columns = df_piv.columns.droplevel(0)
    tb_garden = Table(short_name=underscore(table_name))
    for col in df_piv.columns:
        col_metadata = build_metadata(col, table_name=table_name)
        new_col = underscore(" ".join(col).strip())
        tb_garden[new_col] = df_piv[col]
        tb_garden[new_col].metadata = col_metadata

    return tb_garden


def build_metadata(col: tuple, table_name: str) -> VariableMeta:
    """
    Building the variable level metadata for each of the age-sex-metric combinations
    """
    metric_dict = {
        "Counts": {
            "title": "Number of homicides",
            "unit": "homicides",
            "short_unit": "",
            "numDecimalPlaces": 0,
        },
        "Rate per 100,000 population": {
            "title": "Homicide rate per 100,000 population",
            "unit": "homicides per 100,000 people",
            "short_unit": "",
            "numDecimalPlaces": 2,
        },
    }

    if table_name == "by mechanisms":
        title = f"{metric_dict[col[0]]['title']} - {col[1]}"
        description = (
            f"The {metric_dict[col[0]]['title'].lower()}, where the homicide was carried out using {col[1].lower()}."
        )
    elif table_name == "Total":
        title = f"{metric_dict[col[0]]['title']} - {col[1]} - {col[2]}"
        description = f"The {metric_dict[col[0]]['title'].lower()} recorded in a year."
    elif table_name == "by relationship to perpetrator":
        title = f"{metric_dict[col[0]]['title']} - {col[1]} - {col[2]} - {col[3]}"
        description = f"The {metric_dict[col[0]]['title'].lower()} shown by the victims relationship to the perpertrator. The age and sex characteristics relate to that of the victim, rather than the perpertrator."
    elif table_name == "by situational context":
        title = f"{metric_dict[col[0]]['title']} - {col[1]} - {col[2]} - {col[3]}"
        description = f"The {metric_dict[col[0]]['title'].lower()} shown by the situational context of the homicide."
    else:
        title = ""
        description = ""
    meta = VariableMeta(
        title=title,
        description=description,
        unit=f"{metric_dict[col[0]]['unit']}",
        short_unit=f"{metric_dict[col[0]]['short_unit']}",
    )
    meta.display = {
        "numDecimalPlaces": metric_dict[col[0]]["numDecimalPlaces"],
    }
    return meta


def clean_up_categories(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make the categories used in the dataset a bit more readable.

    """
    category_dict = {
        "Firearms or explosives - firearms": "firearms",
        "Another weapon - sharp object": "a sharp object",
        "Unspecified means": "unspecified means",
        "Without a weapon/ other Mechanism": " without a weapon or by another mechanism",
        "Firearms or explosives": "firearms or explosives",
        "Another weapon": "sharp or blunt object, including motor vehicles",
        "Intimate partner or family member": "Perpertrator is an intimate partner or family member",
        "Intimate partner or family member: Intimate partner": "Perpertrator is an intimate partner",
        "Intimate partner or family member: Family member": "Perpertrator is a family member",
        "Other Perpetrator known to the victim": "Another known perpetrator",
        "Perpetrator unknown": "Perpertrator is unknown",
        "Relationship to perpetrator is not known": "Perpertrator where the relationship to the victim is not known",
        "Socio-political homicide - terrorist offences": "Terrorist offences",
        "Unknown types of homicide": "Unknown situational context",
    }
    df = df.replace({"category": category_dict})

    assert df["category"].isna().sum() == 0
    return df


def calculate_share_of_homicides(total_table: Table, perp_table: Table) -> Table:
    """
    Calculate the share of total homicides where:

    * The perpertrator is an intimate partner
    * The perpertrator is a family member
    * The perpertrator is unknown
    """
    merge_table = pd.merge(total_table, perp_table, on=["country", "year"])

    sexes = ["both_sexes", "female", "male"]
    perpertrators = [
        "perpertrator_is_an_intimate_partner",
        "perpertrator_is_a_family_member",
        "perpertrator_is_unknown",
    ]
    share_df = pd.DataFrame()
    for sex in sexes:
        sex_select = f"counts_{sex}_all_ages"
        for perp in perpertrators:
            perp_select = f"counts_{perp}_{sex}_all_ages"
            new_col = underscore(f"Share of homicides of {sex} where the {perp}")
            share_df[new_col] = (merge_table[perp_select] / merge_table[sex_select]) * 100
            share_df[new_col] = share_df[new_col].replace(np.inf, np.nan)
    share_table = Table(share_df, short_name="share")
    return share_table

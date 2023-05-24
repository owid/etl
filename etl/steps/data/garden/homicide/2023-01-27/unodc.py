import json
from typing import List, cast

import pandas as pd
from owid.catalog import Dataset, Table, VariableMeta
from owid.catalog.utils import underscore
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("unodc.start")

    # read dataset from meadow
    ds_meadow = paths.meadow_dataset
    tb_meadow = ds_meadow["unodc"]

    df = pd.DataFrame(tb_meadow)

    log.info("unodc.exclude_countries")
    df = exclude_countries(df)

    log.info("unodc.harmonize_countries")
    df = harmonize_countries(df)

    df = clean_up_categories(df)
    tb_garden_list = clean_data(df)

    # create new dataset with the same metadata as meadow
    ds_garden = Dataset.create_empty(dest_dir, metadata=ds_meadow.metadata)
    for tb in tb_garden_list:
        ds_garden.add(tb)
        ds_garden.save()
    # ds_garden = create_dataset(dest_dir, tables=[df_mech, df_tot])
    # ds_garden.save()

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
    * by relationship to perpatrator
    * by situational context
    """
    df_mech = create_mechanism_table(df, table_name="by mechanisms")
    df_tot = create_total_table(df)

    # tb_garden = pd.merge(df_mech, df_tot, how="outer", on=["country", "year"])

    tb_garden_list = [df_mech, df_tot]

    return tb_garden_list


def create_mechanism_table(df: pd.DataFrame, table_name: str) -> Table:
    """
    Create the homicides by mechanism dataframe where we will have  homicides/homicide rate
    disaggregated by mechanism (e.g. weapon)

    """
    assert any(df["dimension"] == table_name), "table_name must be a dimension in df"
    df_mech = df[df["dimension"] == table_name]

    # Make the table wider so we have a column for each mechanism
    df_mech = pivot_and_format_table(
        df_mech,
        drop_columns=["region", "subregion", "indicator", "dimension", "source", "sex", "age"],
        pivot_index=["country", "year"],
        pivot_values=["value"],
        pivot_columns=["unit_of_measurement", "category"],
        table_name=table_name,
    )

    return df_mech


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

    df_tot["age"] = df_tot["age"].map({"Total": "All ages"}, na_action="ignore").fillna(df_tot["age"])
    df_tot["sex"] = df_tot["sex"].map({"Total": "Both sexes"}, na_action="ignore").fillna(df_tot["sex"])

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


def pivot_and_format_table(df, drop_columns, pivot_index, pivot_values, pivot_columns, table_name) -> Table:
    """
    - Dropping a selection of columns
    - Pivoting by the desired disaggregations e.g. category, unit of measurement
    - Tidying the column names
    """
    df = df.drop(columns=drop_columns)
    df = df.pivot(index=pivot_index, columns=pivot_columns, values=pivot_values)

    df.columns = df.columns.droplevel(0)
    tb_garden = Table(short_name=underscore(table_name))
    for col in df.columns:
        col_metadata = build_metadata(col, table_name=table_name)
        new_col = underscore(" ".join(col).strip())
        tb_garden[new_col] = df[col]
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
        description = f"The {metric_dict[col[0]]['title'].lower()} for {col[1].lower()}, {col[2].lower()}"
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
    category_dict = {
        "Firearms or explosives - firearms": "firearms",
        "Another weapon - sharp object": "a sharp object",
        "Unspecified means": "unspecified means",
        "Without a weapon/ other Mechanism": " without a weapon or by another mechanism",
        "Firearms or explosives": "firearms or explosives",
        "Another weapon": "an unspecified weapon",
    }
    df = df.replace({"category": category_dict})

    assert df["category"].isna().sum() == 0
    return df

from typing import Dict

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table

from etl.data_helpers import geo


def create_omms(tables_dict: Dict[str, Table], ds_population: Dataset, ds_regions: Dataset) -> None:
    # Adding a global total for Yaws - adding to existing variable
    add_global_yaws(tables_dict, ds_regions)
    # Adding a variables for neonatal cases per million
    add_neonatal_tetanus_cases_per_mil(tables_dict, ds_population)
    # Adding the % of people without access to clean cooking fuels (100 - existing variable)
    add_percentage_without_clean_cooking_fuels(tables_dict)
    # Adding the number of people without access to clean cooking fuels (population in year - existing variable)
    add_population_without_clean_cooking_fuels(tables_dict, ds_population)

    add_global_total_leprosy(tables_dict, ds_regions)

    add_both_sexes_for_pneumonia(tables_dict)

    # df_variables = add_youth_mortality_rates(
    #    df_variables=df_variables,
    #    younger_ind="Indicator:Under-five mortality rate (per 1000 live births) (SDG 3.2.1) - Sex:Both sexes",
    #    older_ind="Indicator:Mortality rate among children ages 5 to 9 years (per 1000 children aged 5) - Sex:Both sexes",
    #    new_ind_name_short="Under-ten mortality rate",
    #    new_ind_name_long="Indicator:Under-ten mortality rate (per 1000 live births)",
    #    new_ind_desc="Definition: Under ten mortality rate is the share of newborns who die before reaching the age of 10. It is calculated by OWID based on WHO Global Health Observatory data.",
    # )


def add_both_sexes_for_pneumonia(tables_dict: dict[str, Table]) -> None:
    """We miss `both sexes` for pneumonia. Calculate it by averaging estimates for both sexes."""
    indicator_name = "Children aged < 5 years with pneumonia symptoms taken to a health facility (%)"
    col = "children_aged__lt__5_years_with_pneumonia_symptoms_taken_to_a_health_facility__pct"
    tb = tables_dict[indicator_name]

    # create `both sexes` by averaging males and females (this is not ideal as both populations might
    # not be equal, but we don't have that data)
    tb = tb.query("sex.notnull()")
    both_sexes_avg = tb[[col]].groupby(["year", "country"], observed=True).mean().reset_index()
    both_sexes_avg["sex"] = "both sexes"
    for col in tb.index.names:
        if col not in ("year", "country", "sex"):
            both_sexes_avg[col] = np.nan

    both_sexes_avg = both_sexes_avg.set_index(tb.index.names)

    tb = pd.concat([tb, both_sexes_avg]).copy_metadata(tb)

    tables_dict[indicator_name] = tb


def add_population_without_clean_cooking_fuels(tables_dict: dict[str, Table], ds_population: Dataset) -> None:
    indicator_name = "Population with primary reliance on clean fuels and technologies for cooking (in millions)"
    col = "population_with_primary_reliance_on_clean_fuels_and_technologies_for_cooking__in_millions"
    new_col = "population_without_primary_reliance_on_clean_fuels_and_technologies_for_cooking__in_millions"

    tb = tables_dict[indicator_name]
    tb[col].m.unit = "persons"

    # Add population
    tb = geo.add_population_to_table(tb=tb.reset_index(), ds_population=ds_population, warn_on_missing_countries=False)

    # Calculate the number of people without access to clean cooking fuels
    tb[new_col] = tb["population"] / 1000000 - tb[col]

    # Some estimates could be negative because WHO uses slightly different population estimates, limit them to 0
    tb[new_col] = tb[new_col].clip(lower=0)

    # Drop rows that aren't Total from the new column
    tb.loc[tb.residence_area_type != "Total", new_col] = np.nan

    tables_dict[indicator_name] = tb.drop(columns=["population"]).set_index(["year", "country", "residence_area_type"])


def add_percentage_without_clean_cooking_fuels(tables_dict: dict[str, Table]) -> None:
    indicator_name = "Proportion of population with primary reliance on clean fuels and technologies for cooking (%)"
    col = "proportion_of_population_with_primary_reliance_on_clean_fuels_and_technologies_for_cooking__pct"
    new_col = "proportion_of_population_without_primary_reliance_on_clean_fuels_and_technologies_for_cooking__pct"

    tb = tables_dict[indicator_name]

    tb[new_col] = 100 - tb[col]


# def adjust_mortality_rates(younger_df: pd.DataFrame, older_df: pd.DataFrame, output_str: str) -> pd.Series:
#     raise NotImplementedError("This function was commented in importers repository")
#     df = younger_df.merge(older_df, on=["country", "year"], how="outer")
#     df["adjusted_older_rate"] = ((1000 - df["value_x"]) / 1000) * df["value_y"]
#     df[output_str] = df["adjusted_older_rate"] + df["value_x"]
#     out_df = df[["country", "year", output_str]].dropna()

#     return out_df


# def add_youth_mortality_rates(
#     df_variables: pd.DataFrame,
#     younger_ind: str,
#     older_ind: str,
#     new_ind_name_short: str,
#     new_ind_name_long: str,
#     new_ind_desc: str,
# ) -> pd.DataFrame:
#     younger_id, younger_df = get_dataframe_from_variable_name(df_variables, younger_ind)
#     older_id, older_df = get_dataframe_from_variable_name(df_variables, older_ind)
#     new_df = adjust_mortality_rates(
#         younger_df,
#         older_df,
#         new_ind_name_short,
#     )
#     new_var = df_variables[df_variables["name"] == younger_ind].copy()
#     new_var["name"] = new_ind_name_long
#     new_var["description"] = new_ind_desc
#     new_var["id"] = max(df_variables["id"]) + 1

#     new_df.to_csv(
#         os.path.join(
#             OUTPATH,
#             "datapoints",
#             "datapoints_%s.csv" % str(max(df_variables["id"]) + 1),
#         )
#     )
#     df_variables = pd.concat([df_variables, new_var], axis=0)
#     return df_variables


def _add_global_total(tb: Table, ds_regions: Dataset) -> Table:
    """Add global total. Table shouldn't contain regional data."""
    # Exclude regions from the sum
    country_names = ds_regions["regions"].query('region_type == "country"').name
    total = tb[tb.index.get_level_values("country").isin(country_names)]

    # Calculate global total
    global_total = total.groupby(["year"]).sum().assign(country="World").set_index("country", append=True)

    # Append to the original table
    return pr.concat([tb.reset_index(), global_total.reset_index()]).set_index(tb.index.names)


def add_global_yaws(tables_dict: dict[str, Table], ds_regions: Dataset) -> None:
    indicator_name = "Number of cases of yaws reported"
    tables_dict[indicator_name] = _add_global_total(tables_dict[indicator_name], ds_regions)


def add_global_total_leprosy(tables_dict: dict[str, Table], ds_regions: Dataset) -> None:
    indicator_name = "Number of new leprosy cases"
    tables_dict[indicator_name] = _add_global_total(tables_dict[indicator_name], ds_regions)


def add_neonatal_tetanus_cases_per_mil(tables_dict: dict[str, Table], ds_population: Dataset) -> None:
    indicator_name = "Neonatal tetanus - number of reported cases"
    tb = tables_dict[indicator_name]

    # Add population
    tb = geo.add_population_to_table(tb=tb.reset_index(), ds_population=ds_population, warn_on_missing_countries=False)

    # We don't have precise population estimates for WHO regions, drop them
    tb = tb.dropna(subset=["population"])

    tb["neonatal_tetanus__number_of_reported_cases_per_million"] = (
        tb["neonatal_tetanus__number_of_reported_cases"] / tb["population"] * 1000000
    ).round(2)

    tables_dict[indicator_name] = tb.drop(columns=["population"]).set_index(["year", "country"])

"""Load a meadow dataset and create a garden dataset."""

from math import trunc
from typing import List

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("igme", version="2023-08-16")
    # Load vintage dataset which has older data.
    ds_vintage: Dataset = paths.load_dependency("igme", version="2018")
    # Read table from meadow dataset.
    tb = ds_meadow["igme"].reset_index()
    tb_vintage = ds_vintage["igme"].reset_index()
    tb_youth = tb_vintage[tb_vintage["indicator_name"].isin(["Deaths age 5 to 14", "Mortality rate age 5 to 14"])]
    tb_youth = process_vintage_data(tb_youth)

    # Process current data.
    #
    tb = fix_sub_saharan_africa(tb)
    tb: Table = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = filter_data(tb)
    tb = round_down_year(tb)
    tb = clean_values(tb)
    tb = convert_to_percentage(tb)
    tb["source"] = "igme (current)"
    # Separate out the variables needed to calculate the under-fifteen mortality rate.
    tb_under_fifteen = tb[
        (
            tb["indicator"].isin(
                ["Under-five deaths", "Deaths age 5 to 14", "Under-five mortality rate", "Mortality rate age 5-14"]
            )
        )
        & (
            tb["unit_of_measure"].isin(
                ["Number of deaths", "Deaths per 1,000 live births", "Deaths per 1000 children aged 5"]
            )
        )
    ]

    # Combine datasets with a preference for the current data when there is a conflict.

    tb_com = combine_datasets(
        tb_a=tb_under_fifteen, tb_b=tb_youth, table_name="igme_combined", preferred_source="igme (current)"
    )

    tb_com = calculate_under_fifteen_deaths(tb_com)
    tb_com = calculate_under_fifteen_mortality_rates(tb_com)
    tb_com = tb_com.set_index(
        ["country", "year", "indicator", "sex", "wealth_quintile", "unit_of_measure"], verify_integrity=True
    ).drop(columns=["source"])

    # Calculate post neonatal deaths
    tb = add_post_neonatal_deaths(tb)
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    tb = tb.set_index(
        ["country", "year", "indicator", "sex", "wealth_quintile", "unit_of_measure"], verify_integrity=True
    ).drop(columns=["source"])
    ds_garden = create_dataset(
        dest_dir, tables=[tb, tb_com], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def convert_to_percentage(tb: Table) -> Table:
    """
    Convert the units which are given as 'per 1,000...' into percentages.
    """
    rate_conversions = {
        "Deaths per 1,000 live births": "Deaths per 100 live births",
        "Deaths per 1000 children aged 1": "Deaths per 100 children aged 1",
        "Deaths per 1000 children aged 5": "Deaths per 100 children aged 5",
        "Deaths per 1000 children aged 10": "Deaths per 100 children aged 10",
        "Deaths per 1000 children aged 15": "Deaths per 100 children aged 15",
        "Deaths per 1000 children aged 20": "Deaths per 100 children aged 20",
        "Stillbirths per 1000 births": "Stillbirths per 100 births",
    }
    # Dividing values of selected rows by 10

    selected_rows = tb["unit_of_measure"].isin(rate_conversions.keys())
    tb.loc[selected_rows, ["obs_value", "lower_bound", "upper_bound"]] = tb.loc[
        selected_rows, ["obs_value", "lower_bound", "upper_bound"]
    ].div(10)

    tb = tb.replace({"unit_of_measure": rate_conversions})

    return tb


def add_post_neonatal_deaths(tb: Table) -> Table:
    """
    Calculate the deaths for the post-neonatal age-group, 28 days - 1 year
    """
    infant_deaths = tb[(tb["indicator"] == "Infant deaths")]
    neonatal_deaths = tb[(tb["indicator"] == "Neonatal deaths")]

    tb_merge = pr.merge(
        infant_deaths,
        neonatal_deaths,
        on=["country", "year", "wealth_quintile", "sex", "unit_of_measure", "source"],
        suffixes=("_infant", "_neonatal"),
    )
    tb_merge["obs_value"] = tb_merge["obs_value_infant"] - tb_merge["obs_value_neonatal"]
    tb_merge["lower_bound"] = tb_merge["lower_bound_infant"] - tb_merge["lower_bound_neonatal"]
    tb_merge["upper_bound"] = tb_merge["upper_bound_infant"] - tb_merge["upper_bound_neonatal"]
    tb_merge["indicator"] = "Post-neonatal deaths"
    result_tb = tb_merge[
        [
            "country",
            "year",
            "indicator",
            "sex",
            "unit_of_measure",
            "wealth_quintile",
            "obs_value",
            "lower_bound",
            "upper_bound",
            "source",
        ]
    ]
    # There are some cases where the neonatal deaths are greater than the infant deaths, so we need to set these to 0, e.g. for some years in Monaco.
    # Perhaps due to the transitory nature of the population.
    result_tb[["obs_value", "lower_bound", "upper_bound"]] = result_tb[
        ["obs_value", "lower_bound", "upper_bound"]
    ].clip(lower=0)
    assert all(result_tb["obs_value"] >= 0), "Negative values in post-neonatal deaths!"
    tb = pr.concat([tb, result_tb])

    return tb


def add_metadata_and_set_index(tb: Table) -> Table:
    for col in tb.columns[2:]:
        unit = col.split("-")[1]
        tb[col].metadata.unit = unit.lower().strip()
        tb[col].metadata.title = col
        if " per " in unit:
            tb[col].metadata.display = {"numDecimalPlaces": 1}
        else:
            tb[col].metadata.display = {"numDecimalPlaces": 0}
    tb = tb.set_index(["country", "year"], verify_integrity=True)
    return tb


def process_vintage_data(tb_youth: Table) -> Table:
    # Process vintage 5-14 mortality data
    tb_youth = tb_youth.rename(
        columns={
            "indicator_name": "indicator",
            "sex_name": "sex",
            "unit_measure_name": "unit_of_measure",
            "obs_value": "Observation value",
            "lower_bound": "Lower bound",
            "upper_bound": "Upper bound",
        }
    ).drop(columns=["series_name_name"])
    tb_youth = clean_values(tb_youth)
    tb_youth["wealth_quintile"] = "All wealth quintiles"
    tb_youth["source"] = "igme (2018)"
    tb_youth["indicator"] = tb_youth["indicator"].replace({"Mortality rate age 5 to 14": "Mortality rate age 5-14"})

    return tb_youth


def calculate_under_fifteen_mortality_rates(tb: Table) -> Table:
    """
    First of all we must adjust the mortality rates, so that we can combine age groups together.

    For example, if we want to calculate the mortality rate of under-fifteens then we need to combine the under-five mortality rate and the 5-14 year old age group.

    If there are 100 deaths per 1000 under fives, then we need to adjust the denominator of the 5-14 age group to take account of this.
    """
    u5_mortality = tb[tb["indicator"] == "Under-five mortality rate"]
    mortality_5_14 = tb[tb["indicator"] == "Mortality rate age 5-14"]

    tb_merge = pr.merge(
        u5_mortality,
        mortality_5_14,
        on=["country", "year", "wealth_quintile", "sex", "source"],
        suffixes=("_u5", "_5_14"),
    )
    tb_merge["adjusted_5_14_mortality_rate"] = (100 - tb_merge["obs_value_u5"]) / 100 * tb_merge["obs_value_5_14"]
    tb_merge["obs_value"] = tb_merge["obs_value_u5"] + tb_merge["adjusted_5_14_mortality_rate"]
    tb_merge["indicator"] = "Under-fifteen mortality rate"
    tb_merge["unit_of_measure"] = "Deaths per 1,000 live births"

    result_tb = tb_merge[
        [
            "country",
            "year",
            "indicator",
            "sex",
            "unit_of_measure",
            "wealth_quintile",
            "obs_value",
            "source",
        ]
    ]
    result_tb = result_tb[result_tb["indicator"].isin(["Under-fifteen mortality rate", "Under-fifteen deaths"])]
    result_tb.metadata.short_name = "igme_under_fifteen_mortality"

    return result_tb


def combine_datasets(tb_a: Table, tb_b: Table, table_name: str, preferred_source: str) -> Table:
    """
    Combine two tables with a preference for one source of data.
    """
    tb_combined = pr.concat([tb_a, tb_b], short_name=table_name).sort_values(["country", "year", "source"])
    assert any(tb_combined["source"] == preferred_source), "Preferred source not in table!"
    tb_combined = remove_duplicates(
        tb_combined,
        preferred_source=preferred_source,
        dimensions=["country", "year", "sex", "wealth_quintile", "indicator", "unit_of_measure"],
    )

    return tb_combined


def remove_duplicates(tb: Table, preferred_source: str, dimensions: List[str]) -> Table:
    """
    Removing rows where there are overlapping years with a preference for IGME data.

    """
    assert any(tb["source"] == preferred_source)

    duplicate_rows = tb.duplicated(subset=dimensions, keep=False)

    tb_no_duplicates = tb[~duplicate_rows]

    tb_duplicates = tb[duplicate_rows]

    tb_duplicates_removed = tb_duplicates[tb_duplicates["source"] == preferred_source]

    tb = pr.concat([tb_no_duplicates, tb_duplicates_removed])

    assert len(tb[tb.duplicated(subset=dimensions, keep=False)]) == 0, "Duplicates still in table!"

    return tb


def calculate_under_fifteen_deaths(tb: Table) -> Table:
    """
    Calculate the under fifteen mortality total deaths.
    """
    tb_u5 = (
        tb[(tb["indicator"] == "Under-five deaths") & (tb["unit_of_measure"] == "Number of deaths")]
        .drop(columns="indicator")
        .rename(
            columns={
                "Observation value": "under_five_mortality",
                "Lower bound": "under_five_mortality_lb",
                "Upper bound": "under_five_mortality_ub",
            }
        )
    )
    tb_5_14 = (
        tb[(tb["indicator"] == "Deaths age 5 to 14") & (tb["unit_of_measure"] == "Number of deaths")]
        .drop(columns="indicator")
        .rename(
            columns={
                "Observation value": "five_to_fourteen_mortality",
                "Lower bound": "five_to_fourteen_mortality_lb",
                "Upper bound": "five_to_fourteen_mortality_ub",
            }
        )
    )
    tb_u15 = pr.merge(tb_u5, tb_5_14)

    for suffix in ["", "_lb", "_ub"]:
        tb_u15[f"under_fifteen_mortality{suffix}"] = (
            tb_u15[f"under_five_mortality{suffix}"] + tb_u15[f"five_to_fourteen_mortality{suffix}"]
        )

    tb_u15 = tb_u15.drop(
        columns=[
            "under_five_mortality",
            "under_five_mortality_lb",
            "under_five_mortality_ub",
            "five_to_fourteen_mortality",
            "five_to_fourteen_mortality_lb",
            "five_to_fourteen_mortality_ub",
        ]
    ).rename(
        columns={
            "under_fifteen_mortality": "Observation value",
            "under_fifteen_mortality_lb": "Lower bound",
            "under_fifteen_mortality_ub": "Upper bound",
        }
    )
    tb_u15["indicator"] = "Under-fifteen deaths"

    tb = pr.concat([tb, tb_u15])
    return tb


def filter_data(tb: Table) -> Table:
    """
    Filtering out the unnecessary columns and rows from the data.
    We just want the UN IGME estimates, rather than the individual results from the survey data.
    """
    # Keeping only the UN IGME estimates and the total wealth quintile
    tb = tb.loc[(tb["series_name"] == "UN IGME estimate")]
    tb = tb[
        -tb["indicator"].isin(
            ["Progress towards SDG in neonatal mortality rate", "Progress towards SDG in under-five mortality rate"]
        )
    ]
    cols_to_keep = [
        "country",
        "year",
        "indicator",
        "sex",
        "unit_of_measure",
        "wealth_quintile",
        "obs_value",
        "lower_bound",
        "upper_bound",
    ]
    # Keeping only the necessary columns.
    tb = tb[cols_to_keep]

    return tb


def clean_values(tb: Table) -> Table:
    """
    Adding clearer meanings to the values in the table.
    """
    sex_dict = {"Total": "Both sexes"}

    wealth_dict = {
        "Total": "All wealth quintiles",
        "Lowest": "Poorest quintile",
        "Highest": "Richest quintile",
        "Middle": "Middle wealth quintile",
        "Second": "Second poorest quintile",
        "Fourth": "Fourth poorest quintile",
    }
    if "sex" in tb.columns:
        tb["sex"] = tb["sex"].replace(sex_dict)
    if "wealth_quintile" in tb.columns:
        tb["wealth_quintile"] = tb["wealth_quintile"].replace(wealth_dict)

    return tb


def fix_sub_saharan_africa(tb: Table) -> Table:
    """
    Sub-Saharan Africa appears twice in the Table, as it is defined by two different organisations, UNICEF and SDG.
    This function clarifies this by combining the region and organisation into one.
    """
    tb["country"] = tb["country"].astype(str)

    tb.loc[
        (tb["country"] == "Sub-Saharan Africa") & (tb["regional_group"] == "UNICEF"), "country"
    ] = "Sub-Saharan Africa (UNICEF)"

    tb.loc[
        (tb["country"] == "Sub-Saharan Africa") & (tb["regional_group"] == "SDG"), "country"
    ] = "Sub-Saharan Africa (SDG)"

    return tb


def round_down_year(tb: Table) -> Table:
    """
    Round down the year value given - to match what is shown on https://childmortality.org
    """

    tb["year"] = tb["year"].apply(trunc)

    return tb

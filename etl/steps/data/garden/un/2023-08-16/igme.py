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
    tb = tb.rename(
        columns={"obs_value": "Observation value", "lower_bound": "Lower bound", "upper_bound": "Upper bound"}
    )
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

    # Pivot the table so that the variables are in columns.
    tb = pivot_table_and_format(tb)
    # Calculate post neonatal deaths
    tb = add_post_neonatal_deaths(tb)
    tb_com = pivot_table_and_format(tb_com)

    tb_com = calculate_under_fifteen_mortality_rates(tb_com)
    # Add some metadata to the variables. Getting the unit from the column name and inferring the number of decimal places from the unit.
    # If it contains " per " we know it is a rate and should have 1 d.p., otherwise it should be an integer.

    tb = add_metadata_and_set_index(tb)
    tb_com = add_metadata_and_set_index(tb_com)

    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb, tb_com], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_post_neonatal_deaths(tb: Table) -> Table:
    """
    Calculate the deaths for the post-neonatal age-group, 28 days - 1 year
    """
    tb["Observation value-Number of deaths-Post-neonatal deaths-Both sexes-All wealth quintiles"] = (
        tb["Observation value-Number of deaths-Infant deaths-Both sexes-All wealth quintiles"]
        - tb["Observation value-Number of deaths-Neonatal deaths-Both sexes-All wealth quintiles"]
    )

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


def pivot_table_and_format(tb: Table) -> Table:
    tb = tb.pivot(
        index=["country", "year"],
        values=["Observation value", "Lower bound", "Upper bound"],
        columns=["unit_of_measure", "indicator", "sex", "wealth_quintile"],
    )
    tb.columns = ["-".join(col).strip() for col in tb.columns.values]
    tb = tb.reset_index()
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
    tb["adjusted_5_14_mortality_rate"] = (
        (
            1000
            - tb[
                "Observation value-Deaths per 1,000 live births-Under-five mortality rate-Both sexes-All wealth quintiles"
            ]
        )
        / 1000
    ) * tb["Observation value-Deaths per 1000 children aged 5-Mortality rate age 5-14-Both sexes-All wealth quintiles"]

    tb[
        "Observation value-Deaths per 1,000 live births-Under-fifteen mortality rate-Both sexes-All wealth quintiles"
    ] = (
        tb["Observation value-Deaths per 1,000 live births-Under-five mortality rate-Both sexes-All wealth quintiles"]
        + tb["adjusted_5_14_mortality_rate"]
    )
    tb = tb[
        [
            "country",
            "year",
            "Observation value-Deaths per 1,000 live births-Under-fifteen mortality rate-Both sexes-All wealth quintiles",
        ]
    ]
    tb.metadata.short_name = "igme_under_fifteen_mortality_rate"
    return tb


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

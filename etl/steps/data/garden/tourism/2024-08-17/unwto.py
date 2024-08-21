"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("unwto")
    ds_population = paths.load_dataset("population")

    ds_us_cpi = paths.load_dataset("us_consumer_prices")
    ds_exchange_rates = paths.load_dataset("ppp_exchange_rates")
    ds_garden = paths.load_dataset("wdi")

    # Read table from meadow dataset and the datasets used for adjusting for inflation and cost of living
    tb = ds_meadow["unwto"].reset_index()
    tb_us_cpi = ds_us_cpi["us_consumer_prices"].reset_index()
    tb_exchange_rates = ds_exchange_rates["ppp_exchange_rates"].reset_index()
    tb_all_cpi = ds_garden["wdi"]["fp_cpi_totl"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Drop columns with all NaN values
    tb = tb.dropna(axis=1, how="all")

    # Find columns that start with the specified prefixes - units are not thousands
    prefixes_not_thousands = ["inbound_tourism_expenditure", "outbound_tourism_expenditure", "tourism_industries"]
    matching_columns = [col for col in tb.columns if any(col.startswith(prefix) for prefix in prefixes_not_thousands)]

    # Multiply the all the other columns by 1000 as these are in thousands
    for col in tb.columns:
        if col not in matching_columns + ["country", "year"]:
            tb[col] = tb[col] * 1000

    # Find expenditure columns that are in US million and convert
    prefixes_us_dollars = ["inbound_tourism_expenditure", "outbound_tourism_expenditure"]
    matching_columns = [col for col in tb.columns if any(col.startswith(prefix) for prefix in prefixes_us_dollars)]

    for col in matching_columns:
        tb[col] = tb[col] * 1e6
    # Shortern column names
    tb.columns = shorten_column_names(tb.columns)

    # Calculate the business/personal ratio column
    tb["business_personal_ratio"] = tb["in_tour_purpose_business_and_prof"] / tb["in_tour_purpose_personal"]
    # Calculate the inbound/outbound ratio ratio column
    tb["inbound_outbound_tourism"] = (
        tb["in_tour_arrivals_ovn_vis_tourists"] / tb["out_tour_departures_ovn_vis_tourists"]
    )
    # Calculate same-day by tourist trips ratio
    tb["same_day_tourist_ratio"] = tb["in_tour_arrivals_same_day_vis_excur"] / tb["in_tour_arrivals_ovn_vis_tourists"]
    # Add certain indicators per 1,000 inhabitants
    tb = geo.add_population_to_table(tb, ds_population)
    #
    # Calculate per 1,000 inhabitants for some indicators
    #
    cols_per_1000 = [
        "in_tour_arrivals_ovn_vis_tourists",
        "in_tour_arrivals_same_day_vis_excur",
        "out_tour_departures_ovn_vis_tourists",
        "dom_tour_trips_same_day_vis_excur",
        "employment_food_and_beverage_serving_act",
        "employment_total",
    ]

    for col in cols_per_1000:
        tb[f"{col}_per_1000"] = tb[col] / (tb["population"] / 1000)

    tb["dom_tour_trips_ovn_vis_tourists_per_person"] = tb[col] / tb["population"]

    tb = tb.drop(columns=["population"])

    tb[f"{col}_per_person"]
    #
    # Calculate the inbound tourism by region
    #
    region_columns = [
        "in_tour_regions_africa",
        "in_tour_regions_americas",
        "in_tour_regions_east_asia_and_the_pacific",
        "in_tour_regions_europe",
        "in_tour_regions_middle_east",
        "in_tour_regions_other_not_class",
        "in_tour_regions_south_asia",
    ]

    tb_regions_sum = tb[region_columns + ["year"]].groupby("year").sum().reset_index()

    # Rename the columns
    rename_mapping = {
        "in_tour_regions_africa": "Africa (UNWTO)",
        "in_tour_regions_americas": "Americas (UNWTO)",
        "in_tour_regions_east_asia_and_the_pacific": "East Asia and the Pacific (UNWTO)",
        "in_tour_regions_europe": "Europe (UNWTO)",
        "in_tour_regions_middle_east": "Middle East (UNWTO)",
        "in_tour_regions_other_not_class": "Other (UNWTO)",
        "in_tour_regions_south_asia": "South Asia (UNWTO)",
    }
    tb_regions_sum = tb_regions_sum.rename(columns=rename_mapping)

    # Melt the Table so that each region is a row in a country column
    tb_regions_sum = pd.melt(
        tb_regions_sum, id_vars=["year"], var_name="country", value_name="inbound_tourism_by_region"
    )
    tb = pr.concat([tb, tb_regions_sum], axis=0)

    # Add origins to the new column
    tb["inbound_tourism_by_region"].metadata.origins = tb["in_tour_regions_africa"].metadata.origins

    # Adjust inbound and outbound expenditure for inflation and cost of living
    tb = adjust_inflation_cost_of_living(tb, tb_us_cpi, tb_exchange_rates, tb_all_cpi)
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def adjust_inflation_cost_of_living(tb: Table, tb_us_cpi: Table, tb_exchange_rates: Table, tb_all_cpi: Table):
    """
    Adjusts the inbound and outbound tourism expenditure for inflation and cost of living.
    Inbound expenditure is adjusted for local inflation and purchasing power parity.
    Outbound expenditure is adjusted for U.S. inflation.

    Args:
        tb (Table): The main dataframe containing the tourism data.
        tb_us_cpi (Table): The dataframe containing the U.S. CPI data (U.S. Bureau of Labor Statistics).
        tb_exchange_rates (Table): The dataframe containing the exchange rate data (OECD).
        tb_all_cpi (Table): The dataframe containing the CPI data for all countrie (WDI).

    """
    # Calculate the U.S. CPI adjustment factor for 2021
    cpi_2021 = tb_us_cpi.loc[tb_us_cpi["year"] == 2021, "all_items"].values[0]
    tb_us_cpi["cpi_adj_2021"] = tb_us_cpi["all_items"] / cpi_2021

    # Filter the U.S. CPI data to just the 2021 values
    tb_us_cpi_2021 = tb_us_cpi[["cpi_adj_2021", "year"]].copy()

    # Merge the main Table with the 2021 U.S. CPI data
    tb_cpi_inv = pr.merge(tb, tb_us_cpi_2021, on="year", how="left")

    # Adjust the outbound expenditure for U.S. CPI
    tb_cpi_inv["outbound_exp_us_cpi_adjust"] = tb_cpi_inv["out_tour_exp_travel"] / tb_cpi_inv["cpi_adj_2021"]

    # Drop the temporary 'cpi_adj_2021' column
    tb_cpi_inv = tb_cpi_inv.drop("cpi_adj_2021", axis=1)

    # Merge the Tables for expenditure, inflation across the world, and exchange rates
    tb = pr.merge(tb_cpi_inv, tb_all_cpi, on=["country", "year"], how="left")
    tb = pr.merge(tb, tb_exchange_rates, on=["country", "year"], how="left")

    # Filter the Table for the year 2021
    tb_2021 = tb[tb["year"] == 2021]

    # Merge the 2021 values with the original Table
    tb = pr.merge(
        tb,
        tb_2021[["country", "fp_cpi_totl", "purchasing_power_parities_for_household_final_consumption_expenditure"]],
        on="country",
        suffixes=("", "_2021"),
    )

    # Normalize the CPI to 2021 values
    tb["fp_cpi_totl_normalized"] = 100 * tb["fp_cpi_totl"] / tb["fp_cpi_totl_2021"]

    # Adjust the inbound expenditure for local inflation and purchasing power parity
    tb["inbound_ppp_cpi_adj_2021"] = (
        100 * (tb["in_tour_exp_travel"] * tb["exchange_rates__average"]) / tb["fp_cpi_totl_normalized"]
    ) / tb["purchasing_power_parities_for_household_final_consumption_expenditure_2021"]

    # Drop unnecessary columns
    tb = tb.drop(
        [
            "fp_cpi_totl",
            "exchange_rates__average",
            "exchange_rates__end_of_period",
            "purchasing_power_parities_for_gdp",
            "purchasing_power_parities_for_actual_individual_consumption",
            "purchasing_power_parities_for_household_final_consumption_expenditure",
            "fp_cpi_totl_2021",
            "purchasing_power_parities_for_household_final_consumption_expenditure_2021",
            "fp_cpi_totl_normalized",
        ],
        axis=1,
    )

    return tb


def shorten_column_names(columns):
    # Define common abbreviations and replacements
    abbr = {
        "inbound": "in",
        "outbound": "out",
        "tourism": "tour",
        "domestic": "dom",
        "accommodation": "accom",
        "establishments": "estab",
        "visitors": "vis",
        "passengers": "pass",
        "excursionists": "excur",
        "overnights": "ovn",
        "expenditure": "exp",
        "industries": "ind",
        "transportation": "trans",
        "activities": "act",
        "professional": "prof",
        "nationals": "nat",
        "residing": "res",
        "classified": "class",
        "available": "avail",
        "capacity": "cap",
        "average": "avg",
        "number": "num",
        "occupancy": "occ",
    }

    short_columns = []

    for col in columns:
        # Split the column name into parts
        parts = col.split("__")

        # Shorten each part
        short_parts = []
        for part in parts:
            words = part.split("_")
            short_words = [abbr.get(word, word[:10]) for word in words]
            short_parts.append("_".join(short_words))

        # Join the shortened parts
        short_col = "_".join(short_parts)

        # Remove duplicate words
        short_col = "_".join(dict.fromkeys(short_col.split("_")))

        short_columns.append(short_col)

    return short_columns

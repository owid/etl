"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Prefixes for columns that should not be multiplied by 1000
PREFIXES_NOT_THOUSANDS = [
    "inbound_tourism_expenditure",
    "outbound_tourism_expenditure",
    "employment_employed_persons",
    "tourism_industries",
    "tour_ind_environmen_implementa",
]

# Common abbreviations for shortening column names
ABBREVIATIONS = {
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

# Columns to keep after shortening column names
COLUMNS_TO_KEEP = [
    "dom_tour_trips_total_overnight_vis_tourists",
    "dom_tour_accom_short_term_hotels_and_similar_isic_5510_guests",
    "employment_employed_persons_in_the_tour_ind_num",
    "in_tour_accom_short_term_hotels_and_similar_isic_5510_guests",
    "in_tour_arrivals_trips_total_overnight_vis_tourists",
    "in_tour_arrivals_trips_total_same_day_vis_excur",
    "in_tour_exp_balance_of_payments_passenger_transport_vis",
    "in_tour_exp_balance_of_payments_total_vis",
    "in_tour_exp_balance_of_payments_travel_vis",
    "in_tour_purpose_trips_by_business_overnight_vis_tourists",
    "in_tour_purpose_trips_by_personal_overnight_vis_tourists",
    "in_tour_purpose_trips_by_total_overnight_vis_tourists",
    "in_tour_regions_trips_region_overnight_vis_tourists_africa_unwto_total",
    "in_tour_regions_trips_region_overnight_vis_tourists_americas_unwto_total",
    "in_tour_regions_trips_region_overnight_vis_tourists_east_asia_and_the_pacific_unwto_total",
    "in_tour_regions_trips_region_overnight_vis_tourists_europe_unwto_total",
    "in_tour_regions_trips_region_overnight_vis_tourists_middle_east_unwto_total",
    "in_tour_regions_trips_region_overnight_vis_tourists_south_asia_unwto_total",
    "out_tour_departures_trips_total_overnight_vis_tourists",
    "out_tour_exp_balance_of_payments_passenger_transport_vis",
    "out_tour_exp_balance_of_payments_total_vis",
    "out_tour_exp_balance_of_payments_travel_vis",
    "tour_ind_environmen_implementa_of_standard_accounting_tools_to_monitor_the_economic_and_aspects_num_tables",
    "tour_ind_gdp_direct_as_a_proportion_of_total_pct",
    "tour_ind_cap_accom_short_term_hotels_and_similar_isic_5510_length_of_stay_avg",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("unwto")
    ds_wdi = paths.load_dataset("wdi")

    # Read table from meadow dataset
    tb = ds_meadow.read("unwto")
    tb_wdi = ds_wdi.read("wdi")

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb)
    # Drop columns with all NaN values
    tb = tb.dropna(axis=1, how="all")

    # Find columns that start with the specified prefixes - units are not thousands
    matching_columns = [col for col in tb.columns if any(col.startswith(prefix) for prefix in PREFIXES_NOT_THOUSANDS)]
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

    # Check that all expected columns are present
    missing_columns = [col for col in COLUMNS_TO_KEEP if col not in tb.columns]
    assert not missing_columns, f"Missing expected columns: {missing_columns}"

    # Select only the columns we want to keep (plus country and year)
    columns_to_select = ["country", "year"] + COLUMNS_TO_KEEP
    tb = tb[columns_to_select]

    # Calculate the business/personal ratio column
    tb["business_personal_ratio"] = (
        tb["in_tour_purpose_trips_by_business_overnight_vis_tourists"]
        / tb["in_tour_purpose_trips_by_personal_overnight_vis_tourists"]
    )
    # Calculate the inbound/outbound ratio ratio column
    tb["inbound_outbound_tourism"] = (
        tb["in_tour_arrivals_trips_total_overnight_vis_tourists"]
        / tb["out_tour_departures_trips_total_overnight_vis_tourists"]
    )
    # Calculate same-day by tourist trips ratio
    tb["same_day_tourist_ratio"] = (
        tb["in_tour_arrivals_trips_total_same_day_vis_excur"]
        / tb["in_tour_arrivals_trips_total_overnight_vis_tourists"]
    )
    # Add certain indicators per 1,000 inhabitants
    tb = paths.regions.add_population(tb)
    #
    # Calculate per 1,000 inhabitants for some indicators
    #
    cols_per_1000 = [
        "in_tour_arrivals_trips_total_overnight_vis_tourists",
        "in_tour_arrivals_trips_total_same_day_vis_excur",
        "out_tour_departures_trips_total_overnight_vis_tourists",
        "dom_tour_trips_total_same_day_vis_excur",
        "employment_employed_persons_in_the_tour_ind_num",
    ]

    for col in cols_per_1000:
        if col in tb.columns:
            tb[f"{col}_per_1000"] = tb[col] / (tb["population"] / 1000)

    if "dom_tour_trips_total_overnight_vis_tourists" in tb.columns:
        tb["dom_tour_trips_total_overnight_vis_tourists_per_person"] = (
            tb["dom_tour_trips_total_overnight_vis_tourists"] / tb["population"]
        )

    tb = tb.drop(columns=["population"])

    #
    # Calculate the inbound tourism by region
    #
    # Define region columns mapping
    region_columns_mapping = {
        "in_tour_regions_trips_region_overnight_vis_tourists_africa_unwto_total": "Africa (UNWTO)",
        "in_tour_regions_trips_region_overnight_vis_tourists_americas_unwto_total": "Americas (UNWTO)",
        "in_tour_regions_trips_region_overnight_vis_tourists_east_asia_and_the_pacific_unwto_total": "East Asia and the Pacific (UNWTO)",
        "in_tour_regions_trips_region_overnight_vis_tourists_europe_unwto_total": "Europe (UNWTO)",
        "in_tour_regions_trips_region_overnight_vis_tourists_middle_east_unwto_total": "Middle East (UNWTO)",
        "in_tour_regions_trips_region_overnight_vis_tourists_other_not_class_unwto_total": "Other (UNWTO)",
        "in_tour_regions_trips_region_overnight_vis_tourists_south_asia_unwto_total": "South Asia (UNWTO)",
    }

    # Filter to only existing columns
    existing_region_columns = [col for col in region_columns_mapping.keys() if col in tb.columns]

    if existing_region_columns:
        tb_regions_sum = tb[existing_region_columns + ["year"]].groupby("year").sum().reset_index()

        # Rename the columns
        tb_regions_sum = tb_regions_sum.rename(columns=region_columns_mapping)

        # Melt the Table so that each region is a row in a country column
        tb_regions_sum = pr.melt(
            tb_regions_sum, id_vars=["year"], var_name="country", value_name="inbound_tourism_by_region"
        )

        tb = pr.concat([tb, tb_regions_sum], axis=0)

    #
    # Rename environment and GDP variables to shorter names
    # These have already been processed by shorten_column_names function
    #
    rename_dict = {
        "tour_ind_environmen_implementa_of_standard_accounting_tools_to_monitor_the_economic_and_aspects_num_tables": "total_tables",
        "tour_ind_gdp_direct_as_a_proportion_of_total_pct": "tourism_share_gdp",
    }
    tb = tb.rename(columns=rename_dict, errors="raise")

    # Adjust inbound and outbound expenditure for inflation and cost of living
    tb = adjust_inflation_cost_of_living(tb, tb_wdi)

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def shorten_column_names(columns):
    """
    Shorten long column names using common abbreviations.

    This function takes column names from the UNWTO dataset and abbreviates common tourism terms
    to make them more manageable while maintaining readability. For example:
    - "inbound_tourism_accommodation" becomes "in_tour_accom"
    - "outbound_tourism_expenditure" becomes "out_tour_exp"

    Args:
        columns: List or iterable of column names to shorten

    Returns:
        List of shortened column names with duplicate words removed
    """
    short_columns = []

    for col in columns:
        # Split the column name into parts
        parts = col.split("__")

        # Shorten each part
        short_parts = []
        for part in parts:
            words = part.split("_")
            short_words = [ABBREVIATIONS.get(word, word[:10]) for word in words]
            short_parts.append("_".join(short_words))

        # Join the shortened parts
        short_col = "_".join(short_parts)

        # Remove duplicate words
        short_col = "_".join(dict.fromkeys(short_col.split("_")))

        short_columns.append(short_col)

    return short_columns


def adjust_inflation_cost_of_living(tb: Table, tb_wdi: Table) -> Table:
    """
    Adjusts the inbound and outbound tourism expenditure for inflation and cost of living.

    - Inbound expenditure is adjusted for local inflation (CPI) and purchasing power parity (PPP).
    - Outbound expenditure is adjusted for U.S. inflation (CPI).

    All adjustments are normalized to 2021 values.

    Args:
        tb (Table): The main dataframe containing the tourism data.
        tb_wdi (Table): The World Bank WDI dataframe containing CPI, exchange rates, and PPP data.

    Returns:
        Table: The input table with two new columns:
            - inbound_exp_ppp_cpi_adj_2021: Inbound expenditure adjusted for local inflation and PPP
            - outbound_exp_us_cpi_adj_2021: Outbound expenditure adjusted for U.S. inflation
    """
    # Reset index on WDI data to access country and year columns
    tb_wdi = tb_wdi.reset_index()

    # Extract the three indicators we need from WDI
    # pa_nus_atls: Official exchange rate (LCU per US$, period average)
    # pa_nus_prvt_pp: PPP conversion factor, private consumption (LCU per international $)
    # fp_cpi_totl: Consumer price index (2010 = 100)
    tb_wdi_filtered = tb_wdi[["country", "year", "pa_nus_atls", "pa_nus_prvt_pp", "fp_cpi_totl"]].rename(
        columns={
            "pa_nus_atls": "exchange_rate",
            "pa_nus_prvt_pp": "ppp_conversion_factor",
            "fp_cpi_totl": "cpi",
        }
    )

    # Get U.S. CPI data for outbound adjustment
    tb_us_cpi = tb_wdi_filtered[tb_wdi_filtered["country"] == "United States"][["year", "cpi"]].copy()

    # Calculate the U.S. CPI adjustment factor for 2021
    cpi_2021_us = tb_us_cpi.loc[tb_us_cpi["year"] == 2021, "cpi"].values
    if len(cpi_2021_us) == 0:
        raise ValueError("No U.S. CPI data found for year 2021")
    cpi_2021_us = cpi_2021_us[0]

    tb_us_cpi["cpi_adj_2021"] = tb_us_cpi["cpi"] / cpi_2021_us
    tb_us_cpi_adj = tb_us_cpi[["year", "cpi_adj_2021"]].copy()

    # Merge U.S. CPI adjustment with main table for outbound expenditure adjustment
    tb = pr.merge(tb, tb_us_cpi_adj, on="year", how="left")

    # Adjust outbound expenditure for U.S. CPI
    if "out_tour_exp_balance_of_payments_total_vis" in tb.columns:
        tb["outbound_exp_us_cpi_adj_2021"] = tb["out_tour_exp_balance_of_payments_total_vis"] / tb["cpi_adj_2021"]

    # Drop the temporary U.S. CPI column
    tb = tb.drop(columns=["cpi_adj_2021"], errors="ignore")

    # Merge exchange rates, PPP, and local CPI with main table
    tb = pr.merge(tb, tb_wdi_filtered, on=["country", "year"], how="left")

    # Get 2021 reference values for each country
    tb_2021 = tb[tb["year"] == 2021][["country", "cpi", "ppp_conversion_factor"]].copy()
    tb_2021 = tb_2021.rename(columns={"cpi": "cpi_2021", "ppp_conversion_factor": "ppp_2021"})

    # Merge 2021 reference values
    tb = pr.merge(tb, tb_2021, on="country", how="left")

    # Normalize CPI to 2021 = 1
    tb["cpi_normalized_2021"] = tb["cpi"] / tb["cpi_2021"]

    # Adjust inbound expenditure for local inflation and PPP
    # Formula: (expenditure_in_usd * exchange_rate / cpi_normalized) / ppp_2021
    # This converts to local currency, adjusts for inflation, and normalizes by PPP
    if "in_tour_exp_balance_of_payments_total_vis" in tb.columns:
        tb["inbound_exp_ppp_cpi_adj_2021"] = (
            (tb["in_tour_exp_balance_of_payments_total_vis"] * tb["exchange_rate"]) / tb["cpi_normalized_2021"]
        ) / tb["ppp_2021"]

    # Clean up temporary columns
    tb = tb.drop(
        columns=[
            "exchange_rate",
            "ppp_conversion_factor",
            "cpi",
            "cpi_2021",
            "ppp_2021",
            "cpi_normalized_2021",
        ],
        errors="raise",
    )

    return tb

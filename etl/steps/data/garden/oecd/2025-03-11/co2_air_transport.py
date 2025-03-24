"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Define regions to aggregate
REGIONS = ["Europe", "Asia", "North America", "South America", "Africa", "Oceania"]

# Define fraction of allowed NaNs per year
FRAC_ALLOWED_NANS_PER_YEAR = 0.2


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("co2_air_transport")
    ds_tourism = paths.load_dataset("unwto")
    ds_population = paths.load_dataset("population")
    ds_regions = paths.load_dataset("regions")

    # Read table from meadow dataset.
    tb = ds_meadow["co2_air_transport"].reset_index()
    tb_tourism = ds_tourism["unwto"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = tb[tb["flight_type"] == "Passenger flights"]
    tb = tb.drop("flight_type", axis=1)

    tb = tb[tb["emissions_source"].isin(["TER_DOM", "TER_INT"])]

    tb_annual = process_annual_data(tb)
    tb_annual = geo.add_population_to_table(tb_annual, ds_population)

    emissions_columns = [col for col in tb_annual.columns if col not in ("country", "year", "population")]

    # Generate per capital co2 emissions data and add it do the dataframe and convert to kg
    for col in emissions_columns:
        tb_annual[f"per_capita_{col}"] = (tb_annual[col] * 1000) / tb_annual["population"]

    tb_annual = add_inbound_outbound_tour(tb_annual, tb_tourism)

    tb_monthly = process_monthly_data(tb)

    # Generate per capital co2 emissions data and add it do the dataframe and convert to kg
    for col in ["TER_DOM_m", "TER_INT_m"]:
        tb_monthly[f"per_capita_{col}"] = (tb_monthly[col] * 1000) / tb_annual["population"]

    tb = pr.merge(tb_annual, tb_monthly, on=["year", "country"], how="outer")
    tb = tb[tb["year"] != 2025]
    tb = tb.drop(["population"], axis=1)
    tb["total_monthly_emissions"] = tb["TER_INT_m"] + tb["TER_DOM_m"]

    tb = geo.add_regions_to_table(tb=tb, ds_regions=ds_regions, regions=REGIONS, frac_allowed_nans_per_year=0.9)
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


def process_annual_data(tb):
    tb = tb[tb["frequency_of_observation"] == "Annual"]

    tb = tb.drop(["frequency_of_observation", "month"], axis=1)

    tb["emissions_source"] = tb["emissions_source"].apply(lambda x: x + "_a")

    tb = tb.pivot(values="value", index=["country", "year"], columns=["emissions_source"])
    tb = tb.reset_index()
    tb["total_annual_emissions"] = tb["TER_INT_a"] + tb["TER_DOM_a"]
    # Filter data for 2019 and 2024
    emissions_2019 = tb[tb["year"] == 2019][["country", "total_annual_emissions"]].rename(
        columns={"total_annual_emissions": "emissions_2019"}
    )
    emissions_2024 = tb[tb["year"] == 2024][["country", "total_annual_emissions"]].rename(
        columns={"total_annual_emissions": "emissions_2024"}
    )

    # Merge the two datasets on 'country' to align emissions for 2019 and 2024
    emissions_comparison = pd.merge(emissions_2019, emissions_2024, on="country", how="inner")

    # Calculate the difference in emissions
    emissions_comparison["emissions_difference"] = (
        emissions_comparison["emissions_2024"] - emissions_comparison["emissions_2019"]
    )
    emissions_comparison["year"] = 2024

    # Add the difference back to the original table for the year 2024
    tb = pr.merge(
        tb, emissions_comparison[["country", "year", "emissions_difference"]], on=["country", "year"], how="left"
    )

    return tb


def process_monthly_data(tb):
    tb = tb[tb["frequency_of_observation"] == "Monthly"]
    tb = tb.drop(["frequency_of_observation"], axis=1)

    # Remove rows with NaN values in 'year' or 'month'
    tb = tb.dropna(subset=["month"])

    # Create a new 'date' column separately
    date_column = pd.to_datetime(tb["year"].astype(str) + "-" + tb["month"].astype(str) + "-15", format="mixed")
    tb["date"] = date_column
    tb["emissions_source"] = tb["emissions_source"].apply(lambda x: x + "_m")

    # Calculate the number of days since 2019
    tb["days_since_2019"] = (tb["date"] - pd.to_datetime("2019-01-01")).dt.days

    # Pivot the table for monthly data
    tb_monthly = tb.pivot(values="value", index=["country", "days_since_2019"], columns=["emissions_source"])
    tb_monthly = tb_monthly.reset_index()
    tb_monthly = tb_monthly.rename(columns={"days_since_2019": "year"})

    # Add the total monthly emissions with a year as a column
    tb_by_month = tb.copy()

    tb_by_month = tb_by_month.pivot(
        values="value", index=["country", "month", "year"], columns=["emissions_source"]
    ).reset_index()

    # Calculate the total monthly emissions
    tb_by_month["emissions_by_month"] = tb_by_month["TER_DOM_m"] + tb_by_month["TER_INT_m"]
    tb_by_month = tb_by_month.pivot(values="emissions_by_month", index=["country", "month"], columns=["year"])
    tb_by_month = tb_by_month.rename(columns=lambda col: str(col) + "_total_emissions")

    tb_by_month = tb_by_month.reset_index()
    tb_by_month = tb_by_month.rename(columns={"month": "year"})

    tb = pr.merge(tb_monthly, tb_by_month, on=["year", "country"], how="outer")

    return tb


def add_inbound_outbound_tour(tb, tb_tourism):
    just_inb_ratio = tb_tourism[["country", "year", "inbound_outbound_tourism"]]
    tb = pr.merge(tb, just_inb_ratio, on=["year", "country"], how="left")

    # Calculate the interaction between TER_INT_a and inb_outb_tour
    tb["int_inb_out_per_capita"] = tb["per_capita_TER_INT_a"] / tb["inbound_outbound_tourism"]
    tb["int_inb_out_tot"] = tb["TER_INT_a"] * tb["inbound_outbound_tourism"]

    # Drop the 'inb_outb_tour' column
    tb = tb.drop(["inbound_outbound_tourism"], axis=1)

    return tb

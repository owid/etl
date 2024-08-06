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
    ds_meadow = paths.load_dataset("refugee_data")
    ds_population = paths.load_dataset("population")
    ds_resettlement = paths.load_dataset("resettlement")

    # Read table from meadow dataset.
    tb = ds_meadow.read_table("refugee_data")
    tb_resettlement = ds_resettlement.read_table("resettlement")

    # filter out data before data availability starts (s. https://www.unhcr.org/refugee-statistics/methodology/, "Data publication timeline")
    tb["asylum_seekers"] = tb.apply(lambda x: x["asylum_seekers"] if x["year"] > 1999 else pd.NA, axis=1)
    tb["idps_of_concern_to_unhcr"] = tb.apply(
        lambda x: x["idps_of_concern_to_unhcr"] if x["year"] > 1992 else pd.NA, axis=1
    )
    tb["stateless_persons"] = tb.apply(lambda x: x["stateless_persons"] if x["year"] > 2003 else pd.NA, axis=1)
    tb["others_of_concern"] = tb.apply(lambda x: x["others_of_concern"] if x["year"] > 2017 else pd.NA, axis=1)

    # group table by country_of_origin and year
    tb_origin = (
        tb.drop(columns=["country_of_asylum"]).groupby(["country_of_origin", "year"], observed=True).sum().reset_index()
    )
    tb_asylum = (
        tb.drop(columns=["country_of_origin"]).groupby(["country_of_asylum", "year"], observed=True).sum().reset_index()
    )

    # harmonize countries
    tb_origin = geo.harmonize_countries(
        df=tb_origin,
        country_col="country_of_origin",
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
    )
    tb_asylum = geo.harmonize_countries(
        df=tb_asylum,
        country_col="country_of_asylum",
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
    )

    # merge tables
    tb = pr.merge(
        tb_origin,
        tb_asylum,
        left_on=["country_of_origin", "year"],
        right_on=["country_of_asylum", "year"],
        how="outer",
        suffixes=("_origin", "_asylum"),
    )

    # merge country column (data is split between origin and asylum in columns)
    tb["country_of_origin"] = tb["country_of_origin"].fillna(tb["country_of_asylum"])

    # drop country of asylum column
    tb = tb.rename(columns={"country_of_origin": "country"}).drop(columns=["country_of_asylum"])

    # drop "idps_of_concern_to_unhcr_asylum" since it is identical to "idps_of_concern_to_unhcr_origin"
    tb = tb.drop(columns=["idps_of_concern_to_unhcr_asylum"])

    # merge resettlement data
    tb = pr.merge(
        tb,
        tb_resettlement,
        left_on=["country", "year"],
        right_on=["country", "year"],
        how="outer",
    )

    # calculate shares per 1000/ 100,000 population
    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population)

    tb["refugees_per_1000_pop_origin"] = tb["refugees_under_unhcrs_mandate_origin"] / tb["population"] * 1000
    tb["refugees_per_1000_pop_asylum"] = tb["refugees_under_unhcrs_mandate_asylum"] / tb["population"] * 1000

    tb["asylum_seekers_per_100k_pop_origin"] = tb["asylum_seekers_origin"] / tb["population"] * 100_000
    tb["asylum_seekers_per_100k_pop_asylum"] = tb["asylum_seekers_asylum"] / tb["population"] * 100_000

    tb["resettlement_per_100k_origin"] = tb["resettlement_arrivals_origin"] / tb["population"] * 100_000
    tb["resettlement_per_100k_dest"] = tb["resettlement_arrivals_dest"] / tb["population"] * 100_000

    # calculate five-year moving averages
    tb["refugees_origin_5y_avg"] = five_year_moving_window(tb, "refugees_under_unhcrs_mandate_origin")
    tb["refugees_origin_5y_avg_per_1000_pop"] = tb["refugees_origin_5y_avg"] / tb["population"] * 1000

    tb["refugees_asylum_5y_avg"] = five_year_moving_window(tb, "refugees_under_unhcrs_mandate_asylum")
    tb["refugees_asylum_5y_avg_per_1000_pop"] = tb["refugees_asylum_5y_avg"] / tb["population"] * 1000

    tb["asylum_seekers_origin_5y_avg"] = five_year_moving_window(tb, "asylum_seekers_origin")
    tb["asylum_seekers_origin_5y_avg_per_100k_pop"] = tb["asylum_seekers_origin_5y_avg"] / tb["population"] * 100_000

    tb["asylum_seekers_asylum_5y_avg"] = five_year_moving_window(tb, "asylum_seekers_asylum")
    tb["asylum_seekers_asylum_5y_avg_per_100k_pop"] = tb["asylum_seekers_asylum_5y_avg"] / tb["population"] * 100_000

    tb["resettlement_origin_5y_avg"] = five_year_moving_window(tb, "resettlement_arrivals_origin")
    tb["resettlement_origin_5y_avg_per_100k_pop"] = tb["resettlement_origin_5y_avg"] / tb["population"] * 100_000

    tb["resettlement_dest_5y_avg"] = five_year_moving_window(tb, "resettlement_arrivals_dest")
    tb["resettlement_dest_5y_avg_per_100k_pop"] = tb["resettlement_dest_5y_avg"] / tb["population"] * 100_000

    # drop population column
    tb = tb.drop(columns=["population"])

    # format table
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


def five_year_moving_window(tb: Table, var_name: str) -> Table:
    """Calculate a five-year moving average for a variable.
    tb: Table with all variables
    var_name: Name of the column with the variable for which the moving average should be calculated"""

    countries = tb["country"].unique()
    years = tb["year"].unique()
    country_year_combos = Table(pd.DataFrame([(x, y) for x in countries for y in years], columns=["country", "year"]))

    # create table with all years in it
    tb_all_years = pr.merge(country_year_combos, tb[["country", "year", var_name]], on=["country", "year"], how="left")
    tb_all_years = tb_all_years.sort_values(["country", "year"])
    tbs_to_combine = []
    # calculate 5-year moving average for each country
    for country in countries:
        tb_country = tb_all_years[tb_all_years["country"] == country].copy()
        tb_country[var_name + "_5y_avg"] = tb_country[var_name].rolling(5, min_periods=1).mean()
        tbs_to_combine.append(tb_country)
    # to get it back in the same order as it was in the beginning
    tb_combined = pr.concat(tbs_to_combine, ignore_index=True)
    tb = pr.merge(tb, tb_combined[["country", "year", var_name + "_5y_avg"]], on=["country", "year"], how="left")
    return tb[var_name + "_5y_avg"]

"""Combine data from Nemet (2009), Farmer & Lafond (2016) and IRENA on photovoltaic cost and capacity.

Data content:
* Nemet (2009) provides cumulative capacity data between 1975 and 2003.
* Nemet (2009) provides cost data between 1975 and 2003.
* IRENA provides cumulative capacity data from 2000 onwards.
* IRENA provides cost data from 2010 onwards.
* Farmer & Lafond (2016) provide cost data between 1980 and 2013.

For each informed year, we need to combine these sources with the following two constraints:
* Having data from the most recent source.
* Avoid (as much as possible) having cost and capacity data on a given year from different sources.

Therefore, for capacity data, we use Nemet (2009) between 1975 and 2003, and IRENA from 2004 onwards.
For cost data, we use Nemet (2009) between 1975 and 2003, Farmer & Lafond (2016) between 2004 and 2009, and IRENA
from 2010 onwards.

"""

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)

# IRENA costs are given in the latest year's USD, so we convert other costs to the same currency.
LATEST_YEAR = 2025


def get_usd_conversion_factor(from_year: int) -> float:
    """Get the factor to convert constant US$ of a given year into constant US$ of LATEST_YEAR, using the US GDP
    deflator (linked series).

    NOTE: The factor is returned as a plain float so that the deflator's metadata (from WDI, which is used as an
    auxiliary dataset) does not propagate into the indicators.
    """
    ds_deflator = paths.load_dataset("owid_deflator")
    tb_deflator = ds_deflator.read("owid_deflator")
    us = tb_deflator[tb_deflator["country"] == "United States"].set_index("year")["gdp_deflator_linked"]
    factor = float(us.loc[LATEST_YEAR] / us.loc[from_year])
    assert 1.0 <= factor < 2.0, f"Unexpected US deflator factor from {from_year} to {LATEST_YEAR}."

    return factor


def prepare_capacity_data(tb_nemet: Table, tb_irena_capacity: Table) -> Table:
    # Column "previous_capacity" is equivalent to tb_nemet["yearly_capacity"].shift(1).cumsum()
    # As they explain in the paper, "Following Epple et al. (1991), cumulative capacity is lagged one year to account
    # for the time it takes to incorporate new techniques obtained as a result of learning from experience."
    tb_nemet_capacity = tb_nemet[["year", "cost", "previous_capacity"]].rename(
        columns={"previous_capacity": "cumulative_capacity"}, errors="raise"
    )[["year", "cumulative_capacity"]]
    # Add column of origin of the data.
    tb_nemet_capacity["cumulative_capacity_source"] = "Nemet (2009)"

    # Select solar PV cumulative capacity from IRENA's dataset.
    tb_irena_capacity = (
        tb_irena_capacity[tb_irena_capacity["country"] == "World"][["year", "solar_photovoltaic"]]
        .rename(columns={"solar_photovoltaic": "cumulative_capacity"}, errors="raise")
        .reset_index(drop=True)
    )
    tb_irena_capacity["cumulative_capacity_source"] = "IRENA"

    # Combine cumulative capacity from Nemet (2009) and IRENA, prioritizing the former on overlapping years.
    cumulative_capacity = (
        combine_two_overlapping_dataframes(df1=tb_nemet_capacity, df2=tb_irena_capacity, index_columns=["year"])
        .astype({"year": int})
        .sort_values("year")
        .reset_index(drop=True)
    )

    # Improve metadata.
    cumulative_capacity[
        "cumulative_capacity"
    ].metadata.description_processing = "Photovoltaic capacity data between 1975 and 2003 has been taken from Nemet (2009). Data since 2004 has been taken from IRENA."

    # Since sources column has been manually created, it does not have metadata. Copy origins from another column.
    cumulative_capacity["cumulative_capacity_source"].metadata.origins = cumulative_capacity[
        "cumulative_capacity"
    ].metadata.origins.copy()

    return cumulative_capacity


def prepare_cost_data(tb_nemet: Table, tb_irena_cost: Table, tb_farmer_lafond: Table) -> Table:
    tb_nemet = tb_nemet.copy()
    tb_irena_cost = tb_irena_cost.copy()
    tb_farmer_lafond = tb_farmer_lafond.copy()

    # Prepare solar photovoltaic cost data from Nemet (2009).
    tb_nemet_cost = tb_nemet[["year", "cost"]].copy()
    tb_nemet_cost["cost_source"] = "Nemet (2009)"
    # Costs are given in "2004 USD/watt", so we need to convert them to the latest year USD.
    tb_nemet_cost["cost"] *= get_usd_conversion_factor(from_year=2004)
    tb_nemet_cost["cost"].metadata.unit = f"constant {LATEST_YEAR} US$ per watt"

    # Prepare solar photovoltaic cost data from Farmer & Lafond (2016).
    tb_farmer_lafond = (
        tb_farmer_lafond[["year", "photovoltaics"]]
        .dropna()
        .reset_index(drop=True)
        .rename(columns={"photovoltaics": "cost"}, errors="raise")
    )
    tb_farmer_lafond["cost_source"] = "Farmer & Lafond (2016)"
    # Costs are given in "2013 USD/Wp", so we need to convert them to the latest year USD.
    tb_farmer_lafond["cost"] *= get_usd_conversion_factor(from_year=2013)
    tb_farmer_lafond["cost"].metadata.unit = f"constant {LATEST_YEAR} US$ per watt"

    # Prepare solar photovoltaic cost data from IRENA.
    tb_irena_cost = tb_irena_cost.drop(columns="country", errors="raise")

    tb_irena_cost["cost_source"] = "IRENA"
    # Costs are given in latest year "USD/W", so we do not need to correct them.
    # NOTE: The series ends in 2024 (IRENA discontinued the global module price index after the Renewable Power
    #  Generation Costs in 2024 report), but its values are expressed in constant US$ of LATEST_YEAR.
    error = "IRENA data has changed, prices may need to be deflated to the latest year."
    assert tb_irena_cost["cost"].metadata.unit == f"constant {LATEST_YEAR} US$ per watt", error

    # Combine Nemet (2009) and Farmer & Lafond (2016), prioritizing the former.
    combined = combine_two_overlapping_dataframes(df1=tb_nemet_cost, df2=tb_farmer_lafond, index_columns="year")

    # Combine the previous with IRENA, prioritizing the latter.
    combined = combine_two_overlapping_dataframes(df1=tb_irena_cost, df2=combined, index_columns="year")

    # Improve metadata.
    # Since sources column has been manually created, it does not have metadata. Copy origins from another column.
    combined["cost_source"].metadata.origins = combined["cost"].metadata.origins.copy()

    return combined


def run() -> None:
    #
    # Load data.
    #
    # Load Nemet (2009) dataset from garden and read its main table.
    ds_nemet = paths.load_dataset("nemet_2009")
    tb_nemet = ds_nemet.read("nemet_2009")

    # Load Farmer & Lafond (2016) dataset from garden and read its main table.
    ds_farmer_lafond = paths.load_dataset("farmer_lafond_2016")
    tb_farmer_lafond = ds_farmer_lafond.read("farmer_lafond_2016")

    # Load IRENA dataset on capacity from garden and read its main table.
    ds_irena_capacity = paths.load_dataset("renewable_energy_statistics")
    tb_irena_capacity = ds_irena_capacity.read("renewable_energy_statistics")

    # Load IRENA dataset on cost from garden and read its main table.
    ds_irena_cost = paths.load_dataset("renewable_power_generation_costs")
    tb_irena_cost = ds_irena_cost.read("solar_photovoltaic_module_prices")

    #
    # Process data.
    #
    # Create a table of cumulative solar photovoltaic capacity, by combining Nemet (2009) and IRENA data.
    cumulative_capacity = prepare_capacity_data(tb_nemet=tb_nemet, tb_irena_capacity=tb_irena_capacity)

    # Create a table of solar photovoltaic cost, by combining Nemet (2009), Farmer & Lafond (2016) and IRENA data.
    cost = prepare_cost_data(tb_nemet=tb_nemet, tb_irena_cost=tb_irena_cost, tb_farmer_lafond=tb_farmer_lafond)

    # Combine capacity and cost data.
    tb_combined = pr.merge(cost, cumulative_capacity, on="year", how="outer")

    # Add column for region.
    tb_combined = tb_combined.assign(**{"country": "World"})

    # Format table conveniently.
    tb_combined = tb_combined.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = paths.create_dataset(tables=[tb_combined], yaml_params={"LATEST_YEAR": LATEST_YEAR})
    ds_garden.save()

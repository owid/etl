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

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)

# Conversion factors.
# IRENA costs are given in the latest year's USD, so we convert other costs to the same currency.
LATEST_YEAR = 2022
# Convert 2004 USD and 2013 USD to LATEST_YEAR USD , using
# https://www.usinflationcalculator.com/
USD2004_TO_USDLATEST = 1.55
USD2013_TO_USDLATEST = 1.26


def prepare_capacity_data(tb_nemet: Table, tb_irena_capacity: Table) -> Table:
    # Column "previous_capacity" is equivalent to tb_nemet["yearly_capacity"].shift(1).cumsum()
    # As they explain in the paper, "Following Epple et al. (1991), cumulative capacity is lagged one year to account
    # for the time it takes to incorporate new techniques obtained as a result of learning from experience."
    tb_nemet_capacity = tb_nemet[["year", "cost", "previous_capacity"]].rename(
        columns={"previous_capacity": "cumulative_capacity"}, errors="raise"
    )[["year", "cumulative_capacity"]]
    # Add column of origin of the data.
    tb_nemet_capacity["cumulative_capacity_source"] = "Nemet (2009)"

    # I haven't found a precise definition of the variables in IRENA's dataset, but I expect this to be
    # cumulative capacity.
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
    # Costs are given in "2004 USD/Watt", so we need to convert them to the latest year USD.
    tb_nemet_cost["cost"] *= USD2004_TO_USDLATEST
    tb_nemet_cost["cost"].metadata.unit = f"{LATEST_YEAR} USD/Watt"

    # Prepare solar photovoltaic cost data from Farmer & Lafond (2016).
    tb_farmer_lafond = (
        tb_farmer_lafond[["year", "photovoltaics"]]
        .dropna()
        .reset_index(drop=True)
        .rename(columns={"photovoltaics": "cost"}, errors="raise")
    )
    tb_farmer_lafond["cost_source"] = "Farmer & Lafond (2016)"
    # Costs are given in "2013 USD/Wp", so we need to convert them to the latest year USD.
    tb_farmer_lafond["cost"] *= USD2013_TO_USDLATEST
    tb_farmer_lafond["cost"].metadata.unit = f"{LATEST_YEAR} USD/Watt"

    # Prepare solar photovoltaic cost data from IRENA.
    tb_irena_cost = tb_irena_cost.drop(columns="country", errors="raise")

    tb_irena_cost["cost_source"] = "IRENA"
    # Costs are given in latest year "USD/W", so we do not need to correct them.

    # Combine Nemet (2009) and Farmer & Lafond (2016), prioritizing the former.
    combined = combine_two_overlapping_dataframes(df1=tb_nemet_cost, df2=tb_farmer_lafond, index_columns="year")

    # Combine the previous with IRENA, prioritizing the latter.
    combined = combine_two_overlapping_dataframes(df1=tb_irena_cost, df2=combined, index_columns="year")

    # Since sources column has been manually created, it does not have metadata. Copy origins from another column.
    combined["cost_source"].metadata.origins = combined["cost"].metadata.origins.copy()

    return combined


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load Nemet (2009) dataset from garden and read its main table.
    ds_nemet = paths.load_dataset("nemet_2009")
    tb_nemet = ds_nemet["nemet_2009"].reset_index()

    # Load Farmer & Lafond (2016) dataset from garden and read its main table.
    ds_farmer_lafond = paths.load_dataset("farmer_lafond_2016")
    tb_farmer_lafond = ds_farmer_lafond["farmer_lafond_2016"].reset_index()

    # Load IRENA dataset on capacity from garden and read its main table.
    ds_irena_capacity = paths.load_dataset("renewable_electricity_capacity")
    tb_irena_capacity = ds_irena_capacity["renewable_electricity_capacity"].reset_index()

    # Load IRENA dataset on cost from garden and read its main table.
    ds_irena_cost = paths.load_dataset("renewable_power_generation_costs")
    tb_irena_cost = ds_irena_cost["solar_photovoltaic_module_prices"].reset_index()

    #
    # Process data.
    #
    # Create a table of cumulative solar photovoltaic capacity, by combining Nemet (2009) and IRENA data.
    cumulative_capacity = prepare_capacity_data(tb_nemet=tb_nemet, tb_irena_capacity=tb_irena_capacity)

    # Sanity check.
    error = "IRENA data has changed, prices may need to be deflated to the latest year."
    assert tb_irena_cost["year"].max() == LATEST_YEAR, error

    # Create a table of solar photovoltaic cost, by combining Nemet (2009), Farmer & Lafond (2016) and IRENA data.
    cost = prepare_cost_data(tb_nemet=tb_nemet, tb_irena_cost=tb_irena_cost, tb_farmer_lafond=tb_farmer_lafond)

    # Combine capacity and cost data.
    tb_combined = pr.merge(cost, cumulative_capacity, on="year", how="outer")

    # Add column for region.
    tb_combined = tb_combined.assign(**{"country": "World"})

    # Set an appropriate index and sort conveniently.
    tb_combined = tb_combined.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Rename table.
    tb_combined.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb_combined], check_variables_metadata=True)
    ds_garden.save()

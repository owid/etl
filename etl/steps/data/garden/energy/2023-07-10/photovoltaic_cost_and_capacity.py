"""Combine data from Nemet (2009), Farmer & Lafond (2016) and IRENA on photovoltaic cost and capacity.

Data content:
* Nemet (2009) provides cumulative capacity data between 1975 and 2003.
* Nemet (2009) provides cost data between 1975 and 2003.
* IRENA provides cumulative capacity data between 2000 and 2021.
* IRENA provides cost data between 2010 and 2021.
* Farmer & Lafond (2016) provide cost data between 1980 and 2013.

For each informed year, we need to combine these sources with the following two constraints:
* Having data from the most recent source.
* Avoid (as much as possible) having cost and capacity data on a given year from different sources.

Therefore, for capacity data, we use Nemet (2009) between 1975 and 2003, and IRENA between 2004 and 2021.
For cost data, we use Nemet (2009) between 1975 and 2003, Farmer & Lafond (2016) between 2004 and 2009, and IRENA between 2010 and 2021.

"""

import owid.catalog.processing as pr
from owid.catalog import Dataset, Table
from owid.catalog.tables import (
    get_unique_licenses_from_tables,
    get_unique_sources_from_tables,
)
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)

# Conversion factors.
# Convert 2004 USD to 2021 USD.
USD2004_TO_USD2021 = 1.42
# Convert 2013 USD to 2021 USD.
USD2013_TO_USD2021 = 1.19


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

    # Combine cumulative capacity from Nemet (2009) and IRENA, prioritising the former on ovelapping years.
    cumulative_capacity = (
        combine_two_overlapping_dataframes(df1=tb_nemet_capacity, df2=tb_irena_capacity, index_columns=["year"])
        .astype({"year": int})
        .sort_values("year")
        .reset_index(drop=True)
    )
    # NOTE: The previous operation does not propagate metadata. Manually combine sources.
    for column in ["cumulative_capacity", "cumulative_capacity_source"]:
        cumulative_capacity[column].metadata.sources = get_unique_sources_from_tables(
            [tb_nemet_capacity, tb_irena_capacity]
        )
        cumulative_capacity[column].metadata.licenses = get_unique_licenses_from_tables(
            [tb_nemet_capacity, tb_irena_capacity]
        )

    return cumulative_capacity


def prepare_cost_data(tb_nemet: Table, tb_irena_cost: Table, tb_farmer_lafond: Table) -> Table:
    # Prepare solar photovoltaic cost data from Nemet (2009).
    tb_nemet_cost = tb_nemet[["year", "cost"]].copy()
    tb_nemet_cost["cost_source"] = "Nemet (2009)"
    # Costs are given in "2004 USD/Watt", so we need to convert them to 2021 USD.
    tb_nemet_cost["cost"] *= USD2004_TO_USD2021

    # Prepare solar photovoltaic cost data from Farmer & Lafond (2016).
    tb_farmer_lafond = (
        tb_farmer_lafond[["year", "photovoltaics"]]
        .dropna()
        .reset_index(drop=True)
        .rename(columns={"photovoltaics": "cost"}, errors="raise")
    )
    tb_farmer_lafond["cost_source"] = "Farmer & Lafond (2016)"
    # Costs are given in "2013 USD/Wp", so we need to convert them to 2021 USD.
    tb_farmer_lafond["cost"] *= USD2013_TO_USD2021

    # Prepare solar photovoltaic cost data from IRENA.
    tb_irena_cost = tb_irena_cost.drop(columns="country")

    tb_irena_cost["cost_source"] = "IRENA"
    # Costs are given in "2021 USD/W", so we do not need to correct them.

    # Combine Nemet (2009) and Farmer & Lafond (2016), prioritizing the former.
    combined = combine_two_overlapping_dataframes(df1=tb_nemet_cost, df2=tb_farmer_lafond, index_columns="year")

    # Combine the previous with IRENA, prioritizing the latter.
    combined = combine_two_overlapping_dataframes(df1=tb_irena_cost, df2=combined, index_columns="year")

    # NOTE: The previous operation does not propagate metadata. Manually combine sources.
    for column in ["cost", "cost_source"]:
        combined[column].metadata.sources = get_unique_sources_from_tables(
            [tb_nemet_cost, tb_farmer_lafond, tb_irena_cost]
        )
        combined[column].metadata.licenses = get_unique_licenses_from_tables(
            [tb_nemet_cost, tb_farmer_lafond, tb_irena_cost]
        )

    return combined


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load Nemet (2009) dataset from garden and read its main table.
    ds_nemet: Dataset = paths.load_dependency("nemet_2009")
    tb_nemet = ds_nemet["nemet_2009"].reset_index()

    # Load Farmer & Lafond (2016) dataset from garden and read its main table.
    ds_farmer_lafond: Dataset = paths.load_dependency("farmer_lafond_2016")
    tb_farmer_lafond = ds_farmer_lafond["farmer_lafond_2016"].reset_index()

    # Load IRENA dataset on capacity from garden and read its main table.
    ds_irena_capacity: Dataset = paths.load_dependency("renewable_electricity_capacity")
    tb_irena_capacity = ds_irena_capacity["renewable_electricity_capacity"].reset_index()

    # Load IRENA dataset on cost from garden and read its main table.
    ds_irena_cost: Dataset = paths.load_dependency("renewable_power_generation_costs")
    tb_irena_cost = ds_irena_cost["solar_photovoltaic_module_prices"].reset_index()

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

    # Set an appropriate index and sort conveniently.
    tb_combined = tb_combined.set_index(["country", "year"], verify_integrity=True).sort_index()

    # Rename table.
    tb_combined.metadata.short_name = paths.short_name

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb_combined], check_variables_metadata=True)
    # NOTE: Currently, ETL fails if the dataset has no sources. Therefore, manually gather sources from all variables.
    ds_garden.metadata.sources = get_unique_sources_from_tables([tb_combined])
    ds_garden.save()

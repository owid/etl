"""Combine data from Nemet (2009), Farmer & Lafond (2016) and IRENA on photovoltaic cost and capacity.

Data content:
* Nemet (2009) provides cumulative capacity data between 1975 and 2003.
* Nemet (2009) provides cost data between 1975 and 2003.
* IRENA provides cumulative capacity data from 2000 onwards.
* IRENA provides cost data from 2010 onwards, in two series:
  - A global module price index, from 2010 to 2024 (Renewable Power Generation Costs in 2024 report). IRENA
    discontinued it in the following report.
  - Yearly prices by module technology sold in Europe, from 2020 onwards (Renewable Power Generation Costs in 2025
    report). We use the "Mainstream" technology (standard crystalline modules), which is the closest to the index
    where both overlap.
* Farmer & Lafond (2016) provide cost data between 1980 and 2013.

For each informed year, we need to combine these sources with the following two constraints:
* Having data from the most recent source.
* Avoid (as much as possible) having cost and capacity data on a given year from different sources.

Therefore, for capacity data, we use Nemet (2009) between 1975 and 2003, and IRENA from 2004 onwards.
For cost data, we use Nemet (2009) between 1975 and 2003, Farmer & Lafond (2016) between 2004 and 2009, IRENA's
global price index between 2010 and 2019, and IRENA's mainstream module price from 2020 onwards.

"""

import owid.catalog.processing as pr
from owid.catalog import Table
from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)

# IRENA costs are given in the latest year's USD, so we convert other costs to the same currency.
LATEST_YEAR = 2025
# First year of IRENA's module prices by technology, from which they replace the discontinued global price index.
FIRST_YEAR_BY_TECHNOLOGY = 2020
# Module technology (as named by IRENA) used from FIRST_YEAR_BY_TECHNOLOGY onwards.
MODULE_TECHNOLOGY = "Mainstream"


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


def sanity_check_irena_inputs(tb_irena_index: Table, tb_irena_by_technology: Table) -> None:
    expected_unit = f"constant {LATEST_YEAR} US$ per watt"
    for tb in [tb_irena_index, tb_irena_by_technology]:
        assert tb["cost"].metadata.unit == expected_unit, "IRENA data has changed, prices may need to be deflated."
        assert set(tb["country"]) == {"World"}, "Unexpected regions in IRENA module prices."
        assert (tb["cost"] > 0).all(), "Non-positive IRENA module prices."

    error = f"IRENA's global price index should cover 2010 to {FIRST_YEAR_BY_TECHNOLOGY - 1} without gaps."
    assert set(range(2010, FIRST_YEAR_BY_TECHNOLOGY)) <= set(tb_irena_index["year"]), error

    assert MODULE_TECHNOLOGY in set(tb_irena_by_technology["technology"]), "IRENA module technologies have changed."
    tb_technology = tb_irena_by_technology[tb_irena_by_technology["technology"] == MODULE_TECHNOLOGY]
    assert not tb_technology.duplicated(subset=["year"]).any(), "Duplicate years in IRENA module prices."
    error = f"'{MODULE_TECHNOLOGY}' module prices should cover {FIRST_YEAR_BY_TECHNOLOGY} to {LATEST_YEAR}."
    assert set(tb_technology["year"]) == set(range(FIRST_YEAR_BY_TECHNOLOGY, LATEST_YEAR + 1)), error

    # The two series should agree within 20% in the overlapping years, so that they can be joined without rescaling.
    # NOTE: 2024 is excluded, since the index fell much less than every technology.
    index = tb_irena_index.set_index("year")["cost"]
    technology = tb_technology.set_index("year")["cost"]
    for year in [2020, 2021, 2022, 2023]:
        ratio = technology.loc[year] / index.loc[year]
        error = f"'{MODULE_TECHNOLOGY}' module price in {year} is {ratio:.2f} times IRENA's global price index."
        assert 0.8 <= ratio <= 1.2, error


def prepare_irena_cost_data(tb_irena_index: Table, tb_irena_by_technology: Table) -> Table:
    """Join IRENA's global module price index (until it was discontinued) with its price of mainstream modules."""
    # Global price index, until the year before prices by technology start.
    tb_irena_index = tb_irena_index[tb_irena_index["year"] < FIRST_YEAR_BY_TECHNOLOGY][["year", "cost"]].reset_index(
        drop=True
    )
    tb_irena_index["cost_source"] = "IRENA (global price index)"

    # Price of the chosen module technology from then on.
    tb_irena_technology = tb_irena_by_technology[tb_irena_by_technology["technology"] == MODULE_TECHNOLOGY][
        ["year", "cost"]
    ].reset_index(drop=True)
    tb_irena_technology["cost_source"] = f"IRENA ({MODULE_TECHNOLOGY.lower()} modules)"

    tb_irena_cost = pr.concat([tb_irena_index, tb_irena_technology], ignore_index=True).astype({"year": int})

    return tb_irena_cost


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

    # IRENA costs (already joined, with their sources) are given in latest year "USD/W", so we do not correct them.

    # Combine Nemet (2009) and Farmer & Lafond (2016), prioritizing the former.
    combined = combine_two_overlapping_dataframes(df1=tb_nemet_cost, df2=tb_farmer_lafond, index_columns="year")

    # Combine the previous with IRENA, prioritizing the latter.
    combined = combine_two_overlapping_dataframes(df1=tb_irena_cost, df2=combined, index_columns="year")

    # Improve metadata.
    # Since sources column has been manually created, it does not have metadata. Copy origins from another column.
    combined["cost_source"].metadata.origins = combined["cost"].metadata.origins.copy()

    return combined


def sanity_check_outputs(tb: Table) -> None:
    tb_cost = tb.dropna(subset="cost")
    error = f"Cost data should cover every year from 1975 to {LATEST_YEAR}."
    assert sorted(tb_cost["year"]) == list(range(1975, LATEST_YEAR + 1)), error
    assert (tb_cost["cost"] > 0).all(), "Non-positive costs."
    assert tb_cost["cost_source"].notna().all(), "Some costs have no source."
    expected_sources = {
        "Nemet (2009)",
        "Farmer & Lafond (2016)",
        "IRENA (global price index)",
        f"IRENA ({MODULE_TECHNOLOGY.lower()} modules)",
    }
    assert set(tb_cost["cost_source"]) == expected_sources, "Unexpected cost sources."
    error = "Module prices should have fallen by a factor of more than 100 since 1975."
    assert tb_cost.set_index("year")["cost"].loc[1975] / tb_cost.set_index("year")["cost"].loc[LATEST_YEAR] > 100, error


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

    # Load IRENA dataset on cost from garden, and read its global module price index (discontinued after 2024) and its
    # yearly module prices by technology (from 2020 onwards).
    ds_irena_cost = paths.load_dataset("renewable_power_generation_costs")
    tb_irena_index = ds_irena_cost.read("solar_photovoltaic_module_prices")
    tb_irena_by_technology = ds_irena_cost.read("solar_photovoltaic_module_prices_by_technology")

    sanity_check_irena_inputs(tb_irena_index=tb_irena_index, tb_irena_by_technology=tb_irena_by_technology)

    #
    # Process data.
    #
    # Create a table of cumulative solar photovoltaic capacity, by combining Nemet (2009) and IRENA data.
    cumulative_capacity = prepare_capacity_data(tb_nemet=tb_nemet, tb_irena_capacity=tb_irena_capacity)

    # Join IRENA's global price index and its price of mainstream modules.
    tb_irena_cost = prepare_irena_cost_data(
        tb_irena_index=tb_irena_index, tb_irena_by_technology=tb_irena_by_technology
    )

    # Create a table of solar photovoltaic cost, by combining Nemet (2009), Farmer & Lafond (2016) and IRENA data.
    cost = prepare_cost_data(tb_nemet=tb_nemet, tb_irena_cost=tb_irena_cost, tb_farmer_lafond=tb_farmer_lafond)

    # Combine capacity and cost data.
    tb_combined = pr.merge(cost, cumulative_capacity, on="year", how="outer")

    # Add column for region.
    tb_combined = tb_combined.assign(**{"country": "World"})

    sanity_check_outputs(tb=tb_combined)

    # Format table conveniently.
    tb_combined = tb_combined.format(short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = paths.create_dataset(tables=[tb_combined], yaml_params={"LATEST_YEAR": LATEST_YEAR})
    ds_garden.save()

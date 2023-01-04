"""Combine data from Nemet (2009), Farmer & Lafond (2016) and IRENA on photovoltaic cost and capacity.

"""

from owid import catalog

from etl.helpers import PathFinder

__file__ = (
    "/Users/prosado/Documents/owid/repos/etl/etl/steps/data/garden/energy/2023-01-04/photovoltaic_cost_and_capacity"
)

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load Nemet (2009) dataset from Garden.
    ds_nemet: catalog.Dataset = paths.load_dependency("nemet_2009")
    tb_memet = ds_nemet["nemet_2009"].reset_index()

    # Load Farmer & Lafond (2016) dataset from Garden.
    ds_farmer_lafond: catalog.Dataset = paths.load_dependency("farmer_lafond_2016")
    tb_farmer_lafond = ds_farmer_lafond["farmer_lafond_2016"].reset_index()

    # Load IRENA dataset on capacity from Garden.
    ds_irena_capacity: catalog.Dataset = paths.load_dependency("renewable_electricity_capacity")
    tb_irena_capacity = ds_irena_capacity["renewable_electricity_capacity"].reset_index()

    # Load IRENA dataset on cost from Garden.
    ds_irena_cost: catalog.Dataset = paths.load_dependency("renewable_power_generation_costs")
    tb_irena_cost = ds_irena_cost["renewable_power_generation_costs"].reset_index()

    #
    # Process data.
    #
    # TODO: Ensure all dollar units are the same.
    # TODO: Decide which capacity column to use.
    tb_memet = tb_memet[["year", "cost", "yearly_capacity"]].rename(
        columns={"yearly_capacity": "capacity"}, errors="raise"
    )

    tb_farmer_lafond = (
        tb_farmer_lafond[["year", "photovoltaics"]]
        .dropna()
        .reset_index(drop=True)
        .rename(columns={"photovoltaics": "cost"}, errors="raise")
    )

    tb_irena_capacity = (
        tb_irena_capacity[tb_irena_capacity["country"] == "World"][["year", "solar_photovoltaic"]]
        .rename(columns={"solar_photovoltaic": "capacity"}, errors="raise")
        .reset_index(drop=True)
    )

    tb_irena_cost = (
        tb_irena_cost[tb_irena_cost["country"] == "World"][["year", "solar_photovoltaic"]]
        .rename(columns={"solar_photovoltaic": "cost"}, errors="raise")
        .dropna()
        .reset_index(drop=True)
    )

    # TODO: Combine tables.
    tb_combined = tb_irena_cost.copy()

    #
    # Save outputs.
    #
    # Create a new dataset with the same metadata as meadow
    ds_garden = catalog.Dataset.create_empty(dest_dir)

    # Create a new table.
    ds_garden.add(tb_combined)

    # Update dataset metadata and save dataset.
    ds_garden.update_metadata(paths.metadata_path)
    ds_garden.save()

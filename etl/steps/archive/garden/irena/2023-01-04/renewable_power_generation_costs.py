import pandas as pd
from owid import catalog

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load dataset from Meadow.
    ds_meadow: catalog.Dataset = paths.load_dependency("renewable_power_generation_costs")
    # Load main table from dataset.
    tb_meadow = ds_meadow["renewable_power_generation_costs"]
    # Load table on solar photovoltaic module prices.
    tb_meadow_solar_pv = ds_meadow["solar_photovoltaic_module_prices"]

    # Create dataframes out of the tables.
    df = pd.DataFrame(tb_meadow).reset_index()
    df_pv = pd.DataFrame(tb_meadow_solar_pv).reset_index()

    # Harmonize country names.
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)

    #
    # Save outputs.
    #
    # Create a new Garden dataset.
    ds_garden = catalog.Dataset.create_empty(dest_dir)

    # Create a new table of LCOE and add it to the dataset.
    tb_garden = catalog.Table(df, underscore=True, short_name=paths.short_name)
    ds_garden.add(tb_garden)

    # Create a new table of solar PV module prices and add it to the dataset.
    tb_garden_pv = catalog.Table(df_pv, underscore=True, short_name="solar_photovoltaic_module_prices")
    ds_garden.add(tb_garden_pv)

    # Update metadata and save dataset.
    ds_garden.update_metadata(paths.metadata_path)
    ds_garden.save()

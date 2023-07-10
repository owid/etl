import pandas as pd
from owid import catalog
from shared import CURRENT_DIR

from etl.paths import DATA_DIR

# Details for dataset to export.
DATASET_SHORT_NAME = "fossil_fuel_reserves_production_ratio"
DATASET_TITLE = "Fossil fuel reserves/production ratio"
METADATA_FILE_PATH = CURRENT_DIR / f"{DATASET_SHORT_NAME}.meta.yml"
# Path to BP statistical review datset to import.
BP_DATASET_PATH = DATA_DIR / "garden/bp/2022-12-28/statistical_review"


def prepare_bp_data(tb_bp: catalog.Table) -> catalog.Table:
    # Prepare BP data.
    columns = {
        "country": "country",
        "year": "year",
        "coal__reserves__total": "coal_reserves",
        "coal_production__tonnes": "coal_production",
        "oil__proved_reserves": "oil_reserves",
        "oil_production__barrels": "oil_production",
        "gas__proved_reserves": "gas_reserves",
        "gas_production__bcm": "gas_production",
    }
    df_bp = pd.DataFrame(tb_bp).reset_index()[list(columns)].rename(columns=columns)

    # Select only global data.
    df_bp = df_bp[df_bp["country"] == "World"].reset_index(drop=True)

    # Check that the units are the expected ones.
    assert tb_bp["coal__reserves__total"].metadata.unit == "Million tonnes"
    assert tb_bp["coal_production__tonnes"].metadata.unit == "Million tonnes"
    # WARNING: Here the "unit" metadata field seems to be wrong, it should be billion barrels.
    assert tb_bp["oil__proved_reserves"].metadata.unit == "Barrels"
    assert tb_bp["oil_production__barrels"].metadata.unit == "Thousand barrels per day"
    assert tb_bp["gas__proved_reserves"].metadata.unit == "Trillion cubic metres"
    assert tb_bp["gas_production__bcm"].metadata.unit == "Billion cubic metres"

    # Convert to tonnes.
    # Million tonnes to tonnes.
    df_bp["coal_reserves"] *= 1e6
    # Million tonnes to tonnes.
    df_bp["coal_production"] *= 1e6
    # Billion barrels to tonnes.
    df_bp["oil_reserves"] *= 1e9 * 0.1364
    # Thousand barrels per day to tonnes per year.
    df_bp["oil_production"] *= 1e3 * 365 * 0.1364
    # Trillion cubic meters to cubic meters.
    df_bp["gas_reserves"] *= 1e12
    # Billion cubic meters to cubic meters.
    df_bp["gas_production"] *= 1e9

    # Create columns for reserves-production ratio (measured in years of fossil fuels left).
    df_bp["coal_left"] = df_bp["coal_reserves"] / df_bp["coal_production"]
    df_bp["oil_left"] = df_bp["oil_reserves"] / df_bp["oil_production"]
    df_bp["gas_left"] = df_bp["gas_reserves"] / df_bp["gas_production"]

    # Set index, drop rows that only have nans, and sort conveniently.
    df_bp = (
        df_bp.set_index(["country", "year"], verify_integrity=True).dropna(how="all").sort_index().sort_index(axis=1)
    )

    # Create a new table.
    tb = catalog.Table(df_bp, underscore=True)

    return tb


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Read all required datasets.
    ds_bp = catalog.Dataset(BP_DATASET_PATH)

    # Gather all required tables from all datasets.
    tb_bp = ds_bp[ds_bp.table_names[0]]

    #
    # Process data.
    #
    # Prepare BP data.
    tb = prepare_bp_data(tb_bp=tb_bp)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = catalog.Dataset.create_empty(dest_dir)

    # Add table to dataset.
    tb.metadata.short_name = "fossil_fuel_reserves_production_ratio"
    ds_garden.add(tb)

    # Update dataset and table metadata using yaml file.
    ds_garden.update_metadata(METADATA_FILE_PATH)

    # Save dataset.
    ds_garden.save()

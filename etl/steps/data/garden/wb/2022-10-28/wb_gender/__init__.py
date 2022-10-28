"""WB Gender Garden step."""
from pathlib import Path

from owid import catalog
from owid.datautils import geo
from structlog import get_logger

from etl.paths import BASE_DIR as base_path

log = get_logger()


def init_garden_ds(dest_dir: str, ds_meadow: catalog.Dataset) -> catalog.Dataset:
    """Initiate garden dataset.

    Returns
    -------
    catalog.Dataset
        Garden dataset.
    """
    ds = catalog.Dataset.create_empty(dest_dir, ds_meadow.metadata)
    # ds.metadata = catalog.DatasetMeta(
    #     namespace="wb",
    #     short_name="wb_gender",
    #     title="Gender Statistics - World Bank (2022)",
    #     description="Gender statistics by the World Bank. More details at https://genderdata.worldbank.org/.",
    #     version="2022",
    # )
    return ds


def load_meadow_ds() -> catalog.Dataset:
    """Load dataset from Meadow.

    Returns
    -------
    catalog.Dataset
        Meadow dataset.
    """
    meadow_path = base_path / "data/meadow/wb/2022-10-28/wb_gender"
    ds = catalog.Dataset(meadow_path)
    return ds


def clean_data_table(table: catalog.Table) -> catalog.Table:
    """Clean data.

    Parameters
    ----------
    ds : catalog.Table
        Data table.

    Returns
    -------
    catalog.Table
        Table.
    """
    log.info("Loading meadow dataset...")
    table = table.reset_index()
    # Harmonize country names
    log.info("Harmonize country names...")
    table = geo.harmonize_countries(
        df=table,
        countries_file=Path(__file__).parent / "wb_gender.countries.json",
        country_col="country",
        make_missing_countries_nan=True,
        show_full_warning=False,
    )
    # Drop countries without hamonized name
    table = table.dropna(subset=["country"])
    # Set index
    log.info("Set index...")
    column_idx = ["country", "variable", "year"]
    table = table.sort_values(column_idx).set_index(column_idx)
    return table


def run(dest_dir: str) -> None:
    # Load meadow dataset
    ds_meadow = load_meadow_ds()
    # Create garden dataset
    ds_garden = init_garden_ds(dest_dir, ds_meadow)
    # Obtain data table
    table = clean_data_table(ds_meadow["data"])
    # Add table to garden dataset
    ds_garden.add(table)
    # Save state
    ds_garden.save()

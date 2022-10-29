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
    meadow_path = base_path / "data/meadow/wb/2022-10-29/wb_gender"
    ds = catalog.Dataset(meadow_path)
    return ds


def make_table(table: catalog.Table) -> catalog.Table:
    """Generate dataset table.

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


def make_table_metadata(table: catalog.Table, table_metadata: catalog.Table) -> catalog.Table:
    """Create metadata table.

    Parameters
    ----------
    table : catalog.Table
        Raw metadata table.

    Returns
    -------
    catalog.Table
        Metadata table.
    """
    columns_relevant = [
        "indicator_name",
        "topic",
        "series_code",
        "long_definition",
        "unit_of_measure",
        "source",
        "related_source_links",
        "license_type",
    ]
    # Get dataset generic license
    license_ = table.metadata.dataset.licenses[0]
    # Keep relevant columns
    table_metadata = table_metadata[columns_relevant]
    # Check that all variables have either no license or generic dataset license
    licenses = table_metadata.license_type.dropna().unique()
    assert len(licenses) == 1
    assert licenses[0] == "CC BY-4.0", f"License in dataset should be 'CC BY-4.0', but is {licenses[0].name} instead."
    assert license_.name == "CC BY 4.0", f"License in dataset should be 'CC BY 4.0', but is {license_.name} instead."
    # Assign generic dataset license to all variables
    # Replace NaNs in unit_of_measure for empty strings
    table_metadata = table_metadata.assign(
        license_name=license_.name,
        license_url=license_.url,
        unit_of_measure=table_metadata["unit_of_measure"].astype(str).apply(lambda x: x if x != "nan" else ""),
    ).drop(columns=["license_type"])
    return table_metadata


def run(dest_dir: str) -> None:
    # Load meadow dataset
    ds_meadow = load_meadow_ds()
    # Create garden dataset
    ds_garden = init_garden_ds(dest_dir, ds_meadow)
    # Obtain data table
    table = make_table(ds_meadow["wb_gender"])
    # Add table to garden dataset
    ds_garden.add(table)
    # Obtain metadata table
    metadata = make_table_metadata(ds_meadow["wb_gender"], ds_meadow["metadata_variables"])
    # Add table to garden dataset
    ds_garden.add(metadata)
    # Save state
    ds_garden.save()

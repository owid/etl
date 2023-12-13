"""Garden step for European Electricity Review (Ember, 2022).

"""

from owid.catalog import Table
from owid.datautils import dataframes

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get naming conventions.
paths = PathFinder(__file__)

# Convert from megawatt-hours to kilowatt-hours.
MWH_TO_KWH = 1000


def process_net_flows_data(table: Table, tb_regions: Table) -> Table:
    """Process net flows data, including country harmonization.

    Parameters
    ----------
    table : Table
        Table from the meadow dataset on net flows.
    tb_regions : Table
        Table from the owid countries-regions dataset.

    Returns
    -------
    table: Table
        Processed table.

    """
    tb = table.reset_index()

    # Create dictionary mapping country codes to harmonized country names.
    country_code_to_name = tb_regions[["name"]].to_dict()["name"]
    # Add Kosovo, which is missing in countries-regions.
    if "XKX" not in country_code_to_name:
        country_code_to_name["XKX"] = "Kosovo"

    columns = {
        "source_country_code": "source_country",
        "target_country_code": "target_country",
        "year": "year",
        "net_flow_twh": "net_flow__twh",
    }
    tb = tb[list(columns)].rename(columns=columns, errors="raise")
    # Change country codes to harmonized country names in both columns.
    for column in ["source_country", "target_country"]:
        tb[column] = dataframes.map_series(
            series=tb[column],
            mapping=country_code_to_name,
            warn_on_missing_mappings=True,
            show_full_warning=True,
        )

    # Set an appropriate index, and sort conveniently.
    tb = tb.set_index(["source_country", "target_country", "year"], verify_integrity=True).sort_index()

    return tb


def process_generation_data(table: Table) -> Table:
    """Process electricity generation data, including country harmonization.

    Parameters
    ----------
    table : Table
        Table from the meadow dataset on electricity generation.

    Returns
    -------
    table: Table
        Processed table.

    """
    tb = table.reset_index()

    # Sanity checks.
    error = "Columns fuel_code and fuel_desc have inconsistencies."
    assert tb.groupby("fuel_code").agg({"fuel_desc": "nunique"})["fuel_desc"].max() == 1, error
    assert tb.groupby("fuel_desc").agg({"fuel_code": "nunique"})["fuel_code"].max() == 1, error

    # Select useful columns and rename them conveniently.
    columns = {
        "country_name": "country",
        "year": "year",
        "fuel_desc": "fuel_desc",
        "generation_twh": "TWh",
        "share_of_generation_pct": "%",
    }
    tb = tb[list(columns)].rename(columns=columns, errors="raise")

    # Convert from long to wide format table.
    tb = tb.pivot(index=["country", "year"], columns=["fuel_desc"], join_column_levels_with="__")

    # Invert the energy source and unit in the column names.
    tb = tb.rename(
        columns={
            column: f"{column.split('__')[1]}__{column.split('__')[0]}" for column in tb.columns if "__" in column
        },
        errors="raise",
    )

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
        warn_on_missing_countries=True,
    )

    # Ensure columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.underscore().set_index(["country", "year"], verify_integrity=True).sort_index()

    return tb


def process_country_overview_data(table: Table) -> Table:
    """Process country overview data, including country harmonization.

    Parameters
    ----------
    table : Table
        Table from the meadow dataset.

    Returns
    -------
    table: Table
        Processed table.

    """
    # Rename columns for consistency with global electricity review.
    columns = {
        "country_name": "country",
        "year": "year",
        "generation_twh": "total_generation__twh",
        "net_import_twh": "net_imports__twh",
        "demand_twh": "demand__twh",
        "demand_mwh_per_capita": "demand_per_capita__kwh",
    }
    tb = table.reset_index()[list(columns)].rename(columns=columns, errors="raise")
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
        warn_on_missing_countries=True,
    )

    # Convert units of demand per capita.
    tb["demand_per_capita__kwh"] *= MWH_TO_KWH

    # Set an appropriate index, and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    return tb


def process_emissions_data(table: Table) -> Table:
    """Process emissions data, including country harmonization.

    Parameters
    ----------
    table : Table
        Table from the meadow dataset on emissions.

    Returns
    -------
    table: Table
        Processed table.

    """
    # Rename columns for consistency with global electricity review.
    columns = {
        "country_name": "country",
        "year": "year",
        "emissions_intensity_gco2_kwh": "co2_intensity__gco2_kwh",
        "emissions_mtc02e": "total_emissions__mtco2",
    }
    tb = table.reset_index()[list(columns)].rename(columns=columns, errors="raise")
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
        warn_on_missing_countries=True,
    )

    # Set an appropriate index, and sort conveniently.
    tb = tb.set_index(["country", "year"], verify_integrity=True).sort_index()

    return tb


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load dataset from meadow and read its tables.
    ds_meadow = paths.load_dataset("european_electricity_review")
    tb_country_overview = ds_meadow["country_overview"]
    tb_emissions = ds_meadow["emissions"]
    tb_generation = ds_meadow["generation"]
    tb_net_flows = ds_meadow["net_flows"]

    # Load regions dataset and read its main table (to convert country codes to country names in net flows table).
    ds_regions = paths.load_dataset("regions")
    tb_regions = ds_regions["regions"]

    #
    # Process data.
    #
    # Process each individual table.
    tables = {
        "Country overview": process_country_overview_data(table=tb_country_overview),
        "Emissions": process_emissions_data(table=tb_emissions),
        "Generation": process_generation_data(table=tb_generation),
        "Net flows": process_net_flows_data(table=tb_net_flows, tb_regions=tb_regions),
    }

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables.values(), default_metadata=ds_meadow.metadata, check_variables_metadata=True
    )
    ds_garden.save()

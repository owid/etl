import owid.catalog.processing as pr
from owid.catalog import Dataset, Table, Variable
from owid.datautils import dataframes

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


SUB_TECHNOLOGY_RENAMING = {
    "Smart Grids": "Smart grids",
    "Thermal energy storage": "Thermal energy storage",
    "Hydrogen (storage and distribution and applications)": "Hydrogen",
    "Hydropower": "Hydropower",
    "PV": "Solar PV",
    "Solar Thermal": "Solar thermal",
    "CCUS": "Carbon capture and storage",
    "Cross-cutting": "Cross-cutting energy tech",
    "Energy Storage - General": "Storage (general)",
    "Biofuels": "Bioenergy",
    "Fuel from waste": "Bioenergy",
    "Batteries": "Batteries",
    "Fuel Cells": "Fuel cells",
    "Green hydrogen (water eloctrolysis)": "Green hydrogen",
    "Ocean Energy": "Marine and tidal",
    "PV - Thermal Hybrid": "Solar PV-thermal hybrid",
    "Wind Energy": "Wind",
    "Electromobility - Charging Stations": "EV charging stations",
    "Electromobility - Energy Storage": "EV storage",
    "Energy Efficiency": "Efficiency",
    "Others": "Other",
    "Ultracapacitors, supercapacitors, double-layer capacitors": "Ultracapacitors",
    "Mechanical energy storage, e.g. flywheels or pressurised fluids": "Storage (excl. batteries)",
    "Geothermal Energy": "Geothermal",
    "Electromobility - Electric Energy Management": "EV management",
    "Electromobility - Information/Communication Technologies": "EV communication tech",
    "Electromobility - Machine related technology ": "EV machine tech",
    "Heat pumps": "Geothermal",
}

# List of aggregate regions to create.
REGIONS_TO_ADD = [
    # Continents.
    "Africa",
    "Asia",
    "Europe",
    "North America",
    "Oceania",
    "South America",
    # Income groups.
    "High-income countries",
    "Low-income countries",
    "Lower-middle-income countries",
    "Upper-middle-income countries",
    # Other regions.
    "World",
    "European Union (27)",
]


def regroup_sub_technologies(tb: Table) -> Table:
    """Rename sub-technologies conveniently, ignore sector and technology, and recalculate the number of patents per
    sub-technology.

    Parameters
    ----------
    tb : Table
        Data with number of patents per country, year, sector, technology and sub-technology.

    Returns
    -------
    tb : Table
        Data with number of patents per country, year and sub-technology.

    """
    # It seems that sub-technologies can belong to only one technology.
    # As long as this is true, we can just ignore "technology" and select by "sub_technology".
    assert set(tb.groupby("sub_technology").agg({"technology": "nunique"}).reset_index()["technology"]) == {1}
    # This does not happen with sectors.
    # For example, sub-technology Biofuels is included in sectors "Power", "Transport", and "Industry".

    # There seems to be one nan sub-technology. Add this to "Others".
    tb["sub_technology"] = tb["sub_technology"].fillna("Others")

    # Rename sub-technologies conveniently.
    tb["sub_technology"] = Variable(
        dataframes.map_series(
            series=tb["sub_technology"],
            mapping=SUB_TECHNOLOGY_RENAMING,
            warn_on_missing_mappings=True,
            warn_on_unused_mappings=True,
        ),
        name="sub_technology",
    ).copy_metadata(tb["sub_technology"])

    # After renaming, some sub-technologies have been combined (e.g. Biofuels and Fuel from waste are now both Bioenergy).
    # Re-calculate numbers of patents with the new sub-technology groupings.
    tb = (
        tb.groupby(["country", "year", "sector", "technology", "sub_technology"], observed=True)
        .agg({"patents": "sum"})
        .reset_index()
    )

    return tb


def add_region_aggregates(tb: Table, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    # Add aggregates for continents and income groups.
    for region in REGIONS_TO_ADD:
        if region == "World":
            # For the world, add all countries that are not in regions (also exclude any regions given in the original
            # data, that should be named "* (IRENA)" after harmonization, if there was any).
            countries_in_region = [
                country
                for country in tb["country"].unique()
                if country not in REGIONS_TO_ADD
                if not country.endswith("(IRENA)")
            ]
        else:
            # List countries in region.
            countries_in_region = geo.list_members_of_region(
                region=region, ds_regions=ds_regions, ds_income_groups=ds_income_groups
            )
        region_data = (
            tb[tb["country"].isin(countries_in_region)]
            .groupby(["year", "sector", "technology", "sub_technology"], observed=True)
            .agg({"patents": "sum"})
            .reset_index()
            .assign(**{"country": region})
        )
        # Add data for new region to table.
        tb = pr.concat([tb, region_data], ignore_index=True)

    return tb


def create_patents_by_sub_technology(tb: Table) -> Table:
    """Create a simplified wide table that counts number of patents by sub-technologies.

    It will also add a column for the total number of patents (the sum of patents of all sub-technologies).

    Parameters
    ----------
    tb : Table
        Data in long table format (indexed by country, year, sector, technology and sub-technology).

    Returns
    -------
    patents_by_sub_technology : Table
        Wide table of number of patents, indexed by country and year, and with a column per sub-technology.

    """
    # Create a simplified table giving just number of patents by sub-technology.
    # Re-calculate numbers of patents by sub-technology.
    patents_by_sub_technology = (
        tb.reset_index()
        .groupby(["country", "year", "sub_technology"], observed=True)
        .agg({"patents": "sum"})
        .reset_index()
    )

    # Restructure table to have a column per sub-technology.
    patents_by_sub_technology = patents_by_sub_technology.pivot(
        index=["country", "year"], columns="sub_technology", values="patents", join_column_levels_with=" - "
    )

    # Set an appropriate index to the table by sub-technology and sort conveniently.
    patents_by_sub_technology = patents_by_sub_technology.set_index(
        ["country", "year"], verify_integrity=True
    ).sort_index()

    # Create a column for the total number of patents of all sub-technologies.
    patents_by_sub_technology["Total patents"] = patents_by_sub_technology.sum(axis=1)

    # Update the table's short name.
    patents_by_sub_technology.metadata.short_name = "renewable_energy_patents_by_technology"

    return patents_by_sub_technology


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("renewable_energy_patents")
    tb_meadow = ds_meadow["renewable_energy_patents"].reset_index()

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    #
    # Process data.
    #
    # Rename sub-technologies conveniently, and regroup them according to the new sub-technology names.
    tb = regroup_sub_technologies(tb=tb_meadow)

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Add region aggregates.
    tb = add_region_aggregates(tb=tb, ds_regions=ds_regions, ds_income_groups=ds_income_groups)

    # Set an appropriate index to main table and sort conveniently.
    tb = tb.set_index(["country", "year", "sector", "technology", "sub_technology"], verify_integrity=True).sort_index()

    # Create a new, simplified table, that shows number of patents by sub-technology.
    patents_by_sub_technology = create_patents_by_sub_technology(tb=tb)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir=dest_dir, tables=[tb, patents_by_sub_technology], check_variables_metadata=True)
    ds_garden.save()

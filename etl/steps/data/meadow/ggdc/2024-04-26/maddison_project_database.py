"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

GDP_PC_COLUMNS = {
    "East Asia": "East Asia",
    "Eastern Europe": "Eastern Europe",
    "Latin America": "Latin America",
    "Middle East and North Africa": "Middle East and North Africa",
    "South and South East Asia": "South and South East Asia",
    "Sub Saharan Africa": "Sub Saharan Africa",
    "Western Europe": "Western Europe",
    "Western Offshoots": "Western offshoots",
    "World GDP pc": "World",
}
POPULATION_COLUMNS = {
    "East Asia": "East Asia",
    "Eastern Europe": "Eastern Europe",
    "Latin America": "Latin America",
    "Middle East and North Africa": "Middle East and North Africa",
    "South and South East Asia": "South and South East Asia",
    "Sub Saharan SSA": "Sub Saharan Africa",
    "Western Europe": "Western Europe",
    "Western Offshoots": "Western offshoots",
    "World Population": "World",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("maddison_project_database.xlsx")

    # Load data from snapshot.
    tb = snap.read(sheet_name="Full data")
    tb_regions = snap.read(sheet_name="Regional data")

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def format_regional_data(tb: Table) -> Table:
    """
    Combine GDP pc and population data in the Regional data sheet and make it long.
    """

    # Prepare additional population data.
    population_columns = [
        "Region",
        "Western Europe.1",
        "Western Offshoots.1",
        "Eastern Europe.1",
        "Latin America.1",
        "Asia (South and South-East).1",
        "Asia (East).1",
        "Middle East.1",
        "Sub-Sahara Africa.1",
        "World",
    ]
    additional_population_data = additional_data[population_columns]
    additional_population_data = additional_population_data.rename(
        columns={region: region.replace(".1", "") for region in additional_population_data.columns}
    )
    additional_population_data = additional_population_data.melt(
        id_vars="Region", var_name="country", value_name="population"
    ).rename(columns={"Region": "year"})

    # Prepare additional GDP data.
    gdp_columns = [
        "Region",
        "Western Europe",
        "Eastern Europe",
        "Western Offshoots",
        "Latin America",
        "Asia (East)",
        "Asia (South and South-East)",
        "Middle East",
        "Sub-Sahara Africa",
        "World GDP pc",
    ]
    additional_gdp_data = additional_data[gdp_columns].rename(columns={"World GDP pc": "World"})
    additional_gdp_data = additional_gdp_data.melt(
        id_vars="Region", var_name="country", value_name=GDP_PER_CAPITA_COLUMN
    ).rename(columns={"Region": "year"})

    # Merge additional population and GDP data.
    additional_combined_data = pr.merge(
        additional_population_data,
        additional_gdp_data,
        on=["year", "country"],
        how="inner",
    )
    # Convert units.
    additional_combined_data["population"] = additional_combined_data["population"] * 1000

    # Create column for GDP.
    additional_combined_data[GDP_COLUMN] = (
        additional_combined_data[GDP_PER_CAPITA_COLUMN] * additional_combined_data["population"]
    )

    assert len(additional_combined_data) == len(additional_population_data)
    assert len(additional_combined_data) == len(additional_gdp_data)

    return additional_combined_data

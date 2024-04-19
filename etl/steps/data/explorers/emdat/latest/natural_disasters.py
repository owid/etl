"""Natural disasters explorer data step.

Loads the latest EM-DAT natural_disasters data from garden and stores a table (as a csv file) for yearly data, and
another for decadal data.

NOTES:
* Some of the columns in the output files are not used by the explorer (but they appear in the "Sort by" dropdown menu),
  consider removing them. For now, we'll ensure all of the old columns are present, to avoid any possible issues.
* Most charts in the explorer are generated from the data in the files, but 3 of them are directly linked to grapher
  charts, namely:
  "All disasters (by type) - Deaths - Decadal average - false"
  "All disasters (by type) - Deaths - Decadal average - true"
  "All disasters (by type) - Economic damages (% GDP) - Decadal average - false"
  At some point it would be good to let the explorer take all the data from files.

"""

from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

paths = PathFinder(__file__)

# Mapping of old to new disaster type names.
DISASTER_TYPE_RENAMING = {
    "all_disasters": "all_disasters",
    "drought": "drought",
    "earthquake": "earthquake",
    "extreme_temperature": "temperature",
    "flood": "flood",
    "fog": "fog",
    "glacial_lake_outburst": "glacial_lake",
    "landslide": "landslide",
    "dry_mass_movement": "mass_movement",
    "extreme_weather": "storm",
    "volcanic_activity": "volcanic",
    "wildfire": "wildfire",
}


def create_wide_tables(table: Table) -> Table:
    """Convert input table from long to wide format, and adjust column names to adjust to the old names in the files
    used by the explorer.
    """
    # Adapt disaster type names to match those in the old explorer files.
    table = table.reset_index()
    table["type"] = table.astype({"type": str})["type"].replace(DISASTER_TYPE_RENAMING)

    # Create wide table.
    table_wide = table.pivot(index=["country", "year"], columns="type", join_column_levels_with="_")

    # Rename columns to match the old names in explorer.
    table_wide = table_wide.rename(
        columns={
            column: column.replace("per_100k_people", "rate_per_100k")
            .replace("total_dead", "deaths")
            .replace("total_damages_per_gdp", "total_damages_pct_gdp")
            for column in table_wide.columns
        },
        errors="raise",
    )

    # Remove unnecessary columns.
    table_wide = table_wide[
        [
            column
            for column in table_wide.columns
            if not column.startswith(
                ("gdp_", "n_events_", "population_", "insured_damages_per_gdp", "reconstruction_costs_per_gdp_")
            )
            if column
            not in [
                "affected_rate_per_100k_glacial_lake",
                "homeless_rate_per_100k_glacial_lake",
                "total_damages_pct_gdp_fog",
            ]
        ]
    ]

    # Set an appropriate index and sort conveniently.
    table_wide = table_wide.format()

    return table_wide


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load the latest dataset from garden.
    ds_garden = paths.load_dataset("natural_disasters")

    # Read tables with yearly and decadal data.
    tb_yearly = ds_garden["natural_disasters_yearly"]
    tb_decadal = ds_garden["natural_disasters_decadal"]

    #
    # Process data.
    #
    # Create wide tables adapted to the old format in explorers.
    tb_yearly_wide = create_wide_tables(table=tb_yearly)
    tb_decadal_wide = create_wide_tables(table=tb_decadal)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset and add dataset metadata.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb_yearly_wide, tb_decadal_wide],
        default_metadata=ds_garden.metadata,
        check_variables_metadata=True,
        formats=["csv"],
    )
    ds_grapher.save()

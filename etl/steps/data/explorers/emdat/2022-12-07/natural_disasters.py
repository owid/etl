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

from copy import deepcopy

from owid import catalog

from etl.helpers import PathFinder
from etl.paths import DATA_DIR

N = PathFinder(__file__)

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


def create_wide_tables(table: catalog.Table) -> catalog.Table:
    """Convert input table from long to wide format, and adjust column names to adjust to the old names in the files
    used by the explorer.
    """
    # Adapt disaster type names to match those in the old explorer files.
    table = table.reset_index()
    table["type"] = table["type"].replace(DISASTER_TYPE_RENAMING)

    # Create wide dataframes.
    table_wide = table.pivot(index=["country", "year"], columns="type")

    # Flatten column indexes and rename columns to match the old names in explorer.
    table_wide.columns = [
        f"{column}_{subcolumn}".replace("per_100k_people", "rate_per_100k")
        .replace("total_dead", "deaths")
        .replace("total_damages_per_gdp", "total_damages_pct_gdp")
        for column, subcolumn in table_wide.columns
    ]

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

    # Adapt table to the format for explorer files.
    table_wide = table_wide.reset_index()

    return table_wide


def run(dest_dir: str) -> None:
    # Load the latest dataset from garden.
    dataset_garden_latest_dir = sorted((DATA_DIR / "garden" / "emdat").glob("*/natural_disasters"))[-1]
    dataset_garden = catalog.Dataset(dataset_garden_latest_dir)

    # Load tables with yearly and decadal data.
    table_yearly = dataset_garden["natural_disasters_yearly"]
    table_decade = dataset_garden["natural_disasters_decadal"]

    # Create wide tables adapted to the old format in explorers.
    table_yearly_wide = create_wide_tables(table=table_yearly)
    table_decade_wide = create_wide_tables(table=table_decade)

    # Initialize a new grapher dataset and add dataset metadata.
    dataset = catalog.Dataset.create_empty(dest_dir)
    dataset.metadata = deepcopy(dataset_garden.metadata)
    dataset.metadata.version = N.version
    dataset.save()

    # Add tables to dataset. Force publication in csv.
    dataset.add(table_yearly_wide, formats=["csv"])
    dataset.add(table_decade_wide, formats=["csv"])

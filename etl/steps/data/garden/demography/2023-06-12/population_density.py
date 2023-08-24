"""Build population density OMM dataset.

This dataset is built using our population OMM dataset and the land area given by FAOSTAT (RL):

    `population_density = population / land_area`
"""

from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("population_density: start")

    #
    # Load inputs.
    #
    # Load dependency datasets.
    ds_population: Dataset = paths.load_dependency("population")
    ds_land_area: Dataset = paths.load_dependency("faostat_rl")

    # Read relevant tables
    tb_population = ds_population["population_original"].reset_index()
    tb_land_area = ds_land_area["faostat_rl_flat"].reset_index()

    #
    # Process data.
    #
    tb = make_table(tb_population, tb_land_area)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb],
        default_metadata=ds_population.metadata,
    )

    # Additional descriptions.
    tb = ds_garden["population_density"]
    tb.population_density.metadata.description += "\n\n" + tb_population.population.metadata.description
    ds_garden.metadata.description += "\n\n" + ds_population.metadata.description
    ds_garden.add(tb)

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("population_density: end")


def make_table(tb_population: Table, tb_land_area: Table) -> Table:
    """Create a table with population density data."""
    # We use land area of countries as they are defined today (latest reported value)
    log.info("population_density: process land area datafame")
    column_area = "land_area__00006601__area__005110__hectares"
    tb_land_area = (
        tb_land_area.loc[:, [column_area, "country", "year"]]
        .rename(columns={column_area: "area"})
        .sort_values(["country", "year"])
        .drop_duplicates(subset=["country"], keep="last")
        .drop(columns=["year"])
    )

    # Merge dataframes
    log.info("population_density: merge dataframes")
    tb = tb_population.merge(tb_land_area, on="country", how="inner")
    # Drop NaN (no data for area)
    tb = tb.dropna(subset=["area"])
    # Estimate population density as population / land_area(in km2)
    tb["population_density"] = tb["population"] / (0.01 * tb["area"])  # 0.01 to convert from hectares to km2
    # Rename column source -> source_population
    tb = tb.rename(columns={"source": "source_population"})
    # Select relevant columns, order them, set index
    tb = tb[["country", "year", "population_density", "source_population"]].set_index(["country", "year"]).sort_index()

    # Build table
    log.info("population_density: build table")
    tb.metadata.short_name = paths.short_name
    return tb

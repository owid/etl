"""Build population density OMM dataset.

This dataset is built using our population OMM dataset and the land area given by FAOSTAT (RL):

    `population_density = population / land_area`
"""

import pandas as pd
from owid.catalog import Dataset, DatasetMeta, Table, VariableMeta
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
    tb_population = ds_population["population"]
    tb_land_area = ds_land_area["faostat_rl_flat"]

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
        default_metadata=build_metadata(ds_population, ds_land_area),
    )
    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("population_density: end")


def make_table(tb_population: Table, tb_land_area: Table) -> Table:
    """Create a table with population density data."""
    # Dataframe population
    df_population = pd.DataFrame(tb_population).reset_index()
    # Dataframe land area
    # We use land area of countries as they are defined today (latest reported value)
    log.info("population_density: process land area datafame")
    column_area = "land_area__00006601__area__005110__hectares"
    df_land_area = (
        pd.DataFrame(tb_land_area)[[column_area]]
        .reset_index()
        .rename(columns={column_area: "area"})
        .sort_values(["country", "year"])
        .drop_duplicates(subset=["country"], keep="last")
        .drop(columns=["year"])
    )

    # Merge dataframes
    log.info("population_density: merge dataframes")
    df = df_population.merge(df_land_area, on="country", how="inner")
    # Drop NaN (no data for area)
    df = df.dropna(subset=["area"])
    # Estimate population density as population / land_area(in km2)
    df["population_density"] = df["population"] / (0.01 * df["area"])  # 0.01 to convert from hectares to km2
    # Rename column source -> source_population
    df = df.rename(columns={"source": "source_population"})
    # Select relevant columns, order them, set index
    df = df[["country", "year", "population_density", "source_population"]].set_index(["country", "year"]).sort_index()

    # Build table
    log.info("population_density: build table")
    tb = Table(df, short_name=paths.short_name)

    # Define variable metadata
    log.info("population_density: define variable metadata")
    tb.population_density.metadata = VariableMeta(
        title="Population density",
        description=(
            "Population density estimated by Our World in Data using population estimates from multiple sources "
            "and land area estimates by the Food and Agriculture Organization of the United Nations. We obtain it"
            "by dividing the population estimates by the land area estimates.\n\n"
            + tb_population.population.metadata.description
        ),
        unit="people per km²",
    )
    tb.source_population.metadata = VariableMeta(
        title="Source (population)",
        description=(
            "Name of the source of the population estimate for a specific data point (country-year). The name includes a short name of the source and a link."
        ),
        unit="",
    )
    return tb


def build_metadata(ds_population: Dataset, ds_land_area: Dataset) -> DatasetMeta:
    """Generate metadata for the dataset based on the metadata from `ds_population` and `ds_land_area`.

    Parameters
    ----------
    ds_population : Dataset
        Dataset with population estimates.
    ds_land_area : Dataset
        Dataset with land area estimates.

    Returns
    -------
    DatasetMeta
        Dataset metadata.
    """
    log.info("population_density: add metadata")
    return DatasetMeta(
        channel=paths.channel,
        namespace=paths.namespace,
        short_name=paths.short_name,
        title="Population density (various sources, 2023.1)",
        description=(
            "Population density is obtained by dividing population by land area.\n\n"
            + ds_population.metadata.description
        ),
        sources=ds_population.metadata.sources + ds_land_area.metadata.sources,
        licenses=ds_population.metadata.licenses + ds_land_area.metadata.licenses,
    )

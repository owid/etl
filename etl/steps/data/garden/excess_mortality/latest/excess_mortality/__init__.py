"""Load a meadow dataset and create a garden dataset.

This code builds the consolidated Excess Mortality dataset by Our World in Data. To this end, we two datasets: Human Mortality Database (HMD) Short-term Mortality Fluctuations project and the World Mortality Dataset (WMD).
Both sources are updated weekly.

This step merges the two datasets into one single dataset, combining metrics from both sources to obtain excess mortality metrics.

"""
from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

from .input import build_df
from .process import process_df

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("excess_mortality: start")

    #
    # Load inputs.
    #
    # Load dependency datasets.
    ds_hmd = paths.load_dataset("hmd_stmf")
    ds_wmd = paths.load_dataset("wmd")
    ds_kobak = paths.load_dataset("xm_karlinsky_kobak")

    # Build initial dataframe
    df = build_df(ds_hmd, ds_wmd, ds_kobak)

    #
    # Process data.
    #
    log.info("excess_mortality: processing data")
    df = process_df(df)
    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=paths.short_name)

    # Create dataset
    ds_garden = create_dataset(dest_dir, tables=[tb_garden.set_index(["entity", "date"])])

    # Add all sources from dependencies to dataset
    ds_garden.metadata.sources = ds_hmd.metadata.sources + ds_wmd.metadata.sources + ds_kobak.metadata.sources

    # Source in YAML file only have name, load them from dataset.
    tb_garden = ds_garden[paths.short_name]  # need to reload it to get updated metadata
    source_by_name = {source.name: source for source in ds_garden.metadata.sources}
    for col in tb_garden.columns:
        tb_garden[col].metadata.sources = [source_by_name[source.name] for source in tb_garden[col].metadata.sources]

    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("excess_mortality: end")

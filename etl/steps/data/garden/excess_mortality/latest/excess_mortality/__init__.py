"""Load a meadow dataset and create a garden dataset.

NOTE: If checking for issues with data not being up-to-date, please check the HMD dataset (https://mortality.org/Data/STMF), which is the only one that is still being updated since late 2024.


This code builds the consolidated Excess Mortality dataset by Our World in Data. To this end, we three datasets:

    - Human Mortality Database (HMD) Short-term Mortality Fluctuations project: https://mortality.org/Data/STMF
        UPDATE policy: Currently being updated, check URL above for latest updates.

    - World Mortality Dataset (WMD): https://github.com/akarlinsky/world_mortality/
        UPDATE policy: No more updates from 2024
            "We will not provide any excess mortality estimates starting from 2024. Our excess mortality calculations are based on linear extrapolation of 2015--2019 trends, and this becomes more and more tenuous as the years go by. We provide and update data for 2015--2024, but excess mortality estimates will only be avilable up to end of 2023."


    - Karlinsky and Kobak (2021) dataset: https://github.com/dkobak/excess-mortality
        UPDATE policy: No more updates from 2024
            "We will not provide any excess mortality estimates starting from 2024. Our excess mortality calculations are based on linear extrapolation of 2015--2019 trends, and this becomes more and more tenuous as the years go by. The repository will keep being updated together with the World Mortality Dataset but will only show excess until the end of 2023."
"""

from input import build_df
from owid.catalog import Table
from process import process_df
from structlog import get_logger

from etl.helpers import PathFinder
from etl.steps.data.garden.covid.latest.shared import add_last12m_to_metric

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
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

    # HOTFIX: Remove age groups for countries Australia and Canada.
    # See full extent of reasons for it in https://owid.slack.com/archives/CV5RY8F1B/p1706099289965929
    # TL;DR: HMD is creating their standard age groups for these countries from an input data that has
    # different age group binning (bigger bins). This results into unaccurate restimates.
    df = df[~((df["entity"].isin(["Australia", "Canada"])) & (df["age"] != "all_ages"))]

    # Actual processing
    log.info("excess_mortality: processing data")
    df = process_df(df)
    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=paths.short_name)

    # Last 12 months
    tb_garden = add_last12m_to_metric(tb_garden, "cum_excess_proj_all_ages", column_country="entity")
    tb_garden = add_last12m_to_metric(tb_garden, "cum_excess_per_million_proj_all_ages", column_country="entity")

    # Format
    tb_garden = tb_garden.format(["entity", "date"])

    # Create dataset
    ds_garden = paths.create_dataset(
        tables=[tb_garden],
        formats=["csv", "feather"],
    )

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

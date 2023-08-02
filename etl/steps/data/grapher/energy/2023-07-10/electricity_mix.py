"""Grapher step for the Electricity Mix (Energy Institute & Ember) dataset.

"""

from owid.catalog import Dataset

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load garden dataset and read its main table.
    ds_garden: Dataset = paths.load_dependency("electricity_mix")
    tb_garden = ds_garden["electricity_mix"]

    #
    # Process data.
    #
    # Drop unnecessary columns.
    tb = tb_garden.drop(columns=["population"])

    # NOTE: Chart [Electricity production by source](https://ourworldindata.org/grapher/electricity-prod-source-stacked)
    # has a timeline starting in 1990 (instead of 1985) because the data is incomplete for many countries between 1985
    # and 1990. If EI data changes in the future and is more complete in that range, remove the timeline restriction.

    #
    # Save outputs.
    #
    ds_grapher = create_dataset(
        dest_dir=dest_dir, tables=[tb], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )
    ds_grapher.save()

"""Simplified countries-regions dataset. It is published as CSV so that it can be easily loaded in the notebooks repos.

"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load simplified regions dataset and read its main table.
    ds_garden = paths.load_dataset("regions")
    tb = ds_garden["regions"].reset_index()

    #
    # Process data.
    #
    # Drop unnecessary columns.
    tb = tb.drop(columns=["year"])

    #
    # Save outputs.
    #
    # Create a new dataset in a convenient csv format.
    ds_grapher = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, formats=["csv"])
    ds_grapher.save()

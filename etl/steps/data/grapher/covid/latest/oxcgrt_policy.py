"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

from .shared import to_grapher_date

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("oxcgrt_policy")

    # Read table from garden dataset.
    tb = ds_garden["oxcgrt_policy"]
    tb_counts = ds_garden["country_counts"]

    #
    # Process data.
    #
    tb = to_grapher_date(tb, "2020-01-01")

    #
    # Save outputs.
    #
    tables = [
        tb,
        tb_counts,
    ]
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()

"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("antimicrobial_usage")

    # Read table from garden dataset.
    tb_class = ds_garden.read("class", reset_index=False)
    tb_aware = ds_garden.read("aware", reset_index=False)
    tb_class_agg = ds_garden.read("class_aggregated", reset_index=False)

    #
    # Process data.
    #

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(
        tables=[tb_class, tb_aware, tb_class_agg],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()

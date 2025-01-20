"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("vaccine_stock_out")

    # Read table from garden dataset.
    tb = ds_garden.read("vaccine_stock_out", reset_index=False)
    tb_agg = ds_garden.read("derived_metrics", reset_index=False)
    tb_cause = ds_garden.read("reason_for_stockout", reset_index=False)
    tb_global = ds_garden.read("global_stockout", reset_index=False)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb, tb_agg, tb_cause, tb_global],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()

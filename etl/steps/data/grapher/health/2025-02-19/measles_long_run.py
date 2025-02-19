"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("measles_long_run")

    # Read table from garden dataset.
    tb = ds_garden.read("measles_long_run", reset_index=True)

    # Create two tables, one for the main data and one for the incomplete years data.
    tb_incomplete = tb[tb["year"] >= tb["year"].max() - 1]
    tb = tb[tb["year"] < tb["year"].max() - 1]

    tb_incomplete = tb_incomplete.format(["country", "year"], short_name="measles_incomplete")
    tb = tb.format(["country", "year"], short_name="measles_long_run")
    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb, tb_incomplete], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()

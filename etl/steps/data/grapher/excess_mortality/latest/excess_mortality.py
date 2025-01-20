"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("excess_mortality")

    # Read table from garden dataset.
    tb = ds_garden["excess_mortality"]

    #
    # Process data.
    #
    # Make grapher friendly
    # tb_garden = make_grapher_friendly(tb_garden)
    tb = tb.rename_index_names({"entity": "country"})

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )
    ds_grapher.save()

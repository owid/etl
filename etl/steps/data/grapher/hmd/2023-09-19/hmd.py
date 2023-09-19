"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("hmd")

    # Read table from garden dataset.
    tb = ds_garden["hmd"]

    #
    # Process data.
    #
    ## Filter indicators
    ## Keep only those with observation period of 1 year (i.e. format = 1x1, 5x1)
    column_index = list(tb.index.names)
    tb = tb.reset_index()
    tb = tb[tb["format"].str.fullmatch(r"\d+x1")]
    ## Set dtype of year to int
    tb["year"] = tb["year"].astype("Int64")
    # Set index back
    tb = tb.set_index(column_index, verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()

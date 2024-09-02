"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("ucdp_prio")

    # Read table from garden dataset.
    tb = ds_garden["ucdp_prio"]
    # tb_country = ds_garden["ucdp_prio_country"]

    #
    # Process data.
    #
    tb = tb.rename_index_names({"region": "country"})

    #
    # Save outputs.
    #
    tables = [
        tb,
        # tb_country,
    ]
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()

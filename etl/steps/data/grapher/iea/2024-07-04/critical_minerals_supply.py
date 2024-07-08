"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("critical_minerals")

    # Read tables for supply of key minerals.
    tb_supply = ds_garden["supply_for_key_minerals"].reset_index()

    #
    # Process data.
    #
    # For supply, there are no scenarios, and only "Best case".
    tb_supply = tb_supply.drop(columns=["case"])

    # Transform table from long to wide format.
    tb_supply = tb_supply.pivot(index=["country", "year"], columns=["mineral", "process"], join_column_levels_with="_")

    # Improve column names.
    tb_supply = tb_supply.rename(
        columns={column: column.replace("supply_", "").replace("__", "_") for column in tb_supply.underscore().columns}
    )

    # Format conveniently.
    tb_supply = tb_supply.format(keys=["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb_supply], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()

"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("global_warming_potential_factors")
    tb = ds_meadow["global_warming_potential_factors"].reset_index()

    #
    # Process data.
    #
    # Combine the name and the formula of each greenhouse gas.
    tb = tb.astype({"name": str, "formula": str})
    tb["greenhouse_gas"] = tb["name"] + " (" + tb["formula"] + ")"
    tb = tb.drop(columns=["name", "formula"])

    # Set an appropriate index and sort conveniently.
    tb = tb.set_index(["greenhouse_gas"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )
    ds_garden.save()

"""Load a meadow dataset and create a garden dataset."""

from owid.datautils.dataframes import map_series

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
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

    # Rename to use American spelling.
    tb["greenhouse_gas"] = map_series(
        series=tb["greenhouse_gas"],
        mapping={"Sulphur hexafluoride (SF₆)": "Sulfur hexafluoride (SF₆)"},
        warn_on_unused_mappings=True,
        warn_on_missing_mappings=False,
    )

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["greenhouse_gas"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()

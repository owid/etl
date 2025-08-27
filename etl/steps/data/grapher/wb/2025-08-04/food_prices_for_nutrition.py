"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("food_prices_for_nutrition")
    tb = ds_garden.read("food_prices_for_nutrition", reset_index=False)

    ####################################################################################################################
    # TODO: The costs in the original data was given in local currency unit, and in current PPP$. In the garden step, we added costs in constant PPP$ (and removed the others, which were not used). Since columns in grapher were originally named "*_in_ppp_dollars" (and they referred to constant PPP dollars) rename them to avoid issues. Once merged, consider keeping the garden names, for clarity.
    tb = tb.rename(
        columns={column: column.replace("in_constant_ppp_dollars", "in_ppp_dollars") for column in tb.columns},
        errors="raise",
    )
    ####################################################################################################################

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb])
    ds_grapher.save()

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

    # The costs in the original data was given in local currency unit, and in current PPP$.
    # In the garden step, we added costs in constant PPP$.
    # For now, since we are only using costs in constant PPP$, remove other cost columns.
    columns_to_drop = [
        column
        for column in tb.columns
        if (("in_local_currency_unit" in column) or ("in_current_ppp_dollars" in column))
    ]
    tb = tb.drop(columns=columns_to_drop, errors="raise")
    # TODO: Since columns in grapher were named "*_in_ppp_dollars" (and they referred to constant PPP dollars) rename them to avoid issues. Once merged, consider keeping the garden names, for clarity.
    tb = tb.rename(columns={column: column.replace("in_constant_ppp_dollars", "in_ppp_dollars") for column in tb.columns}, errors="raise")

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb])
    ds_grapher.save()

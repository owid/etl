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

    # Remove columns in local currency.
    # TODO: Consider adding indicators in local currency units to garden metadata. For now, since they are not used, remove them.
    tb = tb[[column for column in tb.columns if "in_local_currency_unit" not in column]]

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb])
    ds_grapher.save()

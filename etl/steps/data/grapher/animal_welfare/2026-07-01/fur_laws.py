"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Get current year from this step's version.
CURRENT_YEAR = int(paths.version.split("-")[0])


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("fur_laws")
    tb = ds_garden.read("fur_laws")

    #
    # Process data.
    #
    # There is a new category, "NO DATA".
    # For better visualization, we will replace them with nan.
    tb = tb.astype({"fur_farming_status": "string", "fur_trading_status": "string", "fur_farms_active": "string"})
    for column in ["fur_farming_status", "fur_trading_status", "fur_farms_active"]:
        tb.loc[tb[column] == "NO DATA", column] = None

    # Format table conveniently.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], check_variables_metadata=True)
    ds_grapher.save()

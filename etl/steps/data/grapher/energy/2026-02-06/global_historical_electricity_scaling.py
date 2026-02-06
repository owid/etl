"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("global_historical_electricity_scaling")

    # Read table from garden dataset.
    tb = ds_garden.read("global_historical_electricity_scaling")

    #
    # Process data.
    #
    # Select and rename columns conveniently.
    tb = tb.rename(columns={"years_since_100_twh": "year"}, errors="raise")[
        ["country", "year"] + [column for column in tb.columns if "production" in column]
    ]

    # Improve table format.
    tb = tb.format()

    ####################################################################################################################
    # Fill out required metadata.
    # TODO: Fill out yaml meta files properly instead of this.
    for column in tb.columns:
        if column == "total_production":
            title = "Total electricity production"
        else:
            title = f"Electricity production by {column.replace('_production', '').replace('_', ' ')}"
        tb[column].metadata.title = title
        tb[column].metadata.unit = "terawatt-hours"
        tb[column].metadata.short_unit = "TWh"
    ####################################################################################################################

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()

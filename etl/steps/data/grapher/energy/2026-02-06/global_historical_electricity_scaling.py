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

    # For convenience, make a "country" column for sources.
    tb = tb.rename(
        columns={
            column: column.replace("_production", "").replace("_", " ").capitalize()
            for column in tb.columns
            if column not in ["country", "year"]
        },
        errors="raise",
    )
    tb = tb.drop(columns=["country"]).melt(id_vars=["year"], var_name="country", value_name="production")

    # Improve table format.
    tb = tb.format()

    ####################################################################################################################
    # Fill out required metadata.
    # TODO: Fill out yaml meta files properly instead of this.
    tb["production"].metadata.title = "Global electricity production"
    ####################################################################################################################

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()

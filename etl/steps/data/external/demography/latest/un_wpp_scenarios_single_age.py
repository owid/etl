from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("un_wpp")
    #
    # Process data.
    #
    tb = ds_garden["population"]

    ds_garden = paths.create_dataset(
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
        formats=["csv"],
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

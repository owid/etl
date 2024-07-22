"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("population_doubling_times")

    # Read table from garden dataset.
    tb = ds_garden["population_doubling_times"].reset_index()

    #
    # Process data.
    #

    # Change entity
    # Here we change entities from countries to year transition (e.g. "0.5B to 1B").
    # We also change the metrics, to be the countries.
    # We do this in order to be able to plot certain charts on Grapher.
    tb["country"] = (
        (tb["population_target"] / 2).apply(prettify_billion)
        + " to "
        + (tb["population_target"]).apply(prettify_billion)
    )
    # Filter columns, rename columns, set index
    tb = tb.loc[:, ["country", "year", "num_years_to_double"]]
    tb = tb.rename(columns={"num_years_to_double": "World"})
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], default_metadata=ds_garden.metadata)

    # Temporary fix
    ds_grapher["population_doubling_times"].rename(columns={"world": "World"})

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def prettify_billion(num: int) -> str:
    """Convert a large (in the billions) number to a more readable format.

    Example: 1000000000 -> 1B
    """
    # Sanity check
    if num < 1e8:
        raise ValueError(f"Number is too low: {num}")
    elif num >= 1e12:
        raise ValueError(f"Number is too high: {num}")

    # Prettify
    num_ = num / 1e9
    if num_.is_integer():
        return f"{int(num_)}B"
    else:
        return f"{num_}B"

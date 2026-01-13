"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("acceptability_of_us_farming_practices")

    # Read table from garden dataset.
    tb = ds_garden.read("acceptability_of_us_farming_practices")

    #
    # Process data.
    #
    # Adapt to grapher format.
    # In grapher, we use "country" as the entity dimension (here it represents questions).
    # The garden table has "question" in the index, so reset it to make it a column.
    tb = tb.reset_index()

    # Drop the spurious 'index' column if it exists (from reset_index on RangeIndex).
    if "index" in tb.columns:
        tb = tb.drop(columns=["index"])

    # Rename question to country for grapher.
    tb = tb.rename(columns={"question": "country"})

    # Add a year column (take it from the origin's publication date).
    # Use any column to get metadata (they all have the same origins).
    tb["year"] = int(tb["very_unacceptable"].metadata.origins[0].date_published.split("-")[0])

    # Set index for grapher format (country and year).
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Initialize a new grapher dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save grapher dataset.
    ds_grapher.save()

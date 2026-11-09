"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset()

    # Read table from garden dataset.
    tb = ds_garden["press_freedom__freedom_house__2017"]

    #
    # Process data.
    #
    # Store the press freedom status codes as ordered text categories, so the Swimlane chart can show them.
    # NOTE: This is a frozen 2017 release; the codes follow Freedom House's statuses (0 not free, 1 partly free, 2 free).
    labels = {0: "Not free", 1: "Partly free", 2: "Free"}
    unknown = set(tb["freedom_of_the_press"].dropna().unique()) - set(labels)
    assert not unknown, f"`freedom_of_the_press` has codes without a label: {unknown}"
    metadata = tb["freedom_of_the_press"].metadata
    tb["freedom_of_the_press"] = tb["freedom_of_the_press"].map(labels).astype("category")
    tb["freedom_of_the_press"].metadata = metadata
    tb["freedom_of_the_press"].metadata.type = "ordinal"
    tb["freedom_of_the_press"].metadata.sort = list(labels.values())

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata)

    # Save changes in the new grapher dataset.
    ds_grapher.save()

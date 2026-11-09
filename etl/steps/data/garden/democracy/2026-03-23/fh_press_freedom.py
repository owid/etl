"""Load the Freedom of the Press snapshot (Freedom House, 2017) and create a garden dataset.

Freedom House discontinued its Freedom of the Press report in 2017, so this dataset is frozen. It lives next to the
Freedom House (Freedom in the World) steps.
"""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Freedom House's press freedom statuses, in order. They are stored as text, so the Swimlane chart can show them.
# NOTE: The snapshot codes them 0 to 2; the recoding fails on any other code.
STATUS_LABELS = {0: "Not free", 1: "Partly free", 2: "Free"}


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("press_freedom__freedom_house__2017.feather")
    tb = snap.read(safe_types=False).rename(columns={"Freedom of the Press": "freedom_of_the_press"})

    #
    # Process data.
    #
    # Store the status codes as ordered text categories.
    tb = codes_to_ordinal(tb, "freedom_of_the_press", STATUS_LABELS)

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_garden.save()


def codes_to_ordinal(tb: Table, column: str, labels: dict[int, str]) -> Table:
    """Replace the codes of a categorical indicator with their labels, as an ordinal indicator sorted like `labels`."""
    unknown = set(tb[column].dropna().unique()) - set(labels)
    assert not unknown, f"Column `{column}` has codes without a label: {unknown}"
    metadata = tb[column].metadata
    tb[column] = tb[column].map(labels).astype("category")
    tb[column].metadata = metadata
    tb[column].metadata.type = "ordinal"
    tb[column].metadata.sort = list(labels.values())
    return tb

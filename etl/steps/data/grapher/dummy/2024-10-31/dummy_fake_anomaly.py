"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    snap = paths.load_snapshot("dummy_fake_anomaly")
    tb = snap.read().assign(**{"country": "World"}).rename(columns={"old_value": "Old data", "new_value": "New data"})

    for column in ["Old data", "New data"]:
        tb[column].metadata.title = column
        tb[column].metadata.unit = "arbitrary unit"
        tb[column].metadata.short_unit = "au"

    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_grapher.save()

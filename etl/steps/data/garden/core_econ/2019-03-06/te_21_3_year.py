"""Load snapshot and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load data from snapshot.
    #
    snap = paths.load_snapshot()
    tb = snap.read(safe_types=False)
    # The variable in this legacy dataset is called "Year" (column `year`), which
    # collides with the standard `year` index expected by grapher. Rename the
    # value column and synthesise a `year` index from it (they hold the same number).
    tb = tb.rename(columns={"year": "year_value"})
    tb["year"] = tb["year_value"]
    # The underlying MySQL dataset is empty (no data_values); grapher refuses to
    # upload an empty table, so add a placeholder row purely to satisfy the
    # catalogPath-consistency migration.
    if tb.empty:
        tb["country"] = tb["country"].astype("object")
        tb.loc[0] = {"country": "World", "year": 0, "year_value": 0}
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def sanity_check_outputs(tb) -> None:
    # TEMPORARY: deliberate failure, to check what a failed step looks like in the Buildkite log.
    # Reverted before this PR is merged.
    assert tb["design_patents"].max() < 0, "Deliberate failure in us_patents to exercise the failure recap."


def run() -> None:
    ds_meadow = paths.load_dataset("us_patents")
    tb = ds_meadow.read("us_patents", reset_index=False)

    sanity_check_outputs(tb)

    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)
    ds_garden.save()

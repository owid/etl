"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    ## Attention (via tags)
    snap = paths.load_snapshot("guardian_mentions.csv")
    ## Load data from snapshot.
    tb_tags = snap.read(safe_types=False)
    ## Attention (via mentions)
    snap = paths.load_snapshot("guardian_mentions_raw.csv")
    ## Load data from snapshot.
    tb_mentions = snap.read(safe_types=False)

    #
    # Process data.
    #
    # Merge both tables (tags, mentions)
    tb = tb_tags.merge(tb_mentions, on=["country", "year"], how="outer", suffixes=("_tags", "_mentions"))

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb],
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


{% if (_cookiecutter.snapshot_names_with_extension | length) == 1 %}
def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("{{_cookiecutter.snapshot_names_with_extension[0]}}")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [
        tb.format(["country", "year"])
    ]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()

{%- else %}
def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    snapshot_names = {{_cookiecutter.snapshot_names_with_extension}}
    tables = []
    for snapshot_name in snapshot_names:
        # Retrieve snapshot.
        snap = paths.load_snapshot(snapshot_name)

        # Load data from snapshot.
        tb = snap.read()

        #
        # Process data.
        #
        # TODO

        # Format
        tb = tb.format(["country", "year"])

        # Append to main list
        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=tables)

    # Save changes in the new meadow dataset.
    ds_meadow.save()
{%- endif -%}

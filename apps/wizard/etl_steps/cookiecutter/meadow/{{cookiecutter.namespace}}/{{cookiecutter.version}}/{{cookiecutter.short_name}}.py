"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


{% if (_cookiecutter.snapshot_names_with_extension | length) == 1 %}
def run() -> None:
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
    # Improve tables format.
    tables = [
        tb.format(["country", "year"])
    ]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()

{%- else %}
def run() -> None:
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
        # Improve table format.
        tb = tb.format(["country", "year"])

        # Append current table to list of tables.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables)

    # Save meadow dataset.
    ds_meadow.save()
{%- endif -%}

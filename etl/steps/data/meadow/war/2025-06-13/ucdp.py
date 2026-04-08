"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    paths.log.info("start")

    #
    # Load inputs.
    #
    # Retrieve snapshot.
    tables = []
    short_names = {
        "one_sided": {
            "index": ["conflict_id", "dyad_id", "year"],
            "filename": "OneSided_v25_1.csv",
        },
        "non_state": {
            "index": ["conflict_id", "dyad_id", "year"],
        },
        "battle_related_conflict": {
            "index": ["conflict_id", "year"],
        },
        "battle_related_dyadic": {
            "index": ["conflict_id", "dyad_id", "year"],
        },
        "ged": {
            "index": ["id"],
            "dtype": {"gwnoa": "string"},
        },
        "prio_armed_conflict": {
            "index": ["conflict_id", "year"],
        },
    }
    for short_name, props in short_names.items():
        snap = paths.load_snapshot(short_name=f"ucdp_{short_name}.zip")
        paths.log.info(f"creating table from {snap.path}")

        # Load data from snapshot.
        # Set params
        kwargs = {}
        kwarg_names = ["dtype"]
        for name in kwarg_names:
            if name in props:
                kwargs[name] = props[name]

        # Read within zip?
        if "filename" in props:
            tb = snap.read_in_archive(props["filename"], **kwargs)
        else:
            tb = snap.read_csv(**kwargs)

        # Set index
        tb = tb.format(props["index"])

        # Add table to list of tables.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata, check_variables_metadata=True)  # type: ignore

    # Save changes in the new garden dataset.
    ds_meadow.save()

    paths.log.info("end")

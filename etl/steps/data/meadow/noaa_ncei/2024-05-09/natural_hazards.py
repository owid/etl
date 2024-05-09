"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load snapshots and read their data.
    snap_earthquakes = paths.load_snapshot("natural_hazards_earthquakes")
    snap_tsunamis = paths.load_snapshot("natural_hazards_tsunamis")
    snap_volcanoes = paths.load_snapshot("natural_hazards_volcanoes")
    tb_earthquakes = snap_earthquakes.read()
    tb_tsunamis = snap_tsunamis.read()
    tb_volcanoes = snap_volcanoes.read()

    #
    # Process data.
    #
    # Format tables conveniently.
    tb_earthquakes = tb_earthquakes.format(["id"])
    # Note that there is a repeated id in the earthquakes table (id 1926, corresponding to Chile 1961).
    tb_tsunamis = tb_tsunamis.format(["id"], verify_integrity=False)
    tb_volcanoes = tb_volcanoes.format(["id"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(
        dest_dir,
        tables=[tb_earthquakes, tb_tsunamis, tb_volcanoes],
        check_variables_metadata=True,
        default_metadata=snap_earthquakes.metadata,
    )
    ds_meadow.save()

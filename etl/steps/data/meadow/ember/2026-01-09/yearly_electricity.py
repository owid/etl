"""Load snapshot of Ember's Yearly Electricity Data and create a raw data table."""

from etl.helpers import PathFinder

# Get naming conventions.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load snapshot of global electricity and read its data.
    snap_global = paths.load_snapshot("yearly_electricity__global.csv")
    tb_global = snap_global.read(safe_types=False)

    # Load snapshot of european electricity and read its data.
    snap_europe = paths.load_snapshot("yearly_electricity__europe.csv")
    tb_europe = snap_europe.read(safe_types=False)

    #
    # Process data.
    #
    # Format tables conveniently.
    tb_global = tb_global.format(keys=["area", "year", "variable", "unit"], sort_columns=True)
    tb_europe = tb_europe.format(keys=["area", "year", "variable", "unit"], sort_columns=True)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb_global, tb_europe], default_metadata=tb_global.metadata)

    # Save meadow dataset.
    ds_meadow.save()

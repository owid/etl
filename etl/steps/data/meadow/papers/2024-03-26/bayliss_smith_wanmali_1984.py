"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot and read its data.
    snap = paths.load_snapshot("bayliss_smith_wanmali_1984.csv")
    tb = snap.read()

    #
    # Process data.
    #
    # Transpose the data for convenience.
    tb = tb.melt(id_vars=["country"], var_name="year", value_name="wheat_yield", short_name="long_term_wheat_yields")

    # Set an appropriate index and sort conveniently.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""


from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load snapshot and read its data.
    tb = paths.load_snapshot("hawaii_ocean_time_series.csv").read(skiprows=8, sep="\t", na_values=[-999])

    #
    # Process data.
    #

    # Set an appropriate index and sort conveniently.
    tb = tb.format(["date"], sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()

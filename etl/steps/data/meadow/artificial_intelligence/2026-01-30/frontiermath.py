"""Load FrontierMath benchmark data from Epoch AI zip archive into meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Extract and load FrontierMath CSV from benchmark_data.zip."""
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("epoch_benchmark_data.zip")

    #
    # Process data.
    #
    # Extract frontiermath.csv from the zip file.
    with snap.extracted() as archive:
        tb = archive.read("frontiermath.csv", safe_types=False)

    tb = tb.reset_index(drop=True)

    columns = ["Model version", "mean_score", "Release date"]
    tb = tb[columns]

    tb = tb.format(["release_date", "model_version"])

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    ds_meadow.save()

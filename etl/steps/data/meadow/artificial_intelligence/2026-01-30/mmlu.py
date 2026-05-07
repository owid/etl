"""Load MMLU benchmark data from Epoch AI zip archive into meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Extract and load MMLU CSV from benchmark_data.zip."""
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("epoch_benchmark_data.zip")

    #
    # Process data.
    #
    # Extract mmlu_external.csv from the zip file.
    with snap.extracted() as archive:
        tb = archive.read("mmlu_external.csv", safe_types=False)

    tb = tb.reset_index(drop=True)

    columns = ["Model version", "EM", "Release date", "Country", "Name", "Shots"]
    tb = tb[columns]

    # Drop rows with missing key columns.
    tb = tb.dropna(subset=["Model version", "Release date", "Country"])

    # Deduplicate by (model_version, release_date), keeping row with max EM.
    # Where the same model version has multiple entries (different sources / names),
    # keep the one with the highest reported score; also keep its Name.
    tb = tb.sort_values("EM", ascending=False).drop_duplicates(subset=["Model version", "Release date"])

    tb = tb.format(["release_date", "model_version"], short_name="mmlu")

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)
    ds_meadow.save()

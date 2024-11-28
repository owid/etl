"""Load a snapshot and create a meadow dataset."""
import gzip
import json

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("emissions_by_sector.gz")

    # Load data from snapshot.
    with gzip.open(snap.path) as _file:
        data = json.loads(_file.read())

    # Create table with data and metadata.
    tb = snap.read_from_dict(data, underscore=True)

    #
    # Process data.
    #
    # Extract data from column "emissions", which is given as a list of dictionaries with year and value.
    tb = tb.explode("emissions").reset_index(drop=True)

    # Extract data for year and values, and add the original metadata to the newly created columns.
    for column in ["year", "value"]:
        tb[column] = [emissions[column] for emissions in tb["emissions"]]
        tb[column] = tb[column].copy_metadata(tb["emissions"])

    # Drop unnecessary columns.
    tb = tb.drop(columns="emissions", errors="raise")

    # Improve table format.
    tb = tb.format(["country", "year", "gas", "sector", "data_source"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_meadow.save()

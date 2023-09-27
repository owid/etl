"""Load a snapshot and create a meadow dataset."""

import tempfile
import zipfile
from pathlib import Path

import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("general_files.zip")

    # Unzip to temp directory
    with tempfile.TemporaryDirectory() as temp_dir:
        zipfile.ZipFile(snap.path).extractall(temp_dir)

        code_path = Path(temp_dir) / "general_files" / "HYDE_country_codes.xlsx"
        codes = pd.read_excel(code_path.as_posix(), sheet_name="country", usecols="A:B").rename(
            columns={"ISO-CODE": "country_code", "Country": "country"}
        )

    codes["country"] = codes["country"].str.strip()
    codes = codes.drop_duplicates(subset="country_code", keep="first")
    codes.set_index("country_code", inplace=True)

    tb = Table(codes, short_name="country_codes")

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)
    ds.save()

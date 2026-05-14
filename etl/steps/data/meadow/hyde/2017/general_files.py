"""Load a snapshot and create a meadow dataset."""

import tempfile
import zipfile
from pathlib import Path

from owid.catalog import processing as pr

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
        # country_codes is just an ISO-code → country-name reference table — keep it
        # origin-less so the HYDE/PBL origin doesn't leak into every downstream chain
        # that merges on country (e.g. demography/.../population).
        codes = pr.read_excel(
            code_path.as_posix(),
            sheet_name="country",
            usecols="A:B",
        ).rename(columns={"ISO-CODE": "country_code", "Country": "country"})

    codes["country"] = codes["country"].str.strip()
    codes = codes.drop_duplicates(subset="country_code", keep="first")
    codes = codes.set_index("country_code")
    codes.metadata.short_name = "country_codes"

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds = create_dataset(dest_dir, tables=[codes], default_metadata=snap.metadata)
    ds.save()

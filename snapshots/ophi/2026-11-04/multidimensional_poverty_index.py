"""Script to create a snapshot of the Global Multidimensional Poverty Index (MPI) dataset.

OPHI provides the public-use files by email, as a zip archive (`OWID.zip`) with the current margin estimates
(`GMPI<year>_puf.csv`) and the harmonized over time estimates (`GMPI_HOT_<year>_puf.csv`), plus Stata copies of both
and a license file. Run it with the path to the zip:

    etls ophi/<version>/multidimensional_poverty_index --path-to-file ~/Downloads/OWID.zip

NOTE: Until 2025, OPHI shared the files through a public folder on its cloud server
(https://cloud-ophi.qeh.ox.ac.uk/index.php/s/nmrnMs2oaLNDKx7). In 2026 the server was unreachable and the files came
by email. Check whether the folder is back online before the next update: if it is, the snapshots can go back to
`url_download` in the `.dvc` files.
"""

import re
import tempfile
import zipfile
from pathlib import Path

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# Map each snapshot variant to the pattern of its file name inside the zip.
SNAPSHOT_VARIANTS = {
    "cme": r"GMPI\d{4}_puf\.csv$",
    "hot": r"GMPI_HOT_\d{4}_puf\.csv$",
}


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    assert path_to_file is not None, "Pass the path to the zip file sent by OPHI with --path-to-file."

    with zipfile.ZipFile(path_to_file) as zf, tempfile.TemporaryDirectory() as tmp:
        members = zf.namelist()
        for variant, pattern in SNAPSHOT_VARIANTS.items():
            # Find the single csv file of this variant in the zip.
            matches = [m for m in members if re.search(pattern, Path(m).name)]
            assert len(matches) == 1, f"Expected one file matching {pattern} in the zip, found {matches}."

            # Extract it and create the snapshot from it.
            extracted = zf.extract(matches[0], path=tmp)
            snap = Snapshot(f"ophi/{SNAPSHOT_VERSION}/multidimensional_poverty_index_{variant}.csv")
            snap.create_snapshot(filename=extracted, upload=upload)

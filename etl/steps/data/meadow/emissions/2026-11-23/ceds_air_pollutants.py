"""Load a snapshot and create a meadow dataset."""

import re
import zipfile
from pathlib import Path

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder
from etl.snapshot import Snapshot

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def read_data_from_snapshot(snap: Snapshot, filename_pattern: str = r".*\.csv") -> Table:
    tbs = []
    with zipfile.ZipFile(snap.path, "r") as zip_file:
        for filename in zip_file.namelist():
            # Skip macOS metadata files (e.g. "__MACOSX/.../._BC_....csv"), which the producer's zip files include since v_2025_03_18.
            if filename.startswith("__MACOSX/") or Path(filename).name.startswith("._"):
                continue
            if re.fullmatch(filename_pattern, Path(filename).name):
                _tb = pr.read_csv(
                    zip_file.open(filename), origin=snap.metadata.origin, metadata=snap.to_table_metadata()
                )
                # Since v_2025_03_18, the bunkers files have no pollutant column; the pollutant is only given in the file name (e.g. "S.SO2_bunker_estimates_v_2025_03_18.csv").
                if "em" not in _tb.columns:
                    match = re.fullmatch(r"S\.(\w+?)_bunker_estimates_.*\.csv", Path(filename).name)
                    assert match, f"Unexpected file name without a pollutant column: {filename}"
                    _tb.insert(0, "em", match.group(1))
                    _tb["em"] = _tb["em"].copy_metadata(_tb["sector"])
                tbs.append(_tb)

    # Combine all tables into a single one.
    tb = pr.concat(tbs)

    return tb


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshots.
    snap_detailed = paths.load_snapshot("ceds_air_pollutants__detailed.zip")
    snap_bunkers = paths.load_snapshot("ceds_air_pollutants__bunkers.zip")
    snap_extension = paths.load_snapshot("ceds_air_pollutants__extension.zip")

    # Read data from all csv files within each of the snapshot zip folders.
    tb_detailed = read_data_from_snapshot(snap=snap_detailed)
    tb_bunkers = read_data_from_snapshot(snap=snap_bunkers)
    # The extension zip folder contains the same global totals by sector, by fuel, and by sector and fuel.
    # Read only the files by sector (which is the breakdown we use), to avoid counting emissions more than once.
    tb_extension = read_data_from_snapshot(
        snap=snap_extension, filename_pattern=r".*_global_estimates_by_sector_v.*\.csv"
    )

    #
    # Process data.
    #
    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tables = [
        tb_detailed.format(["em", "country", "sector", "fuel"]),
        tb_bunkers.format(["em", "country", "sector"]),
        tb_extension.format(["em", "sector"]),
    ]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap_detailed.metadata)

    # Save meadow dataset.
    ds_meadow.save()

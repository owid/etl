"""Create the Statistical Review of World Energy snapshots from files downloaded manually.

The Energy Institute serves its data files behind a Cloudflare challenge, so they can no longer
be fetched with a plain ``url_download``. Download them by hand and let this script ingest them.

To update:

1. Go to https://www.energyinst.org/statistical-review/resources-and-data-downloads and download:
   - The consolidated dataset in *narrow format* (CSV).
   - The main "all data" workbook (``EI-Stats-Review-ALL-data.xlsx``).
2. Leave them in your ``~/Downloads`` folder (or pass ``--path-to-file <folder>``).
3. Create both snapshots with a single command:
       etls energy_institute/<version>/statistical_review_of_world_energy

We ingest two files:
- The narrow-format consolidated CSV is the source of truth for the energy indicators (total
  energy supply, generation, production and consumption by fuel, electricity by fuel, etc.). It is
  much simpler to parse than the workbook, and its values match the workbook exactly.
- The workbook is still needed for the indicators that the consolidated dataset does not include:
  coal/oil/gas reserves, fossil fuel prices, and the thermal-equivalent efficiency factors.
"""

from pathlib import Path

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Map each snapshot (the `.dvc` next to this script) to the name of the file downloaded from the
# Energy Institute. The workbook keeps its original name; the narrow CSV is served with a
# human-readable name by the download button.
FILES = {
    "statistical_review_of_world_energy.csv": "Statistical Review of World Energy Narrow format.csv",
    "statistical_review_of_world_energy.xlsx": "EI-Stats-Review-ALL-data.xlsx",
}


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    """Create the Statistical Review of World Energy snapshots.

    Args:
        upload: Whether to upload the snapshots to S3.
        path_to_file: Folder containing the downloaded files. Defaults to the user's ``~/Downloads``.
    """
    folder = Path(path_to_file) if path_to_file else Path.home() / "Downloads"

    for snapshot_file, downloaded_name in FILES.items():
        source_file = folder / downloaded_name
        if not source_file.exists():
            raise FileNotFoundError(
                f"Could not find '{downloaded_name}' in {folder}. Download it from the Energy Institute "
                f"(see the module docstring) or pass --path-to-file pointing to its folder."
            )

        snap = paths.init_snapshot(filename=snapshot_file)
        snap.create_snapshot(filename=source_file, upload=upload)

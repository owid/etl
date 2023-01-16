"""
- The raw xls file is downloaded from here: http://atmenv.envi.osakafu-u.ac.jp/omu-content/uploads/sites/1215/2015/10/KyotoFullFlower7.xls
- This has data for 812 - 2015.
- We supplement this for 2016-2021 from the table on this page - https://web.archive.org/web/20230102085103/http://atmenv.envi.osakafu-u.ac.jp/aono/kyophenotemp4/
- Data from 2022 was manually added from here - https://web.archive.org/web/20221008084905/https://www.metoffice.gov.uk/about-us/press-office/news/weather-and-climate/2022/kyoto-cherry-blossom-dates-shifted-by-human-influence
"""

from pathlib import Path

import click

from etl.snapshot import Snapshot


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    # Create new snapshot.
    snap = Snapshot("biodiversity/2023-01-11/cherry_blossom.csv")
    # Ensure destination folder exists.
    snap.path.parent.mkdir(exist_ok=True, parents=True)
    # Copy local data file to snapshots data folder.
    snap.path.write_bytes(Path(path_to_file).read_bytes())
    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


if __name__ == "__main__":
    main()

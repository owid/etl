"""Script to create a snapshot of the World Values Survey (time series) dataset.

This dataset contains selected questions of the WVS Time-Series (1981-2022), depending on our needs.
The CSV is produced by running wvs_create_file.do in Stata against the WVS Time-Series Stata file
(see the instructions in that .do file). After generating wvs.csv:

    etls wvs/{date}/world_values_survey --path-to-file snapshots/wvs/{date}/wvs.csv

Then delete the local csv file.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run(upload: bool = True, path_to_file: str | None = None) -> None:
    # Create a new snapshot.
    snap = paths.init_snapshot()

    # Copy local data file to snapshots data folder, add file to DVC and upload to S3.
    snap.create_snapshot(filename=path_to_file, upload=upload)

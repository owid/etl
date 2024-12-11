"""Ingest script for Ember's Yearly electricity data.

Ember's recommendation is to use the Yearly electricity data by default (which is more regularly updated than the Global
and European Electricity Reviews).

"""

import pathlib

import click

from etl.snapshot import Snapshot

# Version of current snapshot.
SNAPSHOT_VERSION = pathlib.Path(__file__).parent.name

########################################################################################################################
# TODO: Temporarily using a local file. Fetch data directly using the yearly electricity data url after next update.
#  The download url should still be the same:
#  https://ember-climate.org/app/uploads/2022/07/yearly_full_release_long_format.csv
# NOTE: This link seems to have changed now to:
# https://storage.googleapis.com/emb-prod-bkt-publicdata/public-downloads/yearly_full_release_long_format.csv
########################################################################################################################


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload Snapshot",
)
@click.option("--path-to-file", prompt=True, type=str, help="Path to local data file.")
def main(path_to_file: str, upload: bool) -> None:
    snap = Snapshot(f"ember/{SNAPSHOT_VERSION}/yearly_electricity.csv")
    snap.create_snapshot(upload=upload, filename=path_to_file)


if __name__ == "__main__":
    main()

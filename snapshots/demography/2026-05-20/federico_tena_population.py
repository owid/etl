"""Snapshot of the Federico–Tena World Population Historical Database (V2, 1991 borders).

The source dataverse (edatos.consorciomadrono.es) sits behind an Anubis JS proof-of-work bot
wall, so the file cannot be fetched unattended (an automated download stores the challenge HTML
page instead of the data). Download the file manually from the DOI landing page — choosing
"Original File Format" — and pass the local path:

    python snapshots/demography/2026-05-20/federico_tena_population.py --path-to-file <path>/federico_tena_population.tab

DOI: 10.21950/GW7SOZ (datafile 51077, original tab-separated).
"""

import click

from etl.helpers import PathFinder

paths = PathFinder(__file__)


@click.command()
@click.option("--upload/--skip-upload", default=True, help="Upload snapshot to S3.")
@click.option("--path-to-file", prompt=True, type=str, help="Path to the locally downloaded data file.")
def run(path_to_file: str, upload: bool) -> None:
    snap = paths.init_snapshot()
    snap.create_snapshot(filename=path_to_file, upload=upload)


if __name__ == "__main__":
    run()

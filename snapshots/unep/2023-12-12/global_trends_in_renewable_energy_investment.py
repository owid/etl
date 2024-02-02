"""Snapshot of UNEP's report called Global trends in renewable energy investment."""

from pathlib import Path

import click

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    snap = Snapshot(f"unep/{SNAPSHOT_VERSION}/global_trends_in_renewable_energy_investment.pdf")
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    main()

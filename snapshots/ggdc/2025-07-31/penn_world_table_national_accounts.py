import click

from etl.snapshot import Snapshot


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot("ggdc/2021-06-18/penn_world_table_national_accounts.xlsx")

    # Save snapshot.
    snap.create_snapshot(upload=upload)


if __name__ == "__main__":
    run()

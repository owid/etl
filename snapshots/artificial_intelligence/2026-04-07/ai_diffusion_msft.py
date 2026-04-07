"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import requests

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

URL = "https://www.microsoft.com/en-us/research/wp-content/uploads/2026/01/Microsoft-AI-Diffusion-Report-2025-H2.pdf"


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    snap = Snapshot(f"artificial_intelligence/{SNAPSHOT_VERSION}/ai_diffusion_msft.pdf")
    snap.path.parent.mkdir(exist_ok=True, parents=True)

    response = requests.get(URL)
    response.raise_for_status()

    with open(snap.path, "wb") as f:
        f.write(response.content)

    snap.create_snapshot(upload=upload, filename=snap.path)


if __name__ == "__main__":
    run()

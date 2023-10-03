from html.parser import HTMLParser
from pathlib import Path

import click
import pandas as pd

from etl.backport_helpers import long_to_wide
from etl.snapshot import Snapshot, SnapshotMeta

SNAPSHOT_NAMESPACE = Path(__file__).parent.parent.name
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option(
    "--upload/--skip-upload",
    default=True,
    type=bool,
    help="Upload dataset to Snapshot",
)
def main(upload: bool) -> None:
    # Load backported snapshot.
    snap_values = Snapshot(
        "backport/latest/dataset_2762_cross_country_literacy_rates__world_bank__cia_world_factbook__and_other_sources_values.feather"
    )
    snap_values.pull()
    snap_config = Snapshot(
        "backport/latest/dataset_2762_cross_country_literacy_rates__world_bank__cia_world_factbook__and_other_sources_config.json"
    )
    snap_config.pull()

    # Create snapshot metadata for the new file
    meta = SnapshotMeta(**snap_values.metadata.to_dict())
    meta.namespace = SNAPSHOT_NAMESPACE
    meta.version = SNAPSHOT_VERSION
    meta.short_name = "literacy_rates"
    meta.fill_from_backport_snapshot(snap_config.path)
    parser = HTMLToMarkdown()
    parser.feed(meta.source.description)  # type: ignore
    meta.source.description = parser.get_markdown()  # type: ignore

    meta.save()

    # Create a new snapshot.
    snap = Snapshot(meta.uri)

    # Convert from long to wide format.
    df = long_to_wide(pd.read_feather(snap_values.path))

    # Copy file to the new snapshot.
    snap.path.parent.mkdir(parents=True, exist_ok=True)
    df.reset_index().to_feather(snap.path)

    # Add file to DVC and upload to S3.
    snap.dvc_add(upload=upload)


class HTMLToMarkdown(HTMLParser):
    def __init__(self):
        super().__init__()
        self.md = []
        self.lasttag = None  # type: ignore

    def handle_starttag(self, tag, attrs):
        if tag == "br":
            self.md.append("\n")
        elif tag == "ul":
            self.md.append("\n")
        elif tag == "li":
            self.md.append("- ")
        elif tag == "a":
            for attr in attrs:
                if attr[0] == "href":
                    self.md.append("[")
                    self.lasttag = ("a", attr[1])  # type: ignore
        elif tag == "strong" or tag == "b":
            self.md.append("**")

    def handle_endtag(self, tag):
        if tag == "a" and self.lasttag and self.lasttag[0] == "a":
            self.md.append("](" + self.lasttag[1] + ")")
            self.lasttag = None  # type: ignore
        elif tag == "ul":
            self.md.append("\n")
        elif tag == "li":
            self.md.append("\n")
        elif tag == "strong" or tag == "b":
            self.md.append("**")

    def handle_data(self, data):
        self.md.append(data)

    def get_markdown(self):
        return "".join(self.md)


if __name__ == "__main__":
    main()

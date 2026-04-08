"""Script to create a snapshot of dataset.

The data for this snapshot was taken from the IUCN Red List Summary Statistics Table 1a, available at: https://www.iucnredlist.org/resources/summary-statistics
"""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool = True) -> None:
    """Create a new snapshot.

    Args:
        upload: Whether to upload the snapshot to S3.
    """
    # Init Snapshot object
    snap = Snapshot(f"iucn/{SNAPSHOT_VERSION}/threatened_and_evaluated_species.csv")
    df = pd.DataFrame(
        {
            "taxonomic_group": [
                "Mammals",
                "Birds",
                "Reptiles",
                "Amphibians",
                "Fishes",
                "All vertebrates",
                "Insects",
                "Molluscs",
                "Crustaceans",
                "Corals",
                "Arachnids",
                "Velvet worms",
                "Horsehoe crabs",
                "Other invertebrates",
                "All invertebrates",
                "Mosses",
                "Ferns and allies",
                "Gymnosperms",
                "Flowering plants",
                "Green algae",
                "Red algae",
                "All plants",
                "Mushrooms",
                "Brown algae",
                "All fungi",
                "All groups",
            ],
            "described_species": [
                6815,
                11185,
                12502,
                8918,
                37288,
                76712,
                1003469,
                88244,
                83263,
                5672,
                97085,
                220,
                4,
                230485,
                1508442,
                21925,
                11800,
                1113,
                369000,
                14550,
                7744,
                426132,
                157648,
                5005,
                162653,
                2173939,
            ],
            "evaluated_species": [
                6036,
                11185,
                10368,
                8051,
                29114,
                64754,
                13696,
                9502,
                3361,
                916,
                1053,
                11,
                4,
                1139,
                29682,
                327,
                834,
                1061,
                74545,
                18,
                78,
                76864,
                1302,
                18,
                1320,
                172620,
            ],
            "threatened_species": [
                1364,
                1256,
                1859,
                2930,
                4085,
                11494,
                2680,
                2616,
                774,
                333,
                390,
                9,
                2,
                183,
                6987,
                181,
                327,
                453,
                28778,
                0,
                9,
                29748,
                411,
                6,
                417,
                48646,
            ],
            "best_share_threatened": [
                26,
                11,
                21,
                41,
                pd.NA,
                21,
                pd.NA,
                pd.NA,
                pd.NA,
                pd.NA,
                pd.NA,
                pd.NA,
                100,
                pd.NA,
                pd.NA,
                pd.NA,
                pd.NA,
                43,
                pd.NA,
                pd.NA,
                pd.NA,
                pd.NA,
                pd.NA,
                pd.NA,
                pd.NA,
                pd.NA,
            ],
        }
    )

    # Save snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()

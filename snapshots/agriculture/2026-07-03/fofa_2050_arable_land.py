"""Script to create a snapshot of FAO's projections of arable land to 2050.

The data is manually transcribed from Table 4.12 of the report (FAO, 2018, The future of food and agriculture -
Alternative pathways to 2050, https://www.fao.org/3/I8429EN/i8429en.pdf).

NOTE: FAO also publishes machine-readable outputs of the underlying GAPS model at
https://www.fao.org/global-perspectives-studies/food-agriculture-projections-to-2050/en/
However, those raw model outputs deviate from the figures published in the report; e.g. the world total in the 2012
base year is 1,601 million hectares in the model outputs, but 1,567 in the report (which coincides with FAOSTAT
cropland at the time of publication). At the country level, deviations between raw model outputs and FAOSTAT data can
be much larger. Therefore, we use the (world-level) published figures, which connect better with the observed FAOSTAT
data.
"""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name

# World totals of arable land (in million hectares) from Table 4.12 of the report.
# The printed table informs a historical value (1970), a base year (2012, shared by all scenarios), and projections
# (2030 and 2050) for each of the three scenarios of the report.
DATA = pd.DataFrame(
    columns=["year", "scenario", "arable_land"],
    data=[
        (1970, "Historical", 1438),
        (2012, "Base year", 1567),
        (2030, "Business As Usual", 1690),
        (2030, "Towards Sustainability", 1594),
        (2030, "Stratified Societies", 1812),
        (2050, "Business As Usual", 1732),
        (2050, "Towards Sustainability", 1653),
        (2050, "Stratified Societies", 1892),
    ],
)


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    # Initialize a new snapshot.
    snap = Snapshot(f"agriculture/{SNAPSHOT_VERSION}/fofa_2050_arable_land.csv")

    # Save snapshot.
    snap.create_snapshot(data=DATA, upload=upload)


if __name__ == "__main__":
    run()

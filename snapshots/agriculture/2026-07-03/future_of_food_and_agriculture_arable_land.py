"""Script to create a snapshot of FAO's projections of arable land to 2050.

The data is manually transcribed from Table 4.12 of the report (FAO, 2018, The future of food and agriculture -
Alternative pathways to 2050, https://www.fao.org/3/I8429EN/i8429en.pdf).

NOTE: FAO also publishes machine-readable outputs of the underlying GAPS model at
https://www.fao.org/global-perspectives-studies/food-agriculture-projections-to-2050/en/
We do not use them for arable land because they correspond to a slightly different (revised) model run, which was
never re-anchored to FAOSTAT land statistics, whereas the figures printed in the report were. Concretely:
- In the 2012 base year, the machine-readable world total is 1,601 million hectares, while the report publishes 1,567
  (which coincides with FAOSTAT cropland at the time of publication). Charts combining these projections with the
  observed FAOSTAT series would therefore show a visible discontinuity if the machine-readable data was used.
- The deviation is not a constant offset: it varies by region and year in both directions (e.g. for Sub-Saharan
  Africa in 2050, the machine-readable total is 11% lower than the published one), and at the country level it can
  be much larger (e.g. more than 40% for Australia and Canada).
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
    snap = Snapshot(f"agriculture/{SNAPSHOT_VERSION}/future_of_food_and_agriculture_arable_land.csv")

    # Save snapshot.
    snap.create_snapshot(data=DATA, upload=upload)

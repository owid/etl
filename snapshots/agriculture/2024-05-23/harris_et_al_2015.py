"""Script to create a snapshot of dataset."""

from pathlib import Path

import click
import pandas as pd

from etl.snapshot import Snapshot

# Version for current snapshot dataset.
SNAPSHOT_VERSION = Path(__file__).parent.name


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def main(upload: bool) -> None:
    # Create a new snapshot.
    snap = Snapshot(f"agriculture/{SNAPSHOT_VERSION}/harris_et_al_2015.csv")

    # Data extracted from chatGPT 4o (and manually inspected and corrected).
    data = """
    1270/1279, Broadberry et al. (2015), 2203
    1300, Allen (2005), 1791
    1300, Overton and Campbell (1996), n/a
    1300/1309, Broadberry et al. (2015), 2056
    1310/1319, Broadberry et al. (2015), 1998
    1380, Overton and Campbell (1996), n/a
    1380/1389, Broadberry et al. (2015), 2467
    1420/1429, Broadberry et al. (2015), 2146
    1450/1459, Broadberry et al. (2015), 2176
    1500, Allen (2005), 3397
    1600, Muldrew (2011), 3062
    1600, Overton and Campbell (1996), n/a
    1600/1609, Broadberry et al. (2015), 2104
    1650/1659, Broadberry et al. (2015), 1945
    1700, Allen (2005), 3255
    1700, Floud et al. (2011) (Estimates A and B), 2230
    1700, Fogel (2004), 2095
    1700, Meredith and Oxley (2014), 2557
    1700, Muldrew (2011), 3579
    1700, Overton and Campbell (1996), n/a
    1700/1709, Broadberry et al. (2015), 2187
    1750, Allen (2005), 3803
    1750, Floud et al. (2011) (Estimate A; with correction), 2328
    1750, Floud et al. (2011) (Estimate B; with correction), 2516
    1750, Fogel (2004), 2168
    1750, Kelly and Ó Gráda (2013b), 2914-2949
    1750/1759, Broadberry et al. (2015), 2178
    1770, Kelly and Ó Gráda (2013b), 3542-3547
    1770, Meredith and Oxley (2014), 3271
    1770, Muldrew (2011), 5047
    1800, Allen (2005), 2938
    1800, Floud et al. (Estimate A), 2472
    1800, Floud et al. (Estimate B), 2439
    1800, Fogel (2004), 2237
    1800, Kelly and Ó Gráda (2013b) (Estimate A), 2941-2956
    1800, Kelly and Ó Gráda (2013b) (Estimate B), 2749-2794
    1800, Meredith and Oxley (2014), 2620
    1800, Muldrew (2011), 3977
    1800, Overton and Campbell (1996), n/a
    1800/1809, Broadberry et al. (2015), 2175
    1830, Overton and Campbell (1996), n/a
    1830/1839, Broadberry et al. (2015), 1950
    1840/1849, Broadberry et al. (2015), 2166
    1850, Allen (2005), 2525
    1850, Floud et al. (2011) (Estimate A), 2505
    1850, Floud et al. (2011) (Estimate B)/Meredith and Oxley (2013), 2545
    1850, Fogel (2004), 2362
    1850/1859, Broadberry et al. (2015), 2111
    1861/1870, Broadberry et al. (2015), 2463
    1871, Overton and Campbell (1996), n/a
    1909/13, Floud et al. (2011) & Meredith and Oxley (2014), 2977
    1909/13, Fogel (2004), 2857
    1954/55, Fogel (2004), 3231
    1961, Fogel (2004), 3170
    1965, Fogel (2004), 3304
    1989, Fogel (2004), 3149
    """

    # Create a dataframe with the extracted data.
    data_parsed = [[item.strip() for item in line.split(",")] for line in data.split("\n")[1:-1]]
    df = pd.DataFrame(data_parsed, columns=["Years", "Source", "Total"])

    # Create snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()

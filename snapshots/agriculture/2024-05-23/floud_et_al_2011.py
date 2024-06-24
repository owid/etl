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
    # Initialize new snapshots for daily caloric intake in the US and in Western Europe.
    snap_us = Snapshot(f"agriculture/{SNAPSHOT_VERSION}/floud_et_al_2011_daily_calories_us.csv")
    snap_europe = Snapshot(f"agriculture/{SNAPSHOT_VERSION}/floud_et_al_2011_daily_calories_europe.csv")

    # Data from Table 6.6 on US daily caloric intake, extracted using chatGPT 4o (and manually inspected).
    data_us = """
Year,Calories
1800,2952
1810,2935
1820,2904
1830,2888
1840,3013
1850,2585
1860,2826
1870,3029
1880,3237
1890,3134
1900,3212
1910,3068
1920,3259
1930,3400
1940,3300
1952,3200
1960,3100
1970,3200
1980,3200
1990,3500
2000,3900
2004,3900
    """

    # Create a dataframe with the extracted data.
    data_us_parsed = [line.split(",") for line in data_us.split("\n")[1:-1]]
    df_us = pd.DataFrame(data_us_parsed[1:], columns=data_us_parsed[0])

    # Data from Table 5.5 on Western Europe daily caloric intake, extracted using chatGPT 4o (and manually inspected).
    data_europe = """
country,1800,1810,1820,1830,1840,1850,1860,1870,1880,1890,1900,1910,1920,1930,1940,1950,1960
Belgium,2840,,,,,2423,2426,2553,2663,2851,2987,3278,,2940,,,3040
England,2436,,,,,2512,,,2773,,,2977,,2810,3060,3120,3280
Finland,,,,,,,1900,,,,,3000,,2950,,,3110
France,1846,,1984,2118,2377,2840,2854,3085,3085,3220,3192,3323,3133,,,,3050
Germany,2210,,,,,,2120,,,,,,,,,,2960
Iceland,,,2887,,3080,3381,,2573,3002,3106,3316,3499,,,,,
Italy,,,,,,,,2647,2197,2119,,2617,,2627,,,2730
Netherlands,,,,,,,2227,,2493,,2721,,,,,,
Norway,,1800,,,2250,,3300,,,,,,,,,,2930
    """
    # Create a dataframe with the extracted data.
    data_europe_parsed = [line.split(",") for line in data_europe.split("\n")[1:-1]]
    df_europe = pd.DataFrame(data_europe_parsed[1:], columns=data_europe_parsed[0])

    # Create snapshots.
    snap_us.create_snapshot(upload=upload, data=df_us)
    snap_europe.create_snapshot(upload=upload, data=df_europe)


if __name__ == "__main__":
    main()

"""Script to create a snapshot of dataset."""

from io import StringIO
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
    snap = Snapshot(f"agriculture/{SNAPSHOT_VERSION}/fao_1949.csv")

    # Data manually extracted.
    data = """
country,1947/48,1948/49
Burma,1986,1877
Ceylon,1977,1918
India,,1570
Japan,1670,1795
Philippines,1770,
Thailand,2110,2020
Austria,2397,2698
Belgium,2667,2760
Czechoslovakia,2402,2656
Denmark,3125,3206
Finland,2617,2851
France,2357,2667
Greece,2266,2358
Hungary,2432,
Iceland,3268,
Ireland,3260,3276
Italy,2249,2398
Netherlands,2856,
Luxembourg,2693,2878
Norway,2899,3051
Poland,2363,2625
Portugal,2279,2184
Spain,2180,2377
Sweden,2871,3108
Switzerland,3050,2996
United Kingdom,2968,3084
Yugoslavia,2144,
Australia,3262,3265
New Zealand,3286,3259
Canada,3161,3141
United States,3244,3186
Cuba,2682,2814
El Salvador,1557,
Mexico,2032,2101
Argentina,3188,3191
Brazil,2245,
Chile,2352,2356
Colombia,1950,
Peru,1925,2219
Uruguay,2490,2529
Egypt,2364,2458
Turkey,2173,2506
Ethiopia,1770,
Algeria,1279,1421
Madagascar,2074,
Morocco,1837,1825
Tanganyika,2163,
Tunisia,1498,1545
Union of South Africa,2422,2517
    """
    # NOTE:
    # * The table includes China, but only for 22 provinces, so we ignore it.
    # * The table includes India and Pakistan, but the footnote says that the value for 1948/49 is only India.
    # * Footnote says about Japan: "1t is believed by the Supreme Command Allied Powers that for staple foods there is an appreciable understatement of production, particularly from home gardens, both in staple foods and vegetables. A nutrition survey conducted by the Ministry of Welfare estimated calorie supplies per person per day at 1,965.".
    # * Footnote says about France: "Unreported production has most likely provided enough calories to raise the level to about 2,500-2,600 calories.".
    # * For some countries, the footnote says "Calendar year basis: 1947 and 1948.", but that is already the years we will use for all countries.

    # Create a dataframe with the extracted data.
    df = pd.read_csv(StringIO(data))

    # Create snapshot.
    snap.create_snapshot(upload=upload, data=df)


if __name__ == "__main__":
    main()

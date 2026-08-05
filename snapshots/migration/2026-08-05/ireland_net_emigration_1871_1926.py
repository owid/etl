"""Hand-entered data from the 1926 Census of Ireland, Volume X — General Report (1934).

Two small tables, both restated for the 26-county area that became the Republic of Ireland:

- Average yearly net emigration by sex for each intercensal period, 1871-1926, from the section
  "Natural Increase and Emigration" on page 19.
- Census populations in thousands, 1871-1926, from the table "Population (in thousands)" on
  page 11 (Saorstat Eireann column). Used as the denominator for rates.

Source PDF: https://www.cso.ie/en/media/csoie/census/census1926results/volume10/C_1926_V10.pdf
(page 19 of the report is page 29 of the PDF; page 11 is page 20).
"""

import click
import pandas as pd

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Page 19: average yearly net emigration (persons per year), by sex, per intercensal period.
NET_EMIGRATION = [
    # (period_start, period_end, males, females)
    (1871, 1881, 24_958, 25_214),
    (1881, 1891, 29_257, 30_476),
    (1891, 1901, 20_315, 19_327),
    (1901, 1911, 11_764, 14_390),
    (1911, 1926, 13_934, 13_068),
]

# Page 11: population of Saorstat Eireann at each census, in thousands.
POPULATION = {1871: 4_053, 1881: 3_870, 1891: 3_469, 1901: 3_222, 1911: 3_140, 1926: 2_972}


@click.command()
@click.option("--upload/--skip-upload", default=True, type=bool, help="Upload dataset to Snapshot")
def run(upload: bool) -> None:
    df = pd.DataFrame(
        NET_EMIGRATION, columns=["period_start", "period_end", "net_emigration_males", "net_emigration_females"]
    )
    df["population_start_thousands"] = df["period_start"].map(POPULATION)
    df["population_end_thousands"] = df["period_end"].map(POPULATION)

    # The report's own text (p. 19): 1911-1926 net emigration totalled 405,029, an average of
    # 27,002 per year. Check the entered values reproduce this within the report's rounding.
    total = df[["net_emigration_males", "net_emigration_females"]].sum(axis=1)
    assert total.iloc[-1] == 27_002, "1911-1926 yearly average does not match the report."
    assert abs(total.iloc[-1] * 15 - 405_029) <= 15, "1911-1926 total does not match the report."

    snap = paths.init_snapshot()
    snap.create_snapshot(data=df, upload=upload)

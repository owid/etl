"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Number of laying hens by farming method (maximum capacity) according to notifications under
# Commission Implementing Regulation (EU) 2017/1185, Art. 12(b) - Annex III.9.
# Extracted from page 8 of the EU Eggs Market Situation Dashboard (last update 2026-04-15).
# Columns: country (EU MS code), year, total hens, and percentage by housing type (as in PDF).
# * 2020 data (EL), ** 2023 data (NL, MT), *** 2024 data (FR, BE, PT, HU, LT); all others are 2025 data.
# fmt: off
DATA = [
    # country  year   total      enriched   barn       free_range  organic
    ("FR",     2024,  59333500,  31.1,      21.9,      34.2,       12.8),
    ("DE",     2025,  57824667,   0.7,      55.6,      30.0,       13.7),
    ("PL",     2025,  53646618,  62.2,      28.8,       7.6,        1.4),
    ("ES",     2025,  47223460,  59.8,      27.4,      11.2,        1.6),
    ("IT",     2025,  44174644,  33.2,      56.7,       5.0,        5.1),
    ("NL",     2023,  29926930,  14.8,      63.1,      16.1,        6.0),
    ("RO",     2025,  12989962,  41.8,      49.5,       6.5,        2.2),
    ("BE",     2024,  11244969,  34.0,      45.8,      13.5,        6.8),
    ("SE",     2025,   9389197,   0.7,      81.2,       8.8,        9.3),
    ("PT",     2024,   8938930,  67.1,      25.7,       6.0,        1.2),
    ("HU",     2024,   8012157,  67.6,      30.8,       1.2,        0.3),
    ("AT",     2025,   7468497,   0.0,      55.0,      32.0,       13.0),
    ("CZ",     2025,   7193248,  42.8,      54.7,       1.9,        0.6),
    ("BG",     2025,   6359900,  64.5,       5.3,      30.2,        0.0),
    ("FI",     2025,   6020435,  19.0,      72.1,       4.6,        4.4),
    ("DK",     2025,   4964091,  11.7,      54.2,       8.4,       25.7),
    ("EL",     2020,   4649598,  76.5,      12.4,       5.5,        5.6),
    ("IE",     2025,   4246029,  25.0,      25.9,      45.7,        3.4),
    ("LT",     2024,   3427926,  76.6,      21.9,       1.3,        0.2),
    ("SK",     2025,   2882998,  49.9,      41.7,       8.1,        0.3),
    ("HR",     2025,   2416733,  42.0,      50.9,       6.2,        0.8),
    ("LV",     2025,   1826683,  45.6,      45.6,       8.6,        0.3),
    ("SI",     2025,   1313673,  14.8,      77.4,       5.1,        2.8),
    ("EE",     2025,    899232,  77.1,       9.8,       8.4,        4.6),
    ("CY",     2025,    569155,  59.7,      21.0,      17.2,        2.1),
    ("MT",     2023,    364624,  97.2,       2.8,       0.0,        0.0),
    ("LU",     2025,    155340,   0.0,      56.4,      16.1,       27.6),
]
# fmt: on

COLUMNS = ["country", "year", "total", "pct_enriched_cage", "pct_barn", "pct_free_range", "pct_organic"]


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("eu_eggs_dashboard_2026_04.pdf")

    #
    # Process data.
    #
    tb = pr.read_df(
        df=pd.DataFrame(DATA, columns=COLUMNS),
        metadata=snap.to_table_metadata(),
        origin=snap.metadata.origin,
    )

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()

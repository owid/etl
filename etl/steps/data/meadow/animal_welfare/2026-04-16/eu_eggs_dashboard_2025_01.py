"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Number of laying hens by farming method (maximum capacity) according to notifications under
# Commission Implementing Regulation (EU) 2017/1185, Art. 12(b) - Annex III.9.
# Extracted from page 8 of the EU Eggs Market Situation Dashboard.
# Columns: country (EU MS code), year, total hens, and percentage by housing type (as in PDF).
# * 2020 data (EL), ** 2021 data (LT), *** 2022 data (SE, HR); all others are 2023 data.
# fmt: off
DATA = [
    # country  year   total      enriched   barn       free_range  organic
    ("FR",     2023,  58471300,  30.1,      24.2,      32.3,       13.4),
    ("DE",     2023,  58103211,   4.0,      54.9,      27.2,       13.8),
    ("PL",     2023,  50693700,  70.1,      21.5,       6.8,        1.6),
    ("ES",     2023,  47704960,  67.1,      21.9,       9.6,        1.4),
    ("IT",     2023,  43279340,  34.0,      56.3,       4.9,        4.9),
    ("NL",     2023,  29926930,  14.8,      63.1,      16.1,        6.0),
    ("BE",     2023,  11004654,  34.7,      45.2,      14.0,        6.1),
    ("RO",     2023,  10367463,  52.8,      42.4,       2.3,        2.5),
    ("PT",     2023,   8946930,  67.2,      25.7,       6.0,        1.2),
    ("SE",     2022,   8323583,   2.7,      78.2,       7.4,       11.7),
    ("CZ",     2023,   7294745,  56.3,      41.5,       1.7,        0.5),
    ("AT",     2023,   7168105,   0.0,      55.6,      31.2,       13.2),
    ("HU",     2023,   7124002,  68.5,      29.6,       1.6,        0.4),
    ("FI",     2023,   5945695,  29.1,      62.9,       3.9,        4.0),
    ("BG",     2023,   5203191,  70.0,      25.3,       4.7,        0.0),
    ("EL",     2020,   4649598,  76.5,      12.4,       5.5,        5.6),
    ("DK",     2023,   4297010,  11.0,      51.5,       7.1,       30.3),
    ("IE",     2023,   3815296,  39.2,       9.0,      48.4,        3.4),
    ("LV",     2023,   3568353,  68.6,      27.2,       4.0,        0.2),
    ("LT",     2021,   2926891,  79.6,      18.5,       1.2,        0.6),
    ("SK",     2023,   2833782,  65.2,      27.6,       7.0,        0.3),
    ("HR",     2022,   2373301,  62.0,      32.0,       5.5,        0.5),
    ("SI",     2023,   1557759,  14.3,      79.2,       4.1,        2.4),
    ("EE",     2023,    888773,  80.9,       7.6,       5.5,        6.0),
    ("CY",     2023,    534036,  64.6,      17.5,      14.8,        3.0),
    ("MT",     2023,    364624,  97.2,       2.8,       0.0,        0.0),
    ("LU",     2023,    142672,   0.0,      62.1,      11.6,       26.4),
]
# fmt: on

COLUMNS = ["country", "year", "total", "pct_enriched_cage", "pct_barn", "pct_free_range", "pct_organic"]


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("eu_eggs_dashboard_2025_01.pdf")

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

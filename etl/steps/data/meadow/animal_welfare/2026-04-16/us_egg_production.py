"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# U.S. Table Egg Layer Flock Estimates — flock size in millions of hens.
# Manually extracted from page 4 of the USDA Egg Markets Overview PDF (September 26, 2025).
# Data as of September 1, 2025. Past year annual numbers reflect flock as of the end of each year.
# Rows follow the same order as the original table for easy comparison with the PDF.
# fmt: off
DATA = {
    #                                    2025,  2024,  2023,  2022,  2021,  2020,  2019,  2018,  2017,  2016,  2015,  2014,  2013,  2012
    "year":                             [2025,  2024,  2023,  2022,  2021,  2020,  2019,  2018,  2017,  2016,  2015,  2014,  2013,  2012],
    # Total U.S. Caged (battery and enriched systems)
    "caged":                            [164.8, 184.0, 197.7, 205.3, 219.4, 235.5, 261.0, 276.1, 275.1, 276.1, 256.1, 281.6, 276.4, 265.5],
    # Total U.S. Cage-Free
    "cage_free":                        [136.6, 120.3, 123.9, 106.2, 111.1,  91.7,  79.7,  59.9,  52.4,  42.9,  37.3,  33.2,  29.6,  28.3],
    # NON-ORGANIC Cage-Free (subtotal)
    "non_organic_cage_free":            [116.6,  99.9, 105.4,  88.0,  92.9,  69.9,  60.3,  42.9,  36.4,  29.0,  26.0,  24.5,  21.4,  19.8],
    #   - Barn/Aviary
    "non_organic_barn_aviary":          [100.5,  84.5,  97.1,  84.1,  89.0,  65.1,  54.1,  39.0,  32.9,  25.3,  22.5,  21.0,  18.5,  16.9],
    #   - Free-Range
    "non_organic_free_range":           [  8.4,   7.8,   3.1,   2.4,   2.4,   2.8,   2.9,   1.5,   1.3,   1.6,   1.5,   1.5,   1.7,   1.6],
    #   - Pastured
    "non_organic_pastured":             [  7.7,   7.6,   5.2,   1.5,   1.5,   2.1,   3.3,   2.4,   2.2,   2.1,   2.0,   2.0,   1.3,   1.3],
    # USDA ORGANIC Cage-Free (subtotal)
    "organic_cage_free":                [ 20.0,  20.3,  18.5,  18.2,  18.2,  21.8,  19.4,  17.0,  16.0,  13.9,  11.4,   8.7,   8.2,   8.5],
    #   - Organic
    "organic":                          [ 11.9,  12.5,  11.9,  11.7,  11.7,  15.1,  14.9,  13.1,  12.3,  11.0,   9.0,   7.0,   6.7,   7.3],
    #   - Organic Free-Range
    "organic_free_range":               [  4.7,   4.5,   3.6,   3.8,   3.8,   4.0,   3.1,   2.7,   2.6,   2.1,   1.7,   1.2,   1.2,   1.0],
    #   - Organic Pastured
    "organic_pastured":                 [  3.4,   3.3,   3.0,   2.7,   2.7,   2.7,   1.4,   1.2,   1.2,   0.8,   0.7,   0.4,   0.4,   0.3],
}
# fmt: on


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("us_egg_production.pdf")

    #
    # Process data.
    #
    # Create table from data extracted from the PDF.
    tb = pr.read_df(df=pd.DataFrame(DATA), metadata=snap.to_table_metadata(), origin=snap.metadata.origin)

    # Improve table format.
    tb = tb.format(["year"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)

    # Save new meadow dataset.
    ds_meadow.save()

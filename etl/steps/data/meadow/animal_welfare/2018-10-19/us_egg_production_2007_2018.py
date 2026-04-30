"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# U.S. Table Egg Layer Cage-Free Flock Estimates — flock size in millions of hens, percentage of total flock.
# Extracted by Claude Code (and reviewed by a human) from page 2 of the USDA Egg Markets Overview PDF (October 19, 2018).
# Past year annual numbers reflect flock as of the end of each year.
# Rows follow the same order as the original table for easy comparison with the PDF.
# fmt: off
nan = float("nan")
DATA = {
    #                                2018    2017    2016    2015    2014    2013    2012    2011    2010    2009    2008    2007
    "year":                         [2018,   2017,   2016,   2015,   2014,   2013,   2012,   2011,   2010,   2009,   2008,   2007],
    # Total U.S. Cage-Free Flock: share of total flock (%) and layers (millions)
    "cage_free_pct":                [18.4,   16.6,   12.3,    8.6,    5.7,    5.9,    6.0,    5.4,    4.4,    3.6,    3.5,    3.2],
    "cage_free":                    [59.9,   52.4,   38.4,   23.6,   17.2,   17.1,   16.9,   15.2,   12.2,   10.2,    9.8,    9.1],
    # USDA Organic Cage-Free: share of total flock (%) and layers (millions)
    "organic_cage_free_pct":        [ 5.2,    5.1,    4.5,    4.2,    2.9,    2.8,    3.0,    2.6,    2.2,    1.8,    1.7,    1.6],
    "organic_cage_free":            [17.0,   16.0,   13.9,   11.4,    8.7,    8.2,    8.5,    7.4,    6.1,    5.1,    4.9,    4.5],
    # Non-Organic Cage-Free: share of total flock (%) and layers (millions)
    # Note: for 2007-2016 this is the combined total (barn/aviary, free-range, and pasture systems).
    "non_organic_cage_free_pct":    [12.5,   11.5,    7.9,    4.5,    2.8,    3.1,    3.0,    2.8,    2.2,    1.8,    1.7,    1.6],
    "non_organic_cage_free":        [42.9,   36.4,   24.5,   12.2,    8.5,    8.9,    8.3,    7.8,    6.1,    5.1,    4.9,    4.6],
    #   UEP Cage-Free (barn and aviary systems): share of total flock (%) and layers (millions)
    "uep_cage_free_pct":            [12.0,   10.4,    nan,    nan,    nan,    nan,    nan,    nan,    nan,    nan,    nan,    nan],
    "uep_cage_free":                [39.0,   32.9,    nan,    nan,    nan,    nan,    nan,    nan,    nan,    nan,    nan,    nan],
    #   Other Cage-Free (free-range and pasture systems): share of total flock (%) and layers (millions)
    "other_cage_free_pct":          [ 1.2,    1.1,    nan,    nan,    nan,    nan,    nan,    nan,    nan,    nan,    nan,    nan],
    "other_cage_free":              [ 3.9,    3.5,    nan,    nan,    nan,    nan,    nan,    nan,    nan,    nan,    nan,    nan],
}
# fmt: on


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("us_egg_production_2007_2018.pdf")

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

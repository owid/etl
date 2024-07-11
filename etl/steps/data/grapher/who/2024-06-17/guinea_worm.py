"""Load a garden dataset and create a grapher dataset."""

import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

YEAR_TO_CAT_MAP = {
    "1997": "1997-1999",
    "1998": "1997-1999",
    "2000": "2000s",
    "2004": "2000s",
    "2005": "2000s",
    "2007": "2000s",
    "2009": "2000s",
    "2011": "2010s",
    "2013": "2010s",
    "2015": "2010s",
    "2018": "2010s",
    "2022": "2020s",
    "2023": "2020s",
    "2024": "2020s",
    "Pre-certification": "Pre-certification",
    "Endemic": "Endemic",
}

YEAR_CATEGORIES = [
    "1997-1999",
    "2000s",
    "2010s",
    "2020s",
    "Pre-certification",
    "Endemic",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset

    print(dest_dir)
    ds_garden = paths.load_dataset("guinea_worm")

    # Read table from garden dataset.
    tb = ds_garden["guinea_worm"].reset_index()

    # remove certified year for all years except the current year
    tb = remove_certified_year(tb, 2023)

    # split "year_certified" in two columns:
    # - time_frame_certified: time frame in which country was certified as disease free (with status messages, Category type)
    # - year_certified: year in which country was certified as disease free (without status messages, Int64 type)

    tb["time_frame_certified"] = pd.Categorical(
        tb["year_certified"].map(YEAR_TO_CAT_MAP), categories=YEAR_CATEGORIES, ordered=True
    )
    # change to numeric dtype
    tb["year_certified"] = (
        pd.to_numeric(tb["year_certified"], errors="coerce").astype("Int64").copy_metadata(tb["year_certified"])
    )

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def remove_certified_year(tb, current_year):
    """Remove the year in which a country was certified as disease free
    except for the row of the current year."""
    tb.loc[tb["year"] != current_year, "year_certified"] = pd.NA
    return tb

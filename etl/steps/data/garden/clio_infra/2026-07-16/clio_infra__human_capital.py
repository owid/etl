"""Load snapshot and create a garden dataset."""

import numpy as np

from etl.helpers import PathFinder

paths = PathFinder(__file__)


def run() -> None:
    snap = paths.load_snapshot()

    #
    # Load data.
    #
    # The Excel is a wide-format table: rows are historical country-entities,
    # columns (from index 6 onward) are calendar years 1500–2015.
    raw = snap.read_excel(sheet_name="Data", header=None)

    # Row 2 contains the column headers; rows 0-1 are title/unit description.
    header = raw.iloc[2]
    year_cols = [
        int(header[i])
        for i in range(6, raw.shape[1])
        if isinstance(header[i], (int, float)) and not np.isnan(header[i])
    ]
    raw.columns = [(int(c) if isinstance(c, (int, float)) and not np.isnan(c) else str(c)) for c in header]
    data = raw.iloc[3:].reset_index(drop=True)

    # Melt wide → long: one row per (country, year).
    tb = data[["country name"] + year_cols].melt(
        id_vars=["country name"], var_name="year", value_name="book_titles_per_capita__fink_jensen_2015"
    )
    tb = tb.dropna(subset=["book_titles_per_capita__fink_jensen_2015"])
    tb = tb.rename(columns={"country name": "country"})
    tb["year"] = tb["year"].astype(int)

    #
    # Harmonize country names.
    #
    tb = paths.regions.harmonize_names(tb)

    #
    # Format and save.
    #
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    ds_garden = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_garden.save()

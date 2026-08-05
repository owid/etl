"""Load the HYDE historical livestock (mammals) snapshot and create a meadow dataset.

The HYDE historical compilation only covers eight mammal species (no birds).
"""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Name of the sheet in the excel file that contains the summary table.
SHEET_NAME = "Summary 2.2 | 10 yr interval"

# Years reported in the summary table (columns, left to right).
YEARS = [1890, 1900, 1910, 1920, 1930, 1940, 1950, 1960, 1970, 1980, 1990, 1998]

# Animal categories reported in the summary table (rows within each region block).
ANIMALS = ["Asses", "Buffaloe", "Cattle", "Goats", "Horses", "Mules", "Pigs", "Sheep"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load snapshot and read the summary sheet (the first two columns hold region and animal labels,
    # and the following columns hold the values for each year; there is no usable header row).
    snap = paths.load_snapshot()
    tb = snap.read_excel(sheet_name=SHEET_NAME, header=None)

    #
    # Process data.
    #
    # The region label only appears on the first row of each region block, so forward-fill it.
    tb = tb.rename(columns={0: "country", 1: "animal"})
    tb = tb.rename(columns={i + 2: year for i, year in enumerate(YEARS)})
    tb["country"] = tb["country"].ffill()

    # Keep only the animal rows (drops title, year-header and blank rows).
    tb = tb[tb["animal"].isin(ANIMALS)].reset_index(drop=True)

    # Reshape to one column per animal, indexed by region and year.
    tb = tb.melt(id_vars=["country", "animal"], value_vars=YEARS, var_name="year", value_name="value")
    tb["animal"] = tb["animal"].replace({"Buffaloe": "Buffalo"})
    tb = tb.pivot(index=["country", "year"], columns="animal", values="value", join_column_levels_with="_")
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=[tb], default_metadata=snap.metadata)
    ds_meadow.save()

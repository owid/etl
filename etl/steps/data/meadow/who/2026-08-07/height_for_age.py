"""Load the WHO height-for-age expanded tables and create a meadow dataset.

WHO publishes one file per (age group, measure, sex). This step concatenates the two
sexes into one table per (age group, measure), leaving the age column in its native
unit — days for the under-fives standards, months for the 5-19 reference. The two age
grids are put on a common footing in garden.
"""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# (age group, measure) -> native age column in the source files.
TABLES = {
    ("under_5", "percentiles"): "day",
    ("under_5", "zscores"): "day",
    ("5_to_19", "percentiles"): "month",
    ("5_to_19", "zscores"): "month",
}

SEXES = ["boys", "girls"]


def run() -> None:
    #
    # Load inputs.
    #
    tables = []
    for (age_group, measure), age_column in TABLES.items():
        tables.append(build_table(age_group, measure, age_column))

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=tables)
    ds_meadow.save()


def build_table(age_group: str, measure: str, age_column: str) -> Table:
    """Concatenate the boys' and girls' files for one (age group, measure) into one table."""
    tables = []
    for sex in SEXES:
        snap = paths.load_snapshot(f"height_for_age_{measure}_{sex}_{age_group}.xlsx")
        # Sheet names are inconsistent across the WHO files (e.g. "lhfa_boys_p_exp" vs
        # "LFA_boys_z_exp", "hfa_boys_perc_WHO2007_exp" vs "hfa_boys_z_WHO 2007_exp"),
        # so always read the first sheet by position.
        tb = snap.read_excel(sheet_name=0)
        tb["sex"] = sex
        tables.append(tb)

    tb = pr.concat(tables, ignore_index=True)
    tb["sex"] = tb["sex"].astype("category")

    return tb.format([age_column, "sex"], short_name=f"{measure}_{age_group}")

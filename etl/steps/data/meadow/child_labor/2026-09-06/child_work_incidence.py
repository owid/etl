"""Load the manually transcribed incidence snapshots, one meadow table per publication.

Every snapshot in this step shares the same schema — ``country, sex, age_group, year,
share, coverage`` — but they are kept as separate tables so that garden can work out which
publications each output indicator actually draws on, rather than attaching all of them to
every column. The sources that already have their own ETL steps (England and Wales, Italy,
the United States) are brought in at the garden level instead.
"""

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# One table per publication, named after the snapshot.
SNAPSHOTS = [
    "child_work_incidence_canada.csv",
    "child_work_incidence_canada_census.csv",
    "child_work_incidence_cunningham_viazzo.csv",
    "child_work_incidence_finland.csv",
    "child_work_incidence_france.csv",
    "child_work_incidence_roubaix.csv",
    "child_work_incidence_germany.csv",
    "child_work_incidence_netherlands.csv",
    "child_work_incidence_tilburg.csv",
    "child_work_incidence_spain_barcelona.csv",
    "child_work_incidence_spain_manlleu.csv",
]

INDEX = ["country", "sex", "age_group", "year"]


def run() -> None:
    #
    # Load inputs.
    #
    tables = []
    for snapshot_name in SNAPSHOTS:
        snap = paths.load_snapshot(snapshot_name)
        tb = snap.read()

        #
        # Process data.
        #
        # Sources that report single ages (Finland: 13, 14) parse as integers, so pin the
        # column to text — garden stacks these tables and needs one dtype across them all.
        age_group_metadata = tb["age_group"].metadata
        tb["age_group"] = tb["age_group"].astype(str)
        tb["age_group"].metadata = age_group_metadata

        assert not tb.duplicated(subset=INDEX).any(), f"Duplicate rows in {snapshot_name}."
        assert tb["share"].between(0, 100).all(), f"Share outside 0-100% in {snapshot_name}."

        for col in ["country", "sex", "age_group", "coverage"]:
            tb[col] = tb[col].astype("category")

        short_name = snapshot_name.removesuffix(".csv").removeprefix("child_work_incidence_")
        tables.append(tb.format(INDEX, short_name=short_name))

    #
    # Save outputs.
    #
    ds_meadow = paths.create_dataset(tables=tables)
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset.

This snapshot step is a bit more complex than usual. This is because the snapshot is a ZIP file that contains numerous RDS files. These RDS files can be merged and concatenated, so that we build a single table with all the data.


The output table has index columns: country, year, scenario, sex, age, education.

When values are aggregates, dimensions are set to "total".
"""


from etl.helpers import PathFinder, create_dataset

from .shared import concatenate_tables, make_scenario_tables, read_data_from_snap

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SCENARIOS_EXPECTED = {
    "ssp1",
    "ssp2",
    "ssp3",
    "ssp4",
    "ssp5",
}

# Renaming of relevant columns
COLUMNS_RENAME = {
    "name": "country",
    "period": "year",
}

# Harmonization of the dimension values
REPLACE_AGE = {
    "all": "total",
}
REPLACE_SEX = {
    "both": "total",
}


# First table contains education = "all", the other the rest
TABLES_COMBINE_EDUCATION = [
    ("asfr", "easfr"),
    ("assr", "eassr"),
    # ("macb", "emacb"),
    ("tfr", "etfr"),
    ("bpop", "epop"),
]

TABLES_CONCAT = [
    # ("prop", "bprop"),
    ("mys", "bmys"),
]
TABLES_DROP = []

# Composition of tables
TABLES_COMPOSITION = {
    # 0/ No dimension
    "main": {"cbr", "cdr", "ggapmys15", "ggapmys25", "growth", "mage", "nirate", "odr", "ydr", "tdr", "macb"},
    # 1/ Sex dimension. NOTE: no sex=total
    "by_sex": {"e0", "pryl15", "ryl15"},
    # 1/ Age dimension
    "by_age": {"sexratio"},
    # 1/ Education dimension
    "by_edu": {"ggapedu15", "ggapedu25", "tfr"},
    # 2/ Sex+Age dimensions. NOTE: no age=total
    "by_sex_age": {"mys", "net"},
    # 2/ Age+Education dimensions. NOTE: no age=total, that's fine. We have tfr for all ages actually.
    "by_age_edu": {"asfr"},
    # 3/ Sex+Age+Education dimensions
    "by_sex_age_edu": {"assr", "pop"},
}


def run(dest_dir: str) -> None:
    """Overall, this step could take 6:30 minutes on my machine (lucas)."""
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("wittgenstein_human_capital_historical.zip")

    # Load data from snapshot. {"1": [tb1, tb2, ...], "2": [tb1, tb2, ...], ...}
    # ~ 1:20 minutes (2 minutes if all scenarios are used)
    tbs_scenario = read_data_from_snap(snap, SCENARIOS_EXPECTED)

    #
    # Process data.
    #
    # Consolidate individual scenario tables: {"main": [tb_main1, tb_main2, ...], "by_sex": [tb_sex1, tb_sex2, ...], ...}
    # ~ 3 minutes (4:30 if all scenarios are used)
    # dix = {k: v for k, v in tbs_scenario.items() if k == "ssp1"}
    # tbs_scenario_ = make_scenario_tables(dix)
    tables = make_scenario_tables(
        tbs_scenario=tbs_scenario,
        tables_combine_edu=TABLES_COMBINE_EDUCATION,
        tables_concat=TABLES_CONCAT,
        tables_drop=TABLES_DROP,
        tables_composition=TABLES_COMPOSITION,
    )

    # Filter out rows that are >2020
    year_ranges_ignore = [f"{i}-{i+5}" for i in range(2021, 2101, 5)] + [f"{i}.0-{i+5}.0" for i in range(2020, 2101, 5)]
    year_ranges_ignore += [f"{i}.0" for i in range(2021, 2101)] + [f"{i}" for i in range(2020, 2101)]

    for tname, tbs in tables.items():
        for i, tb in enumerate(tbs):
            # Remove unwanted years
            tb = tb.loc[~(tb["year"].isin(year_ranges_ignore))]
            # Remove duplicate rows
            # An alternative to solve this is: drop age=All in bpop table.
            # if tname == "by_sex_age_edu":
            #     cols = ["country", "year", "age", "education", "sex", "scenario"]
            #     x = tb.groupby(["country", "year", "age", "education", "sex", "scenario"], as_index=False).size()
            #     x = x[x["size"] > 1]
            #     assert set(x["year"].unique()) == {"2015"}
            #     assert set(x["age"].unique()) == {"total"}
            #     assert set(x["sex"].unique()) == {"male"}
            #     assert set(x["education"].unique()) == {"total"}
            #     tb = tb.drop_duplicates(subset=cols)

            # Overwrite
            tables[tname][i] = tb

    # Remove duplicates

    # Concatenate: [table_main, table_sex, ...]
    # ~ 2 minutes
    tables = concatenate_tables(tables)

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=snap.metadata,
    )

    # Save changes in the new meadow dataset.
    ds_meadow.save()

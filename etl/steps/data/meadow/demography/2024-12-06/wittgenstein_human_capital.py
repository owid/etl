"""Load a snapshot and create a meadow dataset.

This snapshot step is a bit more complex than usual. This is because the snapshot is a ZIP file that contains numerous RDS files. These RDS files can be merged and concatenated, so that we build a single table with all the data.


The output table has index columns: country, year, scenario, sex, age, education.

When values are aggregates, dimensions are set to "total".
"""


from etl.helpers import PathFinder, create_dataset

from .utils import concatenate_tables, make_scenario_tables, read_data_from_snap

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SCENARIOS_EXPECTED = {
    "1",
    "2",
    "22",
    "23",
    "3",
    "4",
    "5",
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
    ("macb", "emacb"),
    # ("net", "netedu"),
    ("tfr", "etfr"),
    ("bpop", "epop"),
]

TABLES_CONCAT = [
    ("prop", "bprop"),
    ("mys", "bmys"),
]
TABLES_DROP = [
    # Several tables are just population!
    "pop",
    "pop-age",
    "pop-age-edattain",
    "pop-age-sex",
    "pop-age-sex-edattain",
    "pop-sex",
    "pop-sex-edattain",
    "pop-total",
]
# Composition of tables
TABLES_COMPOSITION = {
    # 0/ No dimension
    "main": {"cbr", "cdr", "emi", "ggapmys15", "ggapmys25", "growth", "imm", "mage", "nirate", "odr", "ydr", "tdr"},
    # 1/ Sex dimension. NOTE: no sex=total
    "by_sex": {"e0", "pryl15", "ryl15"},
    # 1/ Age dimension
    "by_age": {"sexratio"},
    # 1/ Education dimension
    "by_edu": {"ggapedu15", "ggapedu25", "macb", "net", "tfr"},
    # 2/ Sex+Age dimensions. NOTE: no age=total
    "by_sex_age": {"mys"},
    # 2/ Age+Education dimensions. NOTE: no age=total, that's fine. We have tfr for all ages actually.
    "by_age_edu": {"asfr"},
    # 3/ Sex+Age+Education dimensions
    "by_sex_age_edu": {"assr", "pop", "prop"},
}


def run(dest_dir: str) -> None:
    """Overall, this step could take 6:30 minutes on my machine (lucas)."""
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("wittgenstein_human_capital.zip")

    # Load data from snapshot. {"1": [tb1, tb2, ...], "2": [tb1, tb2, ...], ...}
    # ~ 1:20 minutes (2 minutes if all scenarios are used)
    tbs_scenario = read_data_from_snap(snap, SCENARIOS_EXPECTED)

    #
    # Process data.
    #
    # Consolidate individual scenario tables: {"main": [tb_main1, tb_main2, ...], "by_sex": [tb_sex1, tb_sex2, ...], ...}
    # ~ 3 minutes (4:30 if all scenarios are used)
    # dix = {k: v for k, v in tbs_scenario.items() if k == "1"}
    # tbs_scenario_ = make_scenario_tables(dix)
    tbs_scenario = make_scenario_tables(
        tbs_scenario=tbs_scenario,
        tables_combine_edu=TABLES_COMBINE_EDUCATION,
        tables_concat=TABLES_CONCAT,
        tables_drop=TABLES_DROP,
        tables_composition=TABLES_COMPOSITION,
    )

    # Concatenate: [table_main, table_sex, ...]
    # ~ 2 minutes
    tables = concatenate_tables(tbs_scenario)

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

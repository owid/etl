"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table
from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("prison_rates.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #
    # Clean Namibia pre-trial data
    tb = clean_namibia_pre_trial(tb)

    # Improve tables format.
    tables = [tb.format(["country", "year"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()


def clean_namibia_pre_trial(tb: Table) -> Table:
    """
    Namibia has two sections to the pre-trial table: https://www.prisonstudies.org/country/namibia
    The upper half do not include pre-trial prisoners detained in police holding cells. Those below the line include all pre-trial remand prisoners.
    Here we make sure just the values below the line are included.
    """
    msk = tb["country"] == "Namibia"
    tb_namib = tb[msk].copy()
    tb_other = tb[~msk]
    pre_trial_cols = ["pretrial_remand_number", "pretrial_remand_pct", "pretrial_remand_rate"]

    # Set pre-trial columns to NA for specific year/pretrial_remand_number pairs
    # These are the upper half values that don't include police holding cells
    pairs_to_exclude = {2001: 250, 2007: 322, 2010: 340, 2015: 234}

    for year, pretrial_num in pairs_to_exclude.items():
        condition = (tb_namib["year"] == year) & (tb_namib["pretrial_remand_number"] == pretrial_num)
        tb_namib.loc[condition, pre_trial_cols] = None
    # Get all columns except country and year
    value_cols = [col for col in tb_namib.columns if col not in ["country", "year"]]

    # Drop rows where all value columns are NA
    tb_namib = tb_namib.dropna(subset=value_cols, how="all")

    # Combine back with other countries
    tb = pr.concat([tb_namib, tb_other], ignore_index=True)

    return tb

"""Load a snapshot and create a meadow dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

column_index = ["country", "year", "indicator", "dimension"]


def run() -> None:
    #
    # Load inputs.
    #
    snap = paths.load_snapshot("unaids_epi.zip")

    short_names = [
        "epi",
        "gam",
        "kpa",
        "ncpi",
    ]
    tables = []
    for short_name in short_names:
        # Retrieve snapshot.
        snap = paths.load_snapshot(f"unaids_{short_name}.zip")

        # Load data from snapshot.
        tb = snap.read()

        #
        # Process data.
        #
        tb = clean_table(tb)
        # Format table
        tb = tb.format(column_index, short_name=short_name)

        # Append current table to list of tables.
        tables.append(tb)

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables)

    # Save meadow dataset.
    ds_meadow.save()


def clean_table(tb):
    """Minor table cleaning."""
    paths.log.info(f"Formatting table {tb.m.short_name}")

    # Rename columns, only keep relevant
    columns = {
        "Indicator": "indicator",
        "Unit": "unit",
        "Subgroup": "dimension",
        "Area": "country",
        # "Area ID" : "",
        "Time period": "year",
        "Source": "source",
        "Data value": "value_raw",
        "Formatted": "value",
        "Data_Denominator": "data_denominator",
        "Footnote": "footnote",
    }
    tb = tb.rename(columns=columns)[columns.values()]

    # Drop duplicates
    tb = tb.drop_duplicates(subset=["country", "year", "indicator", "dimension"], keep="first")

    # Handle NaNs
    tb["value"] = tb["value"].replace("...", np.nan)
    tb = tb.dropna(subset=["value"])

    return tb

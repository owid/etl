"""Load a snapshot and create a meadow dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

SHAPE_EXPECTED = (416, 13)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("religious_composition.zip")

    # Load data from snapshot.
    folder_name = "Religious Composition 2010-2020 dataset"
    files = {
        "share": "Religious Composition 2010-2020 (percentages).csv",
        "count": "Religious Composition 2010-2020 (rounded counts).csv",
        "count_unrounded": "Religious Composition 2010-2020 (unrounded counts).csv",
    }
    tbs = []
    tb_regions = None
    with snap.open_archive():
        for varname, fname in files.items():
            # Read raw data
            tb = snap.read_from_archive(
                f"{folder_name}/{fname}",
            )
            # Extract region information
            if tb_regions is None:
                tb_regions = tb.loc[:, ["Region", "Country", "Level"]]
            # Clean table
            tb = clean_data(
                tb,
                var_name=varname,
            )
            # Append to list
            tbs.append(tb)

    # Concatenate tables
    tb = pr.multi_merge(tbs, on=["Country", "Year", "religion"], how="outer")

    # Sanity check
    assert tb.shape[0] == tbs[0].shape[0]

    #
    # Process data.
    #
    assert tb_regions is not None
    tb_regions = (
        tb_regions.loc[tb_regions["Level"] == 1, ["Region", "Country"]].drop_duplicates().reset_index(drop=True)
    )

    # Improve tables format.
    tables = [
        tb.format(["country", "year", "religion"]),
        tb_regions.format(["country"], short_name="pew_regions"),
    ]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(
        tables=tables,
        default_metadata=snap.metadata,
    )

    # Save meadow dataset.
    ds_meadow.save()


def clean_data(tb, var_name):
    tb = tb.dropna(how="all", axis=1)

    # Sanity checks
    ## Shape
    assert tb.shape == SHAPE_EXPECTED, f"Expected shape: {SHAPE_EXPECTED}. Found: {tb.shape}"
    ## Columns
    columns_religion = {
        "Buddhists",
        "Christians",
        "Hindus",
        "Jews",
        "Muslims",
        "Other_religions",
        "Religiously_unaffiliated",
    }
    columns_expected = {
        "Country",
        "Countrycode",
        "Level",
        "Population",
        "Region",
        "Year",
    } | columns_religion
    assert set(tb.columns) == columns_expected, "Unexpected columns found."

    ## Values
    assert set(tb["Level"].unique()) == {1, 2, 3}, "Unexpected values in 'Level' column."

    # Re-format
    tb = tb.melt(id_vars=["Country", "Year"], value_vars=columns_religion, var_name="religion", value_name=var_name)
    return tb

"""Load a snapshot and create a meadow dataset."""

from owid.catalog import Table

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("homicide.xlsx")

    # Load data from snapshot.
    tb = snap.read(skiprows=2, sheet_name="data_cts_intentional_homicide")

    tb = clean_data(tb)
    # Process data.
    #
    # Improve tables format.
    tables = [tb.format(["country", "year", "indicator", "dimension", "category", "sex", "age", "unit_of_measurement"])]

    #
    # Save outputs.
    #
    # Initialize a new meadow dataset.
    ds_meadow = paths.create_dataset(tables=tables, default_metadata=snap.metadata)

    # Save meadow dataset.
    ds_meadow.save()


def clean_data(tb: Table) -> Table:
    tb = tb[
        (tb["Dimension"].isin(["Total", "by mechanisms", "by relationship to perpetrator", "by situational context"]))
        & (
            tb["Indicator"].isin(
                ["Victims of intentional homicide", "Victims of Intentional Homicide - Regional Estimate"]
            )
        )
    ]
    tb = tb.rename(
        columns={
            "Country": "country",
            "Year": "year",
        },
        errors="raise",
    )
    tb = tb.drop(columns=["Iso3_code", "Region", "Subregion"])
    return tb

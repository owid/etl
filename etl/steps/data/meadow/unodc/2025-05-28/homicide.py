"""Load a snapshot and create a meadow dataset."""

import numpy as np
from owid.catalog import Table
from owid.catalog import processing as pr
from owid.catalog.utils import underscore

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
    tb_region = snap.read(
        skiprows=2,
        sheet_name="data_cts_homicide_reg_estimates",
    )

    tb = clean_data(tb)
    # The regional data has camel_case column names, so we need to convert them to snake_case.
    for col in tb.columns:
        new_col = underscore(col)
        tb = tb.rename(columns={col: new_col})
    tb_region = clean_data_regional(tb_region)

    tb = pr.concat([tb, tb_region], ignore_index=True)
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


def clean_data_regional(tb_region: Table) -> Table:
    tb_region = tb_region[
        tb_region["indicator"].isin(
            ["Victims of intentional homicide", "Victims of Intentional Homicide - Regional Estimate"]
        )
    ]
    tb_region = tb_region.rename(
        columns={
            "geo": "country",
        },
        errors="raise",
    )
    tb_region["dimension"] = "Total"
    tb_region["category"] = "Total"
    tb_region["age"] = "Total"
    tb_region["unit_of_measurement"] = np.where(
        tb_region["series"] == "Number of victims of intentional homicide",
        "Counts",
        np.where(
            tb_region["series"] == "Victims of intentional homicide per 100,000 population",
            "Rate per 100,000 population",
            "Unknown",
        ),
    )
    assert sum(tb_region["unit_of_measurement"] == "Unknown") == 0, "Unexpected unit_of_measurement value in tb_region"
    # Drop columns that aren't in tb and aren't needed for the final table.
    tb_region = tb_region.drop(columns=["note", "obs_status", "series"])
    return tb_region

"""Load a snapshot and create a meadow dataset."""

from datetime import datetime

import owid.catalog.processing as pr
import pandas as pd

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

YEAR_NOW = datetime.now().year


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("marriage_divorce_rates.xlsx")

    # Load data from snapshot.
    tb_marriage_rates = snap.read(sheet_name="MarriageRates", header=3)
    tb_divorce_rates = snap.read(sheet_name="DivorceRates", header=3)
    tb_mean_age_first_marriage = snap.read(sheet_name="MeanAgeFirstMarriage", header=3)

    #
    # Process data.
    #
    tbs = []
    tables_info = [
        (tb_marriage_rates, "marriage_rate"),
        (tb_divorce_rates, "divorce_rate"),
        (tb_mean_age_first_marriage, "mean_age_first_marriage"),
    ]

    for tb, value_name in tables_info:
        # Get year columns that exist - check both string and integer formats
        year_cols = []
        for col in tb.columns:
            col_str = str(col)
            if col_str.isdigit() and len(col_str) == 4 and 2001 <= int(col_str) <= YEAR_NOW:
                year_cols.append(col)

        # Select relevant columns
        cols_to_keep = ["Country"] + year_cols
        if "Gender" in tb.columns:
            cols_to_keep = cols_to_keep + ["Gender"]
            id_vars = ["Country", "Gender"]
        else:
            id_vars = ["Country"]

        tb = tb[cols_to_keep]
        tb["Country"] = tb["Country"].ffill()
        tb = tb.dropna(subset=year_cols, how="all")

        # Melt the data so year becomes a column
        tb = tb.melt(
            id_vars=id_vars,
            value_vars=year_cols,
            var_name="year",
            value_name="value",
        )
        tb["indicator"] = value_name
        if "Gender" not in tb.columns:
            tb["Gender"] = "Both"
        tbs.append(tb)

    tb = pr.concat(tbs, ignore_index=True)
    # Clean the value column - replace placeholders with NaN and convert to numeric
    tb["value"] = tb["value"].replace(["..", "—", "-", "…", "nan", ""], None)
    tb["value"] = pd.to_numeric(tb["value"], errors="coerce")

    # Ensure all columns are snake-case, set an appropriate index, and sort conveniently.
    tb["value"].metadata.origins = [snap.metadata.origin]
    tb = tb.format(["country", "year", "gender", "indicator"])

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

"""Load a snapshot and create a meadow dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("bmj_2022.csv")

    # Load data from snapshot.
    tb = snap.read()

    # Process data.
    #

    # Melting the 'statin_utilization' columns and extracting the year
    melted_statin_utilization = pd.melt(
        tb,
        id_vars=["country"],
        value_vars=["statin_utilization_october_2019", "statin_utilization_september_2020"],
        var_name="indicator",
        value_name="value",
    )
    # Note: The 2019 data includes estimates of statin utilization from October 2019 to March 2020, while the 2020 data pertains to assessments conducted from April 2020 to October 2020.
    melted_statin_utilization["year"] = melted_statin_utilization["indicator"].str.extract(r"(\d+)")
    melted_statin_utilization["indicator"] = "Statin use"
    melted_statin_utilization["value"] = melted_statin_utilization["value"].astype(float)

    # Processing 'health_expenditure_per_capita_2018' and renaming columns
    health_exp_tb = tb[["country", "health_expenditure_per_capita_2018"]].copy()
    health_exp_tb["year"] = 2018
    health_exp_tb["indicator"] = "Health expenditure per capita in 2018"
    health_exp_tb = health_exp_tb.rename(columns={"health_expenditure_per_capita_2018": "value"})
    health_exp_tb["value"] = health_exp_tb["value"].replace(r"[\$,e]", "", regex=True).astype(float)

    # Processing 'ihd_mortality_rate_2019' and renaming columns
    ihd_mort_tb = tb[["country", "ihd_mortality_rate_2019"]].copy()
    ihd_mort_tb["year"] = 2019
    ihd_mort_tb["indicator"] = "IHD mortality rate in 2019"
    ihd_mort_tb = ihd_mort_tb.rename(columns={"ihd_mortality_rate_2019": "value"})
    ihd_mort_tb["value"] = ihd_mort_tb["value"].astype(float)

    # Processing 'statins_essential_medicine_2017' and renaming columns
    essential_med_tb = tb[["country", "statins_essential_medicine_2017"]].copy()
    essential_med_tb["year"] = 2017
    essential_med_tb["indicator"] = "Essential medicine list"
    essential_med_tb = essential_med_tb.rename(columns={"statins_essential_medicine_2017": "value"})
    essential_med_tb["value"].replace("-", np.nan, inplace=True)
    essential_med_tb["value"].replace("No", 0, inplace=True)
    essential_med_tb["value"].replace("Yes", 1, inplace=True)
    essential_med_tb["value"] = essential_med_tb["value"].astype(float)

    # Concatenating processed DataFrames to form a single DataFrame
    merged_df = pd.concat([essential_med_tb, ihd_mort_tb, health_exp_tb, melted_statin_utilization])

    # Harmonizing column names, setting index, and sorting
    merged_df = merged_df.underscore().set_index(["country", "year", "indicator"], verify_integrity=True).sort_index()
    tb = Table(merged_df, short_name=paths.short_name, metadata=snap.to_table_metadata())
    tb["value"].m.origins = [snap.m.origin]

    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()

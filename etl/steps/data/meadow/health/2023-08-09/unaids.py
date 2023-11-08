"""Load a snapshot and create a meadow dataset."""

import ast

import numpy as np
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Logger
log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("unaids.csv")

    # Load data from snapshot.
    log.info("health.unaids: loading data from snapshot")
    tb = snap.read()

    #
    # Process data.
    #
    # Split columns of type dictionary in multiple columns
    log.info("health.unaids: basic table cleaning")
    # Fields as dictionaries
    tb["INDICATOR"] = tb["INDICATOR"].apply(ast.literal_eval)
    tb["SUBGROUP"] = tb["SUBGROUP"].apply(ast.literal_eval)
    tb["AREA"] = tb["AREA"].apply(ast.literal_eval)
    tb["UNIT"] = tb["UNIT"].apply(ast.literal_eval)
    # Add new columns
    tb["INDICATOR_DESCRIPTION"] = tb["INDICATOR"].apply(lambda x: x["value"])
    tb["INDICATOR"] = tb["INDICATOR"].apply(lambda x: x["id"])
    tb["SUBGROUP_DESCRIPTION"] = tb["SUBGROUP"].apply(lambda x: x["value"])
    tb["SUBGROUP"] = tb["SUBGROUP"].apply(lambda x: x["id"])
    tb["COUNTRY"] = tb["AREA"].apply(lambda x: x["value"])
    tb["UNIT"] = tb["UNIT"].apply(lambda x: x["id"])
    # Remove unused columns
    tb = tb.drop(columns=["AREA"])

    # Rename column for year
    tb = tb.rename(columns={"TIME_PERIOD": "year"})

    # Underscore columns
    tb = tb.underscore()

    # Remove duplicates
    tb = tb.drop_duplicates(subset=["country", "year", "indicator", "subgroup"], keep="first")

    # Set index
    tb = tb.set_index(["country", "year", "indicator", "subgroup"], verify_integrity=True)

    log.info("health.unaids: fix observed values (NaNs and typos)")
    # Replace '...' with NaN
    tb["obs_value"] = tb["obs_value"].replace("...", np.nan)
    tb["obs_value"] = tb["obs_value"].replace("3488-56", np.nan)
    # Remove unwanted indicators
    log.info("health.unaids: remove unwanted indicators")
    id_desc_rm = [
        "National AIDS strategy/policy",
        "National AIDS strategy/policy includes dedicated budget for gender transformative interventions",
    ]
    tb = tb[~tb["indicator_description"].isin(id_desc_rm)]
    # Type
    tb = tb.astype(
        {
            "obs_value": float,
        }
    )
    #
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_meadow.save()

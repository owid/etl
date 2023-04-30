"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("wb_income: start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow: Dataset = paths.load_dependency("wb_income")

    # Read table from meadow dataset.
    tb_meadow = ds_meadow["wb_income"]

    # Create a dataframe with data from the table.
    df = pd.DataFrame(tb_meadow)

    #
    # Process data.
    #
    log.info("wb_income.harmonize_countries")
    df = geo.harmonize_countries(df=df, countries_file=paths.country_mapping_path)
    # Harmonize income group labels
    df = harmonize_income_group_labels(df)
    # Set index, drop code column
    df = df.drop(columns=["country_code"]).set_index(["country", "year"])
    # Create a new table with the processed data.
    tb_garden = Table(df, short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_garden], default_metadata=ds_meadow.metadata)
    ds_garden.update_metadata(paths.metadata_path)
    # Save changes in the new garden dataset.
    ds_garden.save()

    log.info("wb_income: end")


def harmonize_income_group_labels(df: pd.DataFrame) -> pd.DataFrame:
    # Check raw labels are as expected
    log.info("wb_income: harmonizing classification labels (e.g. 'LM' -> 'Low-income countries')")
    assert (labels := set(df["classification"])) == {
        "..",
        "H",
        "L",
        "LM",
        "LM*",
        "UM",
        np.nan,
    }, f"Unknown income group label! Check {labels}"
    # Check unusual LM* label
    msk = df["classification"] == "LM*"
    lm_special = set(df[msk]["country_code"].astype(str) + df[msk]["year"].astype(str))
    assert lm_special == {"YEM1987", "YEM1988"}, f"Unexpected entries with classification 'LM*': {df[msk]}"
    # Rename labels
    MAPPING_CLASSIFICATION = {
        "..": np.nan,
        "L": "Low-income countries",
        "H": "High-income countrires",
        "UM": "Upper-middle-income countries",
        "LM": "Lower-middle-income countries",
        "LM*": "Lower-middle-income countries",
    }
    df["classification"] = df["classification"].map(MAPPING_CLASSIFICATION)
    # Drop years with no country classification
    df = df.dropna(subset="classification")
    return df

"""
Load the three LIS snapshots and creates three tables in the luxembourg_income_study meadow dataset.
Country names are converted from iso-2 codes in this step and years are reformated from "YY" to "YYYY"
"""


from typing import Dict

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table

from etl.helpers import PathFinder
from etl.steps.data.converters import convert_snapshot_metadata

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Create a dictionary with the names of the snapshots and their id variables
SNAPSHOTS_DICT = {
    "lis_keyvars": ["country", "year", "dataset", "variable", "eq"],
    "lis_abs_poverty": ["country", "year", "dataset", "variable", "eq", "povline"],
    "lis_distribution": ["country", "year", "dataset", "variable", "eq", "percentile"],
    "lis_percentiles": ["country", "year", "dataset", "variable", "eq", "percentile"],
}

# Define a dictionary with the age suffixes for the different snapshots
AGE_DICT = {"all": "", "adults": "_adults"}

# Define a list of datasets to drop
# NOTE: These datasets are dropped because LIS decided to not show key indicators. From a conversation with LIS:
"""
AT87 is still accessible for ongoing research, but we advise not to use it.
The years FR78/FR89/FR94 are not wrong per-se, but inequality numbers are rather different from the HBS based series, and we are not promoting both series at this stage. So all data points available through lissydata are based on the taxregister based series.
DE81 is not in line with the EVS data, nor GSOEP (so we equally do not promote these numbers.
ML13, ML11 or SE67 have high proportion of 0s and/or missing values in DHI, so far we do not show statistics for those.
"""

DROP_DATASETS_LIST = ["AT87", "DE81", "FR78", "FR89", "FR94", "ML11", "ML13", "SE67"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    # Load reference file with country names in OWID standard
    ds_regions = paths.load_dataset("regions")
    tb_countries_regions = ds_regions["regions"].reset_index()

    # Only keep the columns we need
    tb_countries_regions = tb_countries_regions[["iso_alpha2", "name"]]

    # Create a new meadow dataset with the same metadata as the snapshot.
    snap = paths.load_snapshot("lis_keyvars.csv")
    ds_meadow = Dataset.create_empty(dest_dir, metadata=convert_snapshot_metadata(snap.metadata))

    # Ensure the version of the new dataset corresponds to the version of current step.
    ds_meadow.metadata.version = paths.version
    ds_meadow.metadata.short_name = "luxembourg_income_study"

    ds_meadow = edit_snapshots_and_add_to_dataset(
        ds_meadow=ds_meadow, age_dict=AGE_DICT, snapshots_dict=SNAPSHOTS_DICT, tb_countries_regions=tb_countries_regions
    )

    # Save changes in the new garden dataset.
    ds_meadow.save()


def edit_snapshots_and_add_to_dataset(
    ds_meadow: Dataset, age_dict: Dict[str, str], snapshots_dict: Dict[str, list], tb_countries_regions: Table
) -> Dataset:
    """
    Format year and country of each table and add them to the meadow dataset.
    """

    for age, age_suffix in age_dict.items():
        for tb_name, tb_ids in snapshots_dict.items():
            # Retrieve snapshot.
            snap = paths.load_snapshot(f"{tb_name}{age_suffix}.csv")
            tb = snap.read(safe_types=False)

            tb[[col for col in tb.columns if col not in tb_ids]] = tb[
                [col for col in tb.columns if col not in tb_ids]
            ].apply(pd.to_numeric, errors="coerce")

            # Drop datasets LIS is not promoting
            tb = tb[~tb["dataset"].isin(DROP_DATASETS_LIST)].reset_index(drop=True)

            # Extract country and year from dataset
            tb["country"] = tb["dataset"].str[:2].str.upper()
            tb["year"] = tb["dataset"].str[2:4].astype(int)

            # Replace "UK" with "GB" (official ISO-2 name for the United Kingdom)
            tb.loc[tb["country"] == "UK", "country"] = "GB"

            # Create year variable in the format YYYY instead of YY
            # Define mask
            mask = tb["year"] < 50
            tb.loc[mask, "year"] = tb.loc[mask, "year"] + 2000
            tb.loc[~mask, "year"] = tb.loc[~mask, "year"] + 1900

            # Merge dataset and country dictionary to get the name of the country (and rename it as "country")
            tb = pr.merge(
                tb,
                tb_countries_regions,
                left_on="country",
                right_on="iso_alpha2",
                how="left",
            )

            # Assert if there are missing values in the name column and show which countries are missing
            missing_countries = tb[tb["name"].isnull()]["country"].unique()
            assert (
                len(missing_countries) == 0
            ), f"Missing countries in the table {tb_name}{age_suffix}: {missing_countries}"

            tb = tb.drop(columns=["country", "iso_alpha2"])
            tb = tb.rename(columns={"name": "country"})

            # Set indices and sort.
            tb = tb.format(
                tb_ids,
                short_name=f"{tb_name}{age_suffix}",
            )

            # Add the new table to the meadow dataset.
            ds_meadow.add(tb)

    return ds_meadow

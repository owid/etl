"""Load a snapshot and create a meadow dataset."""
import numpy as np
from owid.catalog import Table
from owid.catalog import processing as pr
from owid.catalog.utils import underscore
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

log = get_logger()


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("who.csv")
    snap_regions = paths.load_snapshot("who_regions.csv")
    tb = snap.read()
    tb_reg = snap_regions.read()
    # Prepare data.
    tb = tb.drop(columns=["iso3"], axis=1)
    tb_reg["name"] = tb_reg["region"] + "-" + tb_reg["region_type"]
    tb_reg = tb_reg.drop(columns=["region", "region_type"], axis=1)

    # Combine national and regional data
    tb = pr.concat([tb, tb_reg])
    tb = tb.set_index(["name", "year", "residence"])
    # Save outputs.
    #
    # Create a new meadow dataset with the same metadata as the snapshot.
    ds_meadow = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new meadow dataset.
    ds_meadow.save()


def combine_headers(tb: Table) -> Table:
    tb.columns = [
        "_".join([part.replace("\n", " ").strip() for part in col if "Unnamed" not in part]) for col in tb.columns
    ]
    return tb


def prepare_data(tb: Table, sheet_name: str):
    tb = combine_headers(tb)
    tb.metadata.short_name = underscore(sheet_name)
    # Several empty lines in the regional data is filled with '-'
    tb = tb.replace("-", np.nan)
    tb = tb.dropna(subset="Year")
    # Each sheet has a different column name for country.
    tb = tb.rename(
        columns={
            "Year": "year",
            "MENSTRUAL HEALTH_COUNTRY, AREA OR TERRITORY": "country",
            "DRINKING WATER_COUNTRY, AREA OR TERRITORY": "country",
            "SANITATION_COUNTRY, AREA OR TERRITORY": "country",
            "HYGIENE_COUNTRY, AREA OR TERRITORY": "country",
            "DRINKING WATER_REGION": "country",
            "SANITATION_REGION": "country",
            "HYGIENE_REGION": "country",
        }
    )
    # Dropping ISO3 columns.
    columns_to_drop = [col for col in tb.columns if "ISO3" in col]
    tb = tb.drop(columns=columns_to_drop, axis=1)

    # Sorting dtypes to avoid errors when saving.
    tb.iloc[:, 2:] = tb.iloc[:, 2:].astype(str)  # String for now due to some usage of - and < in the data.
    tb["country"] = tb["country"].astype(str)
    tb["year"] = tb["year"].astype(int)
    return tb

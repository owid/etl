"""Load a meadow dataset and create a garden dataset."""

import numpy as np
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("polio_vaccine_schedule")

    # Read table from meadow dataset.
    tb = ds_meadow["polio_vaccine_schedule"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Apply the function across the DataFrame rows
    tb["vaccine_schedule"] = tb.apply(categorize_schedule, axis=1)
    tb["vaccine_schedule"] = tb["vaccine_schedule"].copy_metadata(tb["schedulercode_ipv"])
    tb = tb[["country", "year", "vaccine_schedule"]]
    tb = tb.format(["country", "year"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def classify_vaccines_used(tb: Table) -> Table:
    """
    For each country year pair, classify the vaccines used in the country's schedule.
    Classifications are based on the vaccines' scheduler codes. The output we want is:
    - IPV only
    - OPV only
    - IPV and OPV
    """
    tb[""]
    tb.mask(tb.notna(), 1)


# Define the custom function to apply to each row using string comparisons
def categorize_schedule(row):
    has_ipv_or_ipvf = row["schedulercode_ipv"] == "IPV" or row["schedulercode_ipvf"] == "IPVf"
    has_opv = row["schedulercode_opv"] == "OPV"

    if has_opv:
        if has_ipv_or_ipvf:
            return "Both"  # 'IPV' in ipv/ipvf and 'OPV' in opv
        else:
            return "OPV"  # Only 'OPV' in opv
    else:
        if has_ipv_or_ipvf:
            return "IPV"  # 'IPV' in ipv/ipvf and no 'OPV' in opv
        else:
            return np.nan  # No 'IPV' or 'OPV' (unlikely to happen, but just in case)

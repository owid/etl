"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("polio_vaccine_schedule")

    # Read table from meadow dataset.
    tb = ds_meadow.read("polio_vaccine_schedule")

    sanity_check_inputs(tb)

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb)

    # Apply the function across the DataFrame rows
    tb["vaccine_schedule"] = tb.apply(categorize_schedule, axis=1)
    tb["vaccine_schedule"] = tb["vaccine_schedule"].copy_metadata(tb["schedulercode_ipv"])
    tb = tb[["country", "year", "vaccine_schedule"]]

    # Some raw entities harmonize to the same OWID country (e.g. the three Caribbean Netherlands
    # municipalities -> "Bonaire Sint Eustatius and Saba", reported separately by WHO). Collapse
    # exact duplicates, then assert no (country, year) pair is left with disagreeing values -- a
    # silent conflict there would otherwise be dropped by tb.format()'s uniqueness check.
    tb = tb.drop_duplicates(subset=["country", "year", "vaccine_schedule"]).reset_index(drop=True)
    conflicts = tb[tb.duplicated(subset=["country", "year"], keep=False)]
    assert conflicts.empty, (
        "Conflicting vaccine_schedule values for the same (country, year) after harmonizing "
        f"multiple source entities to one country:\n{conflicts.sort_values(['country', 'year'])}"
    )

    sanity_check_outputs(tb)

    tb = tb.format(["country", "year"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()


def sanity_check_inputs(tb) -> None:
    assert set(tb["schedulercode_ipv"].dropna().unique()) <= {"IPV"}, "Unexpected value in schedulercode_ipv."
    assert set(tb["schedulercode_ipvf"].dropna().unique()) <= {"IPVf"}, "Unexpected value in schedulercode_ipvf."
    assert set(tb["schedulercode_opv"].dropna().unique()) <= {"OPV"}, "Unexpected value in schedulercode_opv."
    assert not tb.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows in meadow input."


def sanity_check_outputs(tb) -> None:
    assert set(tb["vaccine_schedule"].dropna().unique()) <= {
        "IPV",
        "OPV",
        "Both IPV and OPV",
    }, "Unexpected category in vaccine_schedule."
    # Coverage shouldn't shrink vs. the previous version (213 countries in who/2025-01-13).
    assert tb["country"].nunique() >= 213, f"Country coverage dropped: {tb['country'].nunique()} < 213."


# Define the custom function to apply to each row using string comparisons
def categorize_schedule(row):
    # Explicit NA checks: schedulercode_* columns can be pd.NA (string[pyarrow]), and
    # `pd.NA == "IPV"` returns pd.NA rather than False, which breaks the `if` below.
    has_ipv_or_ipvf = (pd.notna(row["schedulercode_ipv"]) and row["schedulercode_ipv"] == "IPV") or (
        pd.notna(row["schedulercode_ipvf"]) and row["schedulercode_ipvf"] == "IPVf"
    )
    has_opv = pd.notna(row["schedulercode_opv"]) and row["schedulercode_opv"] == "OPV"

    if has_opv:
        if has_ipv_or_ipvf:
            return "Both IPV and OPV"  # 'IPV' in ipv/ipvf and 'OPV' in opv
        else:
            return "OPV"  # Only 'OPV' in opv
    else:
        if has_ipv_or_ipvf:
            return "IPV"  # 'IPV' in ipv/ipvf and no 'OPV' in opv
        else:
            return np.nan  # No 'IPV' or 'OPV' (unlikely to happen, but just in case)

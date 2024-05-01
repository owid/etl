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
    ds_meadow = paths.load_dataset("ert")

    # Read table from meadow dataset.
    tb = ds_meadow["ert"].reset_index()

    #
    # Process data.
    #
    # Rename columns
    tb = tb.rename(
        columns={
            "country_name": "country",
        }
    )

    # Add regime_ert
    tb = add_regime_indicators(tb)

    # Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
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


def add_regime_indicators(tb: Table) -> Table:
    """Estimate all regime-categorisation-related indicators.

    The source provides one indicator: `reg_type`. We derive four indicators in total:

    - `regime_ert`: a 6-category indicator of the regime type.
    - `regime_dich_ert`: a 2-category indicator of the regime type. Is equivalent to `reg_type`.
    - `regime_trich_ert`: a 3-category indicator of the regime type.
    - `regime_trep_outcome_ert`: a 12-category indicator of the regime type.
    """
    tb = tb.rename(
        columns={
            "reg_type": "regime_dich_ert",
        }
    )

    # Add regime_ert
    column = "regime_ert"
    assert set(tb["dem_ep"]) == {0, 1}, "`dem_ep` must only contain values {0,1}"
    assert set(tb["aut_ep"]) == {0, 1}, "`aut_ep` must only contain values {0,1}"
    tb.loc[(tb["regime_dich_ert"] == 0) & (tb["aut_ep"] == 1) & (tb["dem_ep"] == 0), column] = 0
    tb.loc[(tb["regime_dich_ert"] == 0) & (tb["aut_ep"] == 0) & (tb["dem_ep"] == 0), column] = 1
    tb.loc[(tb["regime_dich_ert"] == 0) & (tb["aut_ep"] == 0) & (tb["dem_ep"] == 1), column] = 2
    tb.loc[(tb["regime_dich_ert"] == 1) & (tb["aut_ep"] == 1) & (tb["dem_ep"] == 0), column] = 3
    tb.loc[(tb["regime_dich_ert"] == 1) & (tb["aut_ep"] == 0) & (tb["dem_ep"] == 0), column] = 4
    tb.loc[(tb["regime_dich_ert"] == 1) & (tb["aut_ep"] == 0) & (tb["dem_ep"] == 1), column] = 5
    tb.loc[(tb["regime_dich_ert"] == 0) & (tb["aut_ep"] == 1) & (tb["dem_ep"] == 1), column] = float("nan")
    tb.loc[(tb["regime_dich_ert"] == 1) & (tb["aut_ep"] == 1) & (tb["dem_ep"] == 1), column] = float("nan")

    # Add regime_trich_ert
    column = "regime_trich_ert"
    tb.loc[(tb["regime_ert"] == 0) | (tb["regime_ert"] == 3), column] = 0
    tb.loc[(tb["regime_ert"] == 1) | (tb["regime_ert"] == 4), column] = 1
    tb.loc[(tb["regime_ert"] == 2) | (tb["regime_ert"] == 5), column] = 2

    # Add regime_trep_outcome_ert
    conditions = [
        (tb["year"] == tb["aut_ep_end_year"]) & (tb["aut_ep_outcome"] == 5),
        (tb["year"] == tb["dem_ep_end_year"]) & (tb["dem_ep_outcome"] == 4),
        (tb["year"] == tb["dem_ep_end_year"]) & (tb["dem_ep_outcome"] == 3),
        (tb["year"] == tb["dem_ep_end_year"]) & (tb["dem_ep_outcome"] == 2),
        (tb["year"] == tb["dem_ep_end_year"]) & (tb["dem_ep_outcome"] == 1),
        (tb["year"] == tb["aut_ep_end_year"]) & (tb["aut_ep_outcome"] == 1),
        (tb["year"] == tb["aut_ep_end_year"]) & (tb["aut_ep_outcome"] == 2),
        (tb["year"] == tb["aut_ep_end_year"]) & (tb["aut_ep_outcome"] == 3),
        (tb["year"] == tb["aut_ep_end_year"]) & (tb["aut_ep_outcome"] == 4),
        (tb["year"] == tb["dem_ep_end_year"]) & (tb["dem_ep_outcome"] == 5),
        (tb["year"] == tb["aut_ep_end_year"]) & (tb["aut_ep_outcome"] == 6),
        (tb["year"] == tb["dem_ep_end_year"]) & (tb["dem_ep_outcome"] == 6),
    ]

    choices = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    tb["regime_trep_outcome_ert"] = np.select(conditions, choices, default=np.nan)

    # Drop unused columns
    tb = tb.drop(columns=["aut_ep_end_year", "dem_ep_end_year", "dem_ep_outcome", "aut_ep_outcome"])

    # Copy metadata from original indicator `regime_dich_ert`
    for column in ["regime_ert", "regime_trich_ert", "regime_trep_outcome_ert"]:
        tb[column] = tb[column].copy_metadata(tb["regime_dich_ert"])

    return tb

"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden datasets.
    ds_long_run = paths.load_dataset("female_labor_force_participation_long_run")
    ds_wdi = paths.load_dataset("wdi")

    # Read table from meadow dataset.
    tb_long_run = ds_long_run.read("female_labor_force_participation_long_run")
    tb_wdi = ds_wdi.read("wdi")

    #
    # Process data.
    #

    # Keep only relevant columns in WDI and rename
    tb_wdi = tb_wdi[["country", "year", "sl_tlf_cact_fe_ne_zs"]].rename(
        columns={"sl_tlf_cact_fe_ne_zs": "female_labor_force_participation"}, errors="raise"
    )

    # Keep only countries available in long-run data
    tb_wdi = tb_wdi[tb_wdi["country"].isin(tb_long_run["country"].unique())].reset_index(drop=True)

    # Merge: outer on years (to keep all years from both sources), but only for long-run countries
    tb = pr.merge(tb_long_run, tb_wdi, on=["country", "year"], how="outer", suffixes=("_long_run", "_wdi"))

    # If wdi data is available, use it. If not, use long run data.
    tb["female_labor_force_participation"] = tb["female_labor_force_participation_wdi"].combine_first(
        tb["female_labor_force_participation_long_run"]
    )

    # Use origins from both sources
    tb["female_labor_force_participation"].m.origins = (
        tb["female_labor_force_participation_wdi"].m.origins + tb["female_labor_force_participation_long_run"].m.origins
    )

    # Drop the original columns
    tb = tb.drop(
        columns=["female_labor_force_participation_long_run", "female_labor_force_participation_wdi"], errors="raise"
    )

    # Improve table format.
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_long_run.metadata)

    # Save garden dataset.
    ds_garden.save()

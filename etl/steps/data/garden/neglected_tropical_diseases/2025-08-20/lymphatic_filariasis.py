"""Load a meadow dataset and create a garden dataset."""

from typing import List

import numpy as np
from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("lymphatic_filariasis")
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Read table from meadow dataset.
    tb = ds_meadow["lymphatic_filariasis"].reset_index()
    #
    # Harmonize countries
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Process data.
    # There are separate rows for each combination of drugs used, but this is duplicate for `national_coverage__pct`, so we will extract this column and create a separate table for it

    # In many cases the are two identical values for 'national_coverage__pct', for each country year, this de-duplicates them
    tb_nat = (
        tb[["country", "year", "national_coverage__pct", "population_requiring_pc_for_lf"]].copy().drop_duplicates()
    )
    tb_nat["estimated_number_of_people_treated"] = (
        tb_nat["national_coverage__pct"] * tb_nat["population_requiring_pc_for_lf"] / 100
    )
    tb_nat = add_regions_to_selected_vars(
        tb_nat,
        cols=["country", "year", "population_requiring_pc_for_lf", "estimated_number_of_people_treated"],
        ds_regions=ds_regions,
    )
    # There are a few cases with two values for some country-year combos, here we drop them because we are not sure which is the correct value
    tb_nat = tb_nat.drop_duplicates(subset=["country", "year"])
    tb_nat.metadata.short_name = "lymphatic_filariasis_national"
    # Drop `national_coverage_pct` from tb
    tb = tb.drop(
        columns=["national_coverage__pct", "population_requiring_pc_for_lf", "region", "country_code", "mapping_status"]
    )
    # Replace "No data" with NaN
    tb = tb.replace("No data", np.nan)
    # Format the tables
    tb = tb.format(["country", "year", "type_of_mda"])
    tb_nat = tb_nat.format(["country", "year"])
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb, tb_nat], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_regions_to_selected_vars(tb: Table, cols: List[str], ds_regions: Dataset) -> Table:
    """
    Adding regions to selected variables in the table and then combining the table with the original table
    """

    tb_agg = geo.add_regions_to_table(
        tb[cols],
        regions=REGIONS,
        ds_regions=ds_regions,
        min_num_values_per_year=1,
    )
    tb_agg = tb_agg[tb_agg["country"].isin(REGIONS)]
    tb = pr.concat([tb, tb_agg], axis=0, ignore_index=True)

    return tb

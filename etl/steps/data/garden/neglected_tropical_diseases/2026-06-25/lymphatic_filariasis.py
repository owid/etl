"""Load a meadow dataset and create a garden dataset."""

import numpy as np
from owid.catalog import Table
from owid.catalog import processing as pr

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
    # Read table from meadow dataset.
    tb = ds_meadow["lymphatic_filariasis"].reset_index()
    #
    # Harmonize countries
    tb = paths.regions.harmonize_names(tb)

    # Process data.
    # There are separate rows for each combination of drugs used, but this is duplicate for `national_coverage__pct`, so we will extract this column and create a separate table for it

    # In Bangladesh before 2010 and India before 2009 they give values for both Albendazole (ALB) + Diethylcarbamazine (DEC) citrate as well as only DEC coverage. We filter here for only the combined coverage, so it is consistent with later years.

    tb = tb[~((tb["country"] == "Bangladesh") & (tb["year"] < 2010) & (tb["type_of_mda"] == "DEC alone"))]
    tb = tb[~((tb["country"] == "India") & (tb["year"] < 2009) & (tb["type_of_mda"] == "DEC alone"))]

    # In many cases the are two identical values for 'national_coverage__pct', for each country year, this de-duplicates them
    tb_nat = (
        tb[["country", "year", "national_coverage__pct", "population_requiring_pc_for_lf"]].copy().drop_duplicates()
    )

    # remove rows without coverage data
    tb_nat = tb_nat.dropna(subset=["national_coverage__pct"])

    # Calculate estimated number of people treated by multiplying the national coverage percentage by the population requiring PC for LF
    tb_nat["estimated_number_of_people_treated"] = (
        tb_nat["national_coverage__pct"] * tb_nat["population_requiring_pc_for_lf"] / 100
    )
    tb_nat = add_regions_to_selected_vars(
        tb_nat,
        cols=["country", "year", "population_requiring_pc_for_lf", "estimated_number_of_people_treated"],
    )

    tb_nat.metadata.short_name = "lymphatic_filariasis_national"
    # Drop `national_coverage_pct` from tb

    tb = tb.drop(
        columns=["national_coverage__pct", "population_requiring_pc_for_lf", "region", "country_code", "mapping_status"]
    )
    # Replace "No data" with NaN
    tb = tb.replace("No data", np.nan)
    # Format the tables
    tb = tb.format(["country", "year", "type_of_mda", "mda_status"])
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


def add_regions_to_selected_vars(tb: Table, cols: list[str]) -> Table:
    """
    Adding regions to selected variables in the table and then combining the table with the original table
    """

    tb_agg = paths.regions.add_aggregates(
        tb[cols],
        regions=REGIONS,
        min_num_values_per_year=1,
    )
    tb_agg = tb_agg[tb_agg["country"].isin(REGIONS)]
    tb = pr.concat([tb, tb_agg], axis=0, ignore_index=True)

    return tb

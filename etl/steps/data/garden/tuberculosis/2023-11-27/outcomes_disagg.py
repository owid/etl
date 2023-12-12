"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Regions to create aggregates for.
REGIONS_TO_ADD = [
    "North America",
    "South America",
    "Europe",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
    "World",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("outcomes_disagg")

    # Read table from meadow dataset.
    tb = ds_meadow["outcomes_disagg"].reset_index()
    # Load regions dataset.
    ds_regions = paths.load_dependency("regions")
    # Load income groups dataset.
    ds_income_groups = paths.load_dependency("income_groups")
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = combining_sexes_for_all_age_groups(tb)
    tb = add_region_sum_aggregates(tb, ds_regions, ds_income_groups)
    tb["tsr"] = tb["tsr"].astype(
        "float16"
    )  # Ensure the column is of type float16 - was getting an error when it was float64
    tb = tb.set_index(["country", "year", "age_group", "sex", "cohort_type"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def combining_sexes_for_all_age_groups(tb: Table) -> Table:
    """
    Not all of the age-groups provided by the WHO have a combined value for both sexes, so we need to combine values for males and females to calculate these.
    """

    tb["age_group"] = tb["age_group"].astype("str")
    age_groups_with_both_sexes = tb[tb["sex"] == "a"]["age_group"].drop_duplicates().to_list()
    msk = tb["age_group"].isin(age_groups_with_both_sexes)
    tb_age = tb[~msk]
    tb_gr = (
        tb_age.groupby(["country", "year", "age_group", "cohort_type"], dropna=False)[
            ["coh", "succ", "fail", "died", "lost", "neval"]
        ]
        .sum()
        .reset_index()
    )
    tb_gr["sex"] = "a"
    tb_gr["tsr"] = (tb_gr["succ"] / tb_gr["coh"]) * 100
    # Set population to nan for rows where the risk factor is not "all"
    tb = pr.concat([tb, tb_gr], axis=0, ignore_index=True, short_name=paths.short_name)

    return tb


def add_region_sum_aggregates(tb: Table, ds_regions: Dataset, ds_income_groups: Dataset) -> Table:
    """
    Calculate region aggregates for all for each combination of age-group, sex and risk factor in the dataset.
    """
    # Create the groups we want to aggregate over.
    tb_gr = tb.groupby(["year", "age_group", "sex", "cohort_type"])
    tb_gr_out = Table()
    for gr_name, gr in tb_gr:
        for region in REGIONS_TO_ADD:
            # List of countries in region.
            countries_in_region = geo.list_members_of_region(
                region=region,
                ds_regions=ds_regions,
                ds_income_groups=ds_income_groups,
            )
            gr_cal = gr[["country", "year", "coh", "succ", "fail", "died", "lost", "neval"]]
            # Add region aggregates.
            gr_reg = geo.add_region_aggregates(
                df=gr_cal,
                region=region,
                countries_in_region=countries_in_region,
                countries_that_must_have_data=[],
                frac_allowed_nans_per_year=0.3,
                num_allowed_nans_per_year=None,
            )
            # Take only region values
            gr_reg = gr_reg[gr_reg["country"] == region]
            # Ensure the region values are assigned the same group values as the original group.
            gr_reg[["age_group", "sex", "cohort_type"]] = [gr_name[1], gr_name[2], gr_name[3]]
            gr_reg["tsr"] = (gr_reg["succ"] / gr_reg["coh"]) * 100
            # Combine the region values with the original group.
            gr = pr.concat([gr, gr_reg], axis=0, ignore_index=True, copy=False)
        # Add the group to the output table.

        tb_gr_out = pr.concat([tb_gr_out, gr], axis=0, ignore_index=True, short_name=paths.short_name)

    return tb_gr_out

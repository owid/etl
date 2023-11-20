"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
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
    ds_meadow = paths.load_dataset("information_capacity_dataset")

    # Read table from meadow dataset.
    tb = ds_meadow["information_capacity_dataset"].reset_index()

    #
    # Process data.
    #

    # Drop country id columns
    tb = tb.drop(columns=["ccodecow", "vdemcode"])

    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Reset index after dropping excluded countries
    tb = tb.reset_index(drop=True)

    # Add regional aggregations
    tb = regional_aggregations(tb)

    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def regional_aggregations(tb: Table) -> Table:
    """
    Add regional aggregations for some of the indicators
    """

    tb_regions = tb.copy()

    # Load population data.
    tb_pop = paths.load_dataset("population")
    tb_pop = tb_pop["population"].reset_index()

    # Merge population data.
    tb_regions = tb_regions.merge(tb_pop[["country", "year", "population"]], how="left", on=["country", "year"])

    # List of index columns
    index_cols = ["censusgraded_ability", "ybcov_ability", "infcap_irt", "infcap_pca"]

    for col in index_cols:
        # Create new columns with the product of the index and the population
        tb_regions[col] = tb_regions[col] * tb_regions["population"]

    # Define regions to aggregate
    regions = [
        "Europe",
        "Asia",
        "North America",
        "South America",
        "Africa",
        "Oceania",
        "High-income countries",
        "Low-income countries",
        "Lower-middle-income countries",
        "Upper-middle-income countries",
        "European Union (27)",
        "World",
    ]

    # Define the variables and aggregation method to be used in the following function loop
    aggregations = dict.fromkeys(
        index_cols + ["population"],
        "sum",
    )

    # Add regional aggregates, by summing up the variables in `aggregations`
    for region in regions:
        tb_regions = geo.add_region_aggregates(
            tb_regions,
            region=region,
            aggregations=aggregations,
            countries_that_must_have_data=[],
        )

    # Filter table to keep only regions
    tb_regions = tb_regions[tb_regions["country"].isin(regions)].reset_index(drop=True)

    # Call the population data again to get regional total population
    tb_regions = tb_regions.merge(
        tb_pop[["country", "year", "population"]], how="left", on=["country", "year"], suffixes=("", "_region")
    )

    # Divide index_cols_pop by population_region to get the index
    for col in index_cols:
        tb_regions[col] = tb_regions[col] / tb_regions["population_region"]

    # # Add missing_pop column, population minus membership_pop and non_membership_pop.
    # tb_regions["missing_pop"] = (
    #     tb_regions["population_region"] - tb_regions["membership_pop"] - tb_regions["non_membership_pop"]
    # )

    # # Assert if missing_pop has negative values
    # if tb_regions["missing_pop"].min() < 0:
    #     paths.log.warning(
    #         f"""`missing_pop` has negative values and will be replaced by 0.:
    #         {print(tb_regions[tb_regions["missing_pop"] < 0])}"""
    #     )
    #     # Replace negative values by 0
    #     tb_regions.loc[tb_regions["missing_pop"] < 0, "missing_pop"] = 0

    # # Include missing_pop in non_membership_pop
    # tb_regions["non_membership_pop"] = tb_regions["non_membership_pop"] + tb_regions["missing_pop"]

    # Drop columns
    tb_regions = tb_regions.drop(columns=["population", "population_region"])

    # Concatenate tb and tb_regions
    tb = pr.concat([tb, tb_regions], ignore_index=True)

    # Make variables integer
    for var in index_cols:
        tb[var] = tb[var].astype("Float64")
        tb[var] = tb[var].replace(np.nan, pd.NA)

    return tb

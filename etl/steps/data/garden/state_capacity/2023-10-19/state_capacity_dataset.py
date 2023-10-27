"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import owid.catalog.processing as pr
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
    ds_meadow = paths.load_dataset("state_capacity_dataset")

    # Read table from meadow dataset.
    tb = ds_meadow["state_capacity_dataset"].reset_index()

    #
    # Process data.

    # Drop columns
    drop_list = ["cntrynum", "iso3", "iso2", "ccode", "scode", "vdem", "wbregion", "sample_polity"]
    tb = tb.drop(columns=drop_list)

    # Convert tax indicators to percentages.
    tax_vars = ["tax_inc_tax", "tax_trade_tax", "taxrev_gdp"]
    tb[tax_vars] *= 100

    # Convert log indicators back to absolute values.
    log_vars = ["milexpercap", "milpercap", "policecap"]

    for var in log_vars:
        tb[var] = tb[var].apply(lambda x: np.exp(x))

    #
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Create regional aggregations.
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
    # Load population data.
    tb_pop = paths.load_dataset("population")
    tb_pop = tb_pop["population"].reset_index()

    tb_regions = tb.copy()

    # Merge population data.
    tb_regions = tb_regions.merge(tb_pop[["country", "year", "population"]], how="left", on=["country", "year"])

    # Create capacity_pop, the product of capacity and population.
    tb_regions["capacity_pop"] = tb_regions["capacity"] * tb_regions["population"]

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

    # Add regional aggregates, by summing up the variables in `aggregations`
    for region in regions:
        tb_regions = geo.add_region_aggregates(
            tb_regions,
            region=region,
            aggregations={"capacity_pop": "sum", "population": "sum"},
            countries_that_must_have_data=[],
            frac_allowed_nans_per_year=0.3,
            population=tb_regions,
        )

    # Keep only regions
    tb_regions = tb_regions[tb_regions["country"].isin(regions)].reset_index(drop=True)

    # Redefine capacity
    tb_regions["capacity"] = tb_regions["capacity_pop"] / tb_regions["population"]

    # Drop capacity_pop and population
    tb_regions = tb_regions.drop(columns=["capacity_pop", "population"])

    # Concatenate tb with tb_regions
    tb = pr.concat([tb, tb_regions], ignore_index=True)

    return tb

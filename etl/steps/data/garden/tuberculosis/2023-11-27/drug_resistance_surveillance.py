"""Load a meadow dataset and create a garden dataset."""
import numpy as np
from owid.catalog import Table
from shared import add_variable_description_from_producer

from etl.data_helpers.geo import add_regions_to_table, harmonize_countries
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

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
    ds_meadow = paths.load_dataset("drug_resistance_surveillance")
    snap = paths.load_snapshot("data_dictionary.csv")
    # Load data dictionary from snapshot.
    dd = snap.read()
    # Load regions dataset.
    ds_regions = paths.load_dependency("regions")
    # Load income groups dataset.
    ds_income_groups = paths.load_dependency("income_groups")
    # Read table from meadow dataset.
    tb = ds_meadow["drug_resistance_surveillance"].reset_index()
    #
    # Process data.
    tb = harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Remove variables `rr_fqr` and `rr_dr_fq` as it's not clear from their desecriptions how they differ from eachother.
    tb = tb.drop(columns=["rr_dr_fq", "rr_fqr"])
    # Adding regional aggregates, setting min_num_values_per_year so missing data isn't falsely given as 0.
    tb = add_regions_to_table(
        tb=tb,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        regions=REGIONS_TO_ADD,
        min_num_values_per_year=1,
    )
    tb = add_variable_description_from_producer(tb, dd)
    tb = sum_hiv_status_for_rifampicin_susceptible(tb)
    tb = calculate_rr_resistance_share(tb)
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


def sum_hiv_status_for_rifampicin_susceptible(tb: Table) -> Table:
    """
    There isn't currently a value for the total number of patients that have been tested for rifampicin susceptibility that are susceptible to rifampicin.
    But this variable is available disggregated by HIV status, so we sum these.
    """
    cols_to_sum = ["nrr_hivneg", "nrr_hivpos", "nrr_hivunk"]

    # Summing the columns, treating NaNs as 0. Unless all values in a row are NaN, then the sum should be NaN.
    tb["nrr_hivall"] = tb[cols_to_sum].sum(axis=1, skipna=True)
    tb["nrr_hivall"] = tb["nrr_hivall"].where(tb[cols_to_sum].notna().any(axis=1), np.nan)

    return tb


def calculate_rr_resistance_share(tb: Table) -> Table:
    """
    Calculating the share of rifampicin resistance among all tested patients.
    """
    tb["rr_new"] = tb["rr_new"].astype(float)
    tb["r_rlt_new"] = tb["r_rlt_new"].astype(float)
    tb["rr_share"] = (tb["rr_new"] / tb["r_rlt_new"]) * 100

    return tb

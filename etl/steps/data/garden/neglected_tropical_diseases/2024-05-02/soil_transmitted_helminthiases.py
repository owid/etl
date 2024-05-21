"""Load a meadow dataset and create a garden dataset."""

from typing import List

from owid.catalog import Dataset, Table
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("soil_transmitted_helminthiases")
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")
    # Read table from meadow dataset.
    tb = ds_meadow["soil_transmitted_helminthiases"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Split out the national coverage variables into separate tables, SAC = school age children, pre-SAC = pre-school age children
    tb_nat_sac = (
        tb[["country", "year", "national_coverage__sac__pct", "population_requiring_pc_for_sth__sac"]]
        .copy()
        .drop_duplicates()
        .dropna(subset=["national_coverage__sac__pct"])
        .drop_duplicates(subset=["country", "year"])
    )
    # Calculating the number of SAC treated
    tb_nat_sac["estimated_number_of_sac_treated"] = (
        tb["national_coverage__sac__pct"] * tb["population_requiring_pc_for_sth__sac"] / 100
    )
    tb_nat_pre_sac = (
        tb[["country", "year", "national_coverage__pre_sac__pct", "population_requiring_pc_for_sth__pre_sac"]]
        .copy()
        .drop_duplicates()
        .dropna(subset=["national_coverage__pre_sac__pct"])
        .drop_duplicates(subset=["country", "year"])
    )
    # Calculating the number of pre-SAC treated
    tb_nat_pre_sac["estimated_number_of_pre_sac_treated"] = (
        tb["national_coverage__pre_sac__pct"] * tb["population_requiring_pc_for_sth__pre_sac"] / 100
    )
    # Adding region aggregates to selected variables
    tb_nat_sac = add_regions_to_selected_vars(
        tb_nat_sac,
        cols=["country", "year", "population_requiring_pc_for_sth__sac", "estimated_number_of_sac_treated"],
        ds_regions=ds_regions,
    )
    tb_nat_pre_sac = add_regions_to_selected_vars(
        tb_nat_pre_sac,
        cols=["country", "year", "population_requiring_pc_for_sth__pre_sac", "estimated_number_of_pre_sac_treated"],
        ds_regions=ds_regions,
    )
    #  Splitting the table into two tables for pre-sac and sac
    age_groups = ["pre_sac", "sac"]
    tbs = {}
    for age_group in age_groups:
        cols = [
            "country",
            "year",
            f"drug_combination__{age_group}",
            f"number_of_{age_group}_targeted",
            f"reported_number_of_{age_group}_treated",
            f"programme_coverage__{age_group}__pct",
        ]
        tbs[f"tb_{age_group}"] = tb[cols].copy()
        tbs[f"tb_{age_group}"].columns = [
            "country",
            "year",
            "drug_combination",
            "number_targeted",
            "reported_number_treated",
            "programme_coverage__pct",
        ]
        tbs[f"tb_{age_group}"] = tbs[f"tb_{age_group}"].dropna(
            subset=[
                "drug_combination",
                "number_targeted",
                "reported_number_treated",
                "programme_coverage__pct",
            ],
        )
        # There are some rows which seem to be erroneous duplicates, we will drop these e.g. Burundi 2015 for sac
        tbs[f"tb_{age_group}"] = tbs[f"tb_{age_group}"].drop_duplicates(subset=["country", "year", "drug_combination"])

    tb_pre_sac = tbs["tb_pre_sac"]
    tb_sac = tbs["tb_sac"]

    tb_sac = tb_sac.format(["country", "year", "drug_combination"], short_name="soil_transmitted_helminthiases_sac")
    tb_pre_sac = tb_pre_sac.format(
        ["country", "year", "drug_combination"], short_name="soil_transmitted_helminthiases_pre_sac"
    )
    tb_nat_sac = tb_nat_sac.format(["country", "year"], short_name="soil_transmitted_helminthiases_national_sac")
    tb_nat_pre_sac = tb_nat_pre_sac.format(
        ["country", "year"], short_name="soil_transmitted_helminthiases_national_pre_sac"
    )
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=[tb_sac, tb_pre_sac, tb_nat_sac, tb_nat_pre_sac],
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
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

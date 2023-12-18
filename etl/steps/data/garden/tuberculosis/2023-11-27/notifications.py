"""Load a meadow dataset and create a garden dataset."""
from owid.catalog import Table
from owid.catalog import processing as pr
from shared import add_variable_description_from_producer, removing_old_variables

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
    ds_meadow = paths.load_dataset("notifications")
    snap = paths.load_snapshot("data_dictionary.csv")
    #
    ds_regions = paths.load_dependency("regions")
    # Load income groups dataset.
    ds_income_groups = paths.load_dependency("income_groups")
    # Load data dictionary from snapshot.
    dd = snap.read()
    # Read table from meadow dataset.
    tb = ds_meadow["notifications"].reset_index()
    #
    # Process data.
    #
    tb = harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = removing_old_variables(tb, dd, dataset_name="Notification")
    tb = add_variable_description_from_producer(tb, dd)
    tb = removing_unclear_variables(tb)
    tb = add_regional_aggregates(tb, ds_regions, ds_income_groups)
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


def removing_unclear_variables(tb: Table) -> Table:
    """
    For some of the variables in the dataset it's not very clear what they mean, or their meaning is contingent on another variable.
    As they are somewhat ancillary variables we have decided to remove them from the dataset.
    """

    cols_to_drop = [
        "newrel_art",
        "newrel_hivpos",
        "newrel_hivtest",
        "mdr_alloral_short_used",
        "mdr_shortreg_used",
        "mdrxdr_dlm_used",
        "mdrxdr_alloral_used",
        "mdrxdr_bdq_used",
        "mdrxdr_bpalm_used",
        "nrr_hpmz_used",
        "rdxsurvey_newinc_rdx",
        "rdxsurvey_newinc",
        "newinc_ep_rdx",
        "newinc_pulm_clindx_rdx",
        "newinc_pulm_labconf_rdx",
        "newinc_rdx",
    ]

    return tb.drop(columns=cols_to_drop)


def add_regional_aggregates(tb: Table, ds_regions: Table, ds_income_groups: Table) -> Table:
    """
    Add regional sum aggregates for the appropriate columns.
    """
    cols_to_drop = ["agegroup_option", "rdx_data_available", "newrel_tbhiv_flg", "tbhiv_014_flg"]

    tb_no_agg = tb[["country", "year"] + cols_to_drop]
    tb_agg = tb.drop(columns=cols_to_drop)

    tb_agg = add_regions_to_table(tb_agg, ds_regions, ds_income_groups, REGIONS_TO_ADD, min_num_values_per_year=1)

    tb = pr.merge(tb_agg, tb_no_agg, on=["country", "year"], how="left")

    return tb

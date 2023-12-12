"""Load a meadow dataset and create a garden dataset."""
from owid.catalog import Table
from shared import add_variable_description_from_producer, removing_old_variables

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("notifications")
    snap = paths.load_snapshot("data_dictionary.csv")
    # Load data dictionary from snapshot.
    dd = snap.read()
    # Read table from meadow dataset.
    tb = ds_meadow["notifications"].reset_index()
    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = removing_old_variables(tb, dd, dataset_name="Notification")
    tb = add_variable_description_from_producer(tb, dd)
    tb = removing_unclear_variables(tb)
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

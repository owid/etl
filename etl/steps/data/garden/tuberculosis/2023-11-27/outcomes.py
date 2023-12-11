"""Load a meadow dataset and create a garden dataset."""
from owid.catalog import Table
from shared import add_variable_description_from_producer

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("outcomes")
    snap = paths.load_snapshot("data_dictionary.csv")
    # Load data dictionary from snapshot.
    dd = snap.read()
    # Read table from meadow dataset.
    tb = ds_meadow["outcomes"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )
    tb = removing_old_variables(tb, dd)
    tb = add_variable_description_from_producer(tb, dd)
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


def removing_old_variables(tb: Table, dd: Table) -> Table:
    """
    There are several variables in this dataset that are recorded as no longer being used after 2011.

    We will remove these as they will be of limited use to us.
    """
    dd = dd[dd["dataset"] == "Outcomes"]
    cols_to_drop = dd["variable_name"][dd["definition"].str.contains("not used after 2011")].to_list()

    tb = tb.drop(columns=cols_to_drop)

    return tb

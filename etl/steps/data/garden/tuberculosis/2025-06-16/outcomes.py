"""Load a meadow dataset and create a garden dataset."""

from shared import add_variable_description_from_producer, removing_old_variables

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("outcomes")
    snap = paths.load_snapshot("data_dictionary.csv")
    # Load data dictionary from snapshot.
    dd = snap.read(safe_types=False)
    # Read table from meadow dataset.
    tb = ds_meadow["outcomes"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = removing_old_variables(tb, dd, dataset_name="Outcomes")
    tb = add_variable_description_from_producer(tb, dd)
    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

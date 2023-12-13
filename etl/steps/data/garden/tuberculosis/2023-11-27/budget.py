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
    ds_meadow = paths.load_dataset("budget")

    snap = paths.load_snapshot("data_dictionary.csv")
    # Read table from meadow dataset.
    tb = ds_meadow["budget"].reset_index()

    dd = snap.read()
    #
    # Process data.
    #
    tb = add_variable_description_from_producer(tb, dd)
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = calculate_budget_gap(tb)
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


def calculate_budget_gap(tb: Table) -> Table:
    """
    Calculating the budget gap for each country-year.

    We substract the total expected funding received from all sources (cf_tot_sources) from the total budget required (budget_tot).
    """

    tb["budget_gap"] = tb["budget_tot"].astype("Int64") - tb["cf_tot_sources"].astype("Int64")

    return tb

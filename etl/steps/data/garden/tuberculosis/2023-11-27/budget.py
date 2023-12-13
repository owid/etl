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
    ds_expense = paths.load_dataset("expenditure")
    snap = paths.load_snapshot("data_dictionary.csv")
    # Read table from meadow dataset.
    tb = ds_meadow["budget"].reset_index()
    tb_exp = ds_expense["expenditure"].reset_index()
    dd = snap.read()
    #
    # Process data.
    #
    tb = add_variable_description_from_producer(tb, dd)
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = calculate_budget_gap(tb, tb_exp)
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


def calculate_budget_gap(tb_budget: Table, tb_exp: Table) -> Table:
    """
    Calculating the budget gap for each country-year.

    We substract the total funding received from all sources (rcvd_tot_sources) from the total budget required (budget_tot).
    """
    tb_exp = tb_exp[["country", "year", "rcvd_tot_sources"]]
    tb_combined = tb_budget.merge(tb_exp, on=["country", "year"], how="left")
    tb_combined["budget_gap"] = tb_combined["budget_tot"] - tb_combined["rcvd_tot_sources"]
    tb_combined = tb_combined.drop(columns=["rcvd_tot_sources"])

    return tb_combined

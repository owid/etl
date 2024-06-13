"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to use from each of the sheets in the data file, and how to rename them.
COLUMNS = {
    "country": "country",
    "year": "year",
    "percent_of_consumer_expenditures_spent_on_food__alcoholic_beverages__and_tobacco_that_were_consumed_at_home__by_selected_countries__share_of_consumer_expenditures_on_food2__percent": "food_as_share_of_all_expenditure",
    "percent_of_consumer_expenditures_spent_on_food__alcoholic_beverages__and_tobacco_that_were_consumed_at_home__by_selected_countries__share_of_consumer_expenditures_on_alcoholic_beverages_and_tobacco__percent": "alcohol_and_tobacco_as_share_of_all_expenditure",
    "percent_of_consumer_expenditures_spent_on_food__alcoholic_beverages__and_tobacco_that_were_consumed_at_home__by_selected_countries__consumer_expenditures3__u_s__dollars_per_person": "consumer_expenditure",
    "percent_of_consumer_expenditures_spent_on_food__alcoholic_beverages__and_tobacco_that_were_consumed_at_home__by_selected_countries__expenditures_on_food2__u_s__dollars_per_person": "food_expenditure",
}


def run_sanity_checks_on_outputs(tb: Table) -> None:
    # Columns of percentages.
    share_columns = [column for column in tb.columns if "share" in column]

    # Columns of expenditures (in dollars).
    dollars_columns = sorted(set(share_columns) - set(["country", "year"]))

    error = "Percentages should be between 0 and 100."
    assert (tb[share_columns] > 0).all().all(), error
    assert (tb[share_columns] < 100).all().all(), error

    error = "Expenditures were expected to be between 0 and 100000$."
    assert (tb[dollars_columns] > 0).all().all(), error
    assert (tb[dollars_columns] < 100000).all().all(), error

    # In principle, food as share of all expenditure should coincide with food expenditure / consumer expenditure * 100.
    # However, in some cases it differs by up to ~6%.
    error = (
        "Food as share of all expenditure should be (almost) equal to food expenditure / consumer expenditure * 100."
    )
    assert (
        abs(tb["food_as_share_of_all_expenditure"] - (tb["food_expenditure"] / tb["consumer_expenditure"] * 100)) < 7
    ).all(), error


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("food_expenditure")
    tb_meadow = ds_meadow["food_expenditure"].reset_index()

    #
    # Process data.
    #
    # Rename columns conveniently.
    tb = tb_meadow.rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Set an appropriate index and sort conveniently.
    tb = tb.format(sort_columns=True)

    # Sanity checks.
    run_sanity_checks_on_outputs(tb=tb)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()

"""Load a meadow dataset and create a garden dataset."""

from owid.datautils.dataframes import combine_two_overlapping_dataframes

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


# Map from columns in the archive dataset to columns in the new dataset.
ARCHIVE_DATA_COLUMNS = {
    "year": "year",
    "household_final_users__food_expenditure_share_of_disposable_personal_income__dpi__1__fah": "Food consumed at home as share of disposable income",
    "household_final_users__food_expenditure_share_of_disposable_personal_income__dpi__fafh": "Food consumed away from home as share of disposable income",
    "household_final_users__food_expenditure_share_of_disposable_personal_income__dpi__all_food": "Food as share of disposable income (at and away from home)",
    "all_purchasers__share_of_food_expenditures__fafh": "Food eaten away from home as share of total food expenditure",
    "all_purchasers__nominal_expenditure_per_capita3__fah": "Food expenditure at home (current prices)",
    "all_purchasers__nominal_expenditure_per_capita__fafh": "Food expenditure away from home (current prices)",
    "all_purchasers__nominal_expenditure_per_capita__all_food": "Food expenditure total (current prices)",
    "all_purchasers__constant_dollar_expenditure_per_capita__1988_100__3__fah": "Food expenditure at home (constant 1988 prices)",
    "all_purchasers__constant_dollar_expenditure_per_capita__1988_100__fafh": "Food expenditure away from home (constant 1988 prices)",
    "all_purchasers__constant_dollar_expenditure_per_capita__1988_100__all_food": "Food expenditure total (constant 1988 prices)",
}

# Map from columns in the recent dataset to columns in the new dataset.
LATEST_DATA_COLUMNS = {
    "year": "year",
    "household_final_users__nominal_food_expenditure_percentage_share_of_disposable_personal_income__dpi__fah": "Food consumed at home as share of disposable income",
    "household_final_users__nominal_food_expenditure_percentage_share_of_disposable_personal_income__dpi__fafh": "Food consumed away from home as share of disposable income",
    "household_final_users__nominal_food_expenditure_percentage_share_of_disposable_personal_income__dpi__all_food": "Food as share of disposable income (at and away from home)",
    "all_purchasers__percentage_share_of_nominal_food_expenditures__fafh": "Food eaten away from home as share of total food expenditure",
    "all_purchasers__nominal_expenditures_per_capita__u_s__dollars__fah": "Food expenditure at home (current prices)",
    "all_purchasers__nominal_expenditures_per_capita__u_s__dollars__fafh": "Food expenditure away from home (current prices)",
    "all_purchasers__nominal_expenditures_per_capita__u_s__dollars__all_food": "Food expenditure total (current prices)",
    "all_purchasers__constant_u_s__dollar_expenditures_per_capita__1988_100__fah": "Food expenditure at home (constant 1988 prices)",
    "all_purchasers__constant_u_s__dollar_expenditures_per_capita__1988_100__fafh": "Food expenditure away from home (constant 1988 prices)",
    "all_purchasers__constant_u_s__dollar_expenditures_per_capita__1988_100__all_food": "Food expenditure total (constant 1988 prices)",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets of archive data and read its main table.
    ds_archive = paths.load_dataset("food_expenditure_in_us_archive")
    tb_archive = ds_archive["food_expenditure_in_us_archive"].reset_index()

    # Load meadow datasets of latest data and read its main table.
    ds_latest = paths.load_dataset("food_expenditure_in_us")
    tb_latest = ds_latest["food_expenditure_in_us"].reset_index()

    #
    # Process data.
    #
    # Rename columns in table of archive data.
    tb_archive = tb_archive[list(ARCHIVE_DATA_COLUMNS)].rename(columns=ARCHIVE_DATA_COLUMNS, errors="raise")

    # Convert all columns in table or archive data to float.
    tb_archive = tb_archive.astype({column: float for column in tb_archive.columns if column != "year"})

    # Add country column to table of archive data.
    tb_archive = tb_archive.assign(**{"country": "United States"})

    # Rename columns in table of latest data.
    tb_latest = tb_latest[list(LATEST_DATA_COLUMNS)].rename(columns=LATEST_DATA_COLUMNS, errors="raise")

    # Convert all columns in table of latest data to float.
    tb_latest = tb_latest.astype({column: float for column in tb_latest.columns if column != "year"})

    # Add country column to table of latest data.
    tb_latest = tb_latest.assign(**{"country": "United States"})

    # Convert "share" columns in table of latest data into a percentage.
    tb_latest[[column for column in tb_latest.columns if "share" in column]] *= 100

    # Ensure columns in both tables of archive and latest data coincide.
    assert set(tb_archive.columns) == set(tb_latest.columns)

    # Combine both tables, prioritizing latest data over archive data.
    tb_combined = combine_two_overlapping_dataframes(tb_latest, tb_archive, index_columns=["country", "year"])

    # Set an appropriate index and sort conveniently.
    tb_combined = tb_combined.format(sort_columns=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb_combined], check_variables_metadata=True)
    ds_garden.save()

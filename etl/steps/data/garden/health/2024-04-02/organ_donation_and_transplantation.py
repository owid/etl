"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Columns to select from the data, and how to rename them.
# NOTE: The meaning of each column name is not explained anywhere, however, in
# https://www.transplant-observatory.org/export-database/
# when selecting "No" to "Export all questions?", a list of variable names appear.
# They seem to map well to the column names.
COLUMNS = {
    "country": "country",
    "reportyear": "year",
    # Annual organ transplantation activity.
    # Actual deceased organ donors.
    # Total number of actual deceased organ donors.
    "total_actual_dd": "n_organ_donors",
    # Number of actual donors after brain death (DBD), i.e., actual deceased organ donors in whom death has been determined by neurological criteria.
    "actual_dbd": "n_donors_after_brain_death",
    # Number of actual donors after circulatory death (DCD), i.e., actual deceased organ donors in whom death has been determined by circulatory criteria.
    "actual_dcd": "n_donors_after_circulatory_death",
    # Total number of utilized deceased organ donors.
    "total_utilized_dd": "n_utilized_organ_donors",
    # Number of utilized donors after brain death (DBD), i.e., utilized deceased organ donors in whom death has been determined by neurological criteria.
    "utilized_dbd": "n_utilized_donors_after_brain_death",
    # Number of utilized donors after circulatory death (DCD), i.e., utilized deceased organ donors in whom death has been determined by circulatory criteria.
    "utilized_dcd": "n_utilized_donors_after_circulatory_death",
    # Transplantation activity.
    # Kidney.
    # Kidney from deceased persons.
    "dd_kidney_tx": "n_kidney_transplantation_from_deceased_persons",
    # Kidney from living persons.
    "ld_kidney_tx": "n_kidney_transplantation_from_living_persons",
    # Total kidney transplantation (deceased, living and domino).
    "total_kidney_tx": "n_kidney_transplantation",
    # Liver.
    # Liver from deceased persons.
    "dd_liver_tx": "n_liver_transplantation_from_deceased_persons",
    # Liver from living persons.
    "ld_liver_tx": "n_liver_transplantation_from_living_persons",
    # Liver domino.
    "domino_liver_tx": "n_domino_liver_transplantation",
    # Total liver transplantation (deceased, living and domino).
    "total_liver_tx": "n_liver_transplantation",
    # Heart.
    # Total heart transplantation.
    "total_heart_tx": "n_heart_transplantation",
    # Lung.
    # Lung from deceased persons.
    "dd_lung_tx": "n_lung_transplantation_from_deceased_persons",
    # Lung from living persons.
    "ld_lung_tx": "n_lung_transplantation_from_living_persons",
    # Total lung transplantation (deceased and living).
    "total_lung_tx": "n_lung_transplantation",
    # Pancreas.
    # Total pancreas transplantation.
    "pancreas_tx": "n_pancreas_transplantation",
    # Kidney-pancreas transplantation.
    "kidney_pancreas_tx": "n_kidney_pancreas_transplantation",
    # Small bowel.
    # Total small bowel transplantation.
    "small_bowel_tx": "n_small_bowel_transplantation",
}


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("organ_donation_and_transplantation")
    tb = ds_meadow["organ_donation_and_transplantation"].reset_index()

    # Load population dataset and read its main table.
    ds_population = paths.load_dataset("population")
    tb_population = ds_population["population"].reset_index()

    #
    # Process data.
    #
    # Select and rename columns.
    tb = tb[COLUMNS.keys()].rename(columns=COLUMNS, errors="raise")

    # Harmonize country names.
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    # Add population to main table.
    tb = geo.add_population_to_table(tb=tb, ds_population=ds_population)

    # Add indicators per million people.
    for column in tb.drop(columns=["country", "year"]).columns:
        tb[f"{column}_per_million_people"] = tb[column] / tb["population"] * 1e6

    # Set an index and sort.
    tb = tb.format()

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()

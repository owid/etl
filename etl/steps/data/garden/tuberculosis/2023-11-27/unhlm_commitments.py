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
    ds_meadow = paths.load_dataset("unhlm_commitments")
    snap = paths.load_snapshot("data_dictionary.csv")
    # Load data dictionary from snapshot.
    dd = snap.read()
    # Read table from meadow dataset.
    tb = ds_meadow["unhlm_commitments"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    tb = add_variable_description_from_producer(tb, dd)
    tb = add_meaning_to_codes(tb)
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


def add_meaning_to_codes(tb: Table) -> Table:
    """
    Adding the values to the coded values based on the data dictionary.
    """

    # Columns where 0 = No, 1 = Yes & 3 = Don't know
    cols_0_1_3 = [
        "annual_report_published",
        "cash_trans",
        "enable_tx_adherence",
        "food_security",
        "free_access_tbdx",
        "free_access_tbtx",
        "ms_review",
        "ms_review_civil_soc",
        "protect_employment",
        "protect_housing",
        "protect_movement",
        "protect_parenting",
        "social_protn",
    ]

    # Columns where 2 = Not applicable, 6 = Not engaged, 230=Advocacy information education and communication; 231=TB prevention and care; 232=Patient support including economic social or nutritional benefits

    cols_other = [
        "min_agg_collab",
        "min_def_collab",
        "min_dev_collab",
        "min_edu_collab",
        "min_fin_collab",
        "min_jus_collab",
        "min_lab_collab",
        "min_tra_collab",
    ]

    tb[cols_0_1_3] = tb[cols_0_1_3].astype("category").replace({0: "No", 1: "Yes", 3: "Don't know"})
    tb[cols_other] = (
        tb[cols_other]
        .astype("object")
        .replace(
            {
                2: "Not applicable",
                6: "Not engaged",
                230: "Advocacy information education and communication",
                231: "TB prevention and care",
                232: "Patient support including economic social or nutritional benefits",
            }
        )
    )

    return tb

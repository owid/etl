"""Load a meadow dataset and create a garden dataset."""

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_cder = paths.load_dataset("cder_approvals")
    # ds_drugs_fda = paths.load_dataset("drugs_approvals")
    ds_orange_book = paths.load_dataset("orange_book")
    ds_purple_book = paths.load_dataset("purple_book")

    # Read table from meadow dataset.
    tb_cder = ds_cder.read("cder_approvals")
    # tb_drugs_fda_sub = ds_drugs_fda.read("drugs_fda_submissions")
    # tb_drugs_fda_products = ds_drugs_fda.read("drugs_fda_products")
    # tb_open_fda = ds_drugs_fda.read("drugs_open_fda")
    tb_orange_book = ds_orange_book.read("orange_book")
    tb_purple_book = ds_purple_book.read("purple_book")

    #
    # Process data.
    #
    # Harmonize country names.
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb])

    # Save garden dataset.
    ds_garden.save()

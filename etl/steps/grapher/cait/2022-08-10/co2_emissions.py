from owid import catalog

from etl import grapher_helpers as gh
from etl.paths import DATA_DIR

from .shared import GARDEN_DATASET_VERSION, NAMESPACE

# Name of table to load from garden dataset to convert into a grapher dataset.
TABLE_NAME = "carbon_dioxide_emissions_by_sector"
# Name of output grapher dataset.
GRAPHER_DATASET_TITLE = "Carbon dioxide emissions by sector (CAIT, 2022)"
# Path to garden dataset to be loaded.
DATASET_PATH = DATA_DIR / "garden" / NAMESPACE / GARDEN_DATASET_VERSION / "ghg_emissions_by_sector"


def run(dest_dir: str) -> None:
    garden_dataset = catalog.Dataset(DATASET_PATH)
    dataset = catalog.Dataset.create_empty(dest_dir, gh.adapt_dataset_metadata_for_grapher(garden_dataset.metadata))

    ####################################################################################################################
    # Grapher seems to be taking the name from the dataset instead of the table.
    # Given that there are different tables in the same dataset, use the table title as the dataset title.
    dataset.metadata.title = GRAPHER_DATASET_TITLE
    dataset.metadata.short_name = TABLE_NAME
    ####################################################################################################################

    dataset.metadata = gh.adapt_dataset_metadata_for_grapher(dataset.metadata)

    dataset.save()

    table = garden_dataset[TABLE_NAME].reset_index().drop(columns=["population"])
    # For convenience, change units from "million tonnes" to "tonnes" and multiply all variables by a million.
    # Doing this, grapher will know when to use the word "million" and when to use "billion".
    variables = [column for column in table.columns if column not in ["country", "year"]]
    for column in variables:
        if table[column].metadata.unit == "million tonnes":
            table[column].metadata.unit = "tonnes"
            table[column].metadata.short_unit = "t"
            table[column].metadata.display["conversionFactor"] = 1e6
            table[column].metadata.description = table[column].metadata.description.replace("million tonnes", "tonnes")
    table = gh.adapt_table_for_grapher(table)
    dataset.add(table)

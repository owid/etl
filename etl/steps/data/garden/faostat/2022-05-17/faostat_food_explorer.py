"""FAOSTAT food explorer.

Load the qcl and fbsc (combination of fbsh and fbs) datasets, and create a combined dataset of food products.

"""

from copy import deepcopy
from pathlib import Path

import pandas as pd
from owid import catalog
from owid.catalog.meta import DatasetMeta
from owid.datautils import geo

from etl.paths import DATA_DIR

# Dataset name and title.
DATASET_TITLE = "Food Explorer."


# TODO: Consider not removing the population variable in garden datasets. It seems in the old explorer we multiply
#  per capita variables by *our* population, and then divide again. It makes more sense to multiply by FAOSTAT
#  population and then divide by our population.
#  Or maybe we could just drop those per capita variables, if we have the analogous non-per-capita variables.

# TODO: It makes sense to add regions and per capita variables already on the individual garden datasets.
#  The main issue with that is that we would need to check which variables (and what aggregation).


def combine_qcl_and_fbsc(
    qcl_table: catalog.Table, fbsc_table: catalog.Table
) -> pd.DataFrame:
    qcl = pd.DataFrame(qcl_table).reset_index()
    qcl["value"] = qcl["value"].astype(float)
    fbsc = pd.DataFrame(fbsc_table).reset_index()
    fbsc["value"] = fbsc["value"].astype(float)

    rename_columns = {"item": "product"}
    index_columns = ["country", "year", "product", "element", "unit"]
    combined = (
        pd.concat([qcl, fbsc.drop(columns="description")], ignore_index=True)
        .rename(columns=rename_columns)
        .sort_values(index_columns)
        .reset_index(drop=True)
    )

    assert len(combined) == (
        len(qcl) + len(fbsc)
    ), "Unexpected number of rows after combining qcl and fbsc datasets."
    assert len(combined[combined["value"].isnull()]) == 0, "Unexpected nan values."

    return combined


def process_combined_data(data: pd.DataFrame) -> pd.DataFrame:
    index_columns = ["product", "country", "year"]

    # Join element and unit into one title column.
    data["title"] = data["element"] + "-" + data["unit"]

    # This will create a table with just one column and country-year as index.
    data_wide = data.pivot(
        index=index_columns, columns=["title"], values="value"
    ).reset_index()

    # Add column for population.
    data_wide = geo.add_population_to_dataframe(df=data_wide)

    # TODO: Select relevant products.

    assert (
        len(data_wide.columns[data_wide.isnull().all(axis=0)]) == 0
    ), "Unexpected columns with only nan values."

    # Set a reasonable index.
    data_wide = data_wide.set_index(index_columns)

    return data_wide


def run(dest_dir: str) -> None:

    ####################################################################################################################
    # Common definitions.
    ####################################################################################################################

    # Assume dest_dir is a path to the step that needs to be run, and fetch namespace and dataset
    # short name from that path.
    dataset_short_name = Path(dest_dir).name
    namespace = dataset_short_name.split("_")[0]
    # Path to latest qcl and fbsc datasets in garden.
    qcl_latest_dir = sorted(
        (DATA_DIR / "garden" / namespace).glob(f"*/{namespace}_qcl*")
    )[-1]
    fbsc_latest_dir = sorted(
        (DATA_DIR / "garden" / namespace).glob(f"*/{namespace}_fbsc*")
    )[-1]

    ####################################################################################################################
    # Load data.
    ####################################################################################################################

    # Load qcl dataset and keep its metadata.
    qcl_dataset = catalog.Dataset(qcl_latest_dir)
    fbsc_dataset = catalog.Dataset(fbsc_latest_dir)

    # Get qcl table inside qcl dataset (assume there is only one).
    assert len(qcl_dataset.table_names) == 1
    qcl_table = qcl_dataset[qcl_dataset.table_names[0]]
    # Idem for fbsc.
    assert len(fbsc_dataset.table_names) == 1
    fbsc_table = fbsc_dataset[fbsc_dataset.table_names[0]]

    ####################################################################################################################
    # Process data.
    ####################################################################################################################

    data = combine_qcl_and_fbsc(qcl_table=qcl_table, fbsc_table=fbsc_table)
    data = process_combined_data(data=data)

    # TODO: Create mapping of element (names and units), items, and possibly outliers.

    ####################################################################################################################
    # Save outputs.
    ####################################################################################################################

    # Initialize new garden dataset.
    explorer_dataset = catalog.Dataset.create_empty(dest_dir)
    # Define metadata for new garden dataset (by default, take metadata from fbsc dataset).
    # TODO: Revisit metadata.
    explorer_sources = deepcopy(fbsc_dataset.metadata.sources[0])
    explorer_sources.source_data_url = None
    explorer_sources.owid_data_url = None
    explorer_dataset.metadata = DatasetMeta(
        namespace=namespace,
        short_name=dataset_short_name,
        title=DATASET_TITLE,
        # TODO: Add custom description and combine sources.
        description=fbsc_dataset.metadata.description,
        sources=fbsc_dataset.metadata.sources + qcl_dataset.metadata.sources,
        licenses=fbsc_dataset.metadata.licenses + qcl_dataset.metadata.licenses,
    )
    # Create new dataset in garden.
    explorer_dataset.save()
    # Create table of products.
    # TODO: Decide if storing one big table, or one small table per product.
    table = catalog.Table(data)
    # Make all column names snake_case.
    table = catalog.utils.underscore_table(table)
    # Add metadata for the table.
    table.metadata.short_name = "all_products"
    table.metadata.primary_key = list(table.index)
    # Add table to dataset.
    explorer_dataset.add(table)

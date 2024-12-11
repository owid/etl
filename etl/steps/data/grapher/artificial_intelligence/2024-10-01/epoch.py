"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset, grapher_checks

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("epoch")

    # Read table from garden dataset.
    tb = ds_garden["epoch"].reset_index()
    #
    # Process data.
    #
    # Extract year from 'publication_date' and create a new 'year' column
    tb["year"] = tb["publication_date"].dt.year

    # For visualization purposes I am adding the rows with the maximum values of compute, data, and parameters in each year to the table as a separate "system". I don't want to do this in garden as it'd affect other datasets that depend on this one.
    columns = {
        "training_computation_petaflop": "compute",
        "training_dataset_size__datapoints": "data",
        "parameters": "parameters",
    }
    # Find maximum values for a given column (compute, data, params) per year, label them, and add summary rows.
    for column, label in columns.items():
        tb = find_max_label_and_concat(tb, column, label)

    # Update metadata
    for col in ["max_compute", "max_parameters", "max_data"]:
        tb[col].metadata.origins = tb["system"].metadata.origins

    # Drop year as we don't need it anymore
    tb = tb.drop("year", axis=1)

    # Rename for plotting model name as country in grapher
    tb = tb.rename(columns={"system": "country", "days_since_1949": "year"})
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb], default_metadata=ds_garden.metadata, check_variables_metadata=True
    )

    #
    # Checks.
    #
    grapher_checks(ds_grapher)

    # Save changes in the new grapher dataset.
    ds_grapher.save()


def find_max_label_and_concat(tb, column, label):
    """
    Find maximum values for a given column per year, label them, and add summary rows.

    This function:
    1. Identifies rows with maximum values for the specified column in each year.
    2. Labels these maximum value rows in a new column using their original system names.
    3. Creates new summary rows for these maximum values.
    4. Adds these new summary rows to the original table.

    Note:
    - Creates a new column named f"max_{label}" to indicate maximum values.
    - Preserves original data and system names.
    - Adds new summary rows with "system" set to f"Maximum {label}".
    """
    tb = tb.sort_values(by=["year"])  # Ensure the DataFrame is sorted by year
    max_value = -float("inf")
    rows_to_keep = []

    for _, row in tb.dropna(subset=[column]).iterrows():
        if row[column] > max_value:
            max_value = row[column]
            rows_to_keep.append(row)

    tb_filtered = Table(rows_to_keep)

    idx = tb_filtered[[column, "year"]].fillna(0).groupby("year")[column].idxmax()

    tb_filtered[f"max_{label}"] = "Other"
    tb_filtered.loc[idx, f"max_{label}"] = f"Maximum {label}"

    max_rows = tb_filtered.loc[idx].copy()
    max_rows["system"] = f"Maximum {label}"

    tb = pr.concat([tb, max_rows], ignore_index=True)

    return tb

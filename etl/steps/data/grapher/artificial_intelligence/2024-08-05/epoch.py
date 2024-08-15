"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr

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

    # Find the index of the maximum 'training_computation_petaflop', 'data', and 'parameters' in each year
    # Fill NaN values with 0 before finding the index of the maximum
    idx_compute = (
        tb[["training_computation_petaflop", "year"]]
        .fillna(0)
        .groupby("year")["training_computation_petaflop"]
        .idxmax()
    )
    idx_data = (
        tb[["training_dataset_size__datapoints", "year"]]
        .fillna(0)
        .groupby("year")["training_dataset_size__datapoints"]
        .idxmax()
    )
    idx_parameters = tb[["parameters", "year"]].fillna(0).groupby("year")["parameters"].idxmax()

    # Create a copy of the rows with largest compute values
    max_compute_rows = tb.loc[idx_compute].copy()
    max_data_rows = tb.loc[idx_data].copy()
    max_parameters_rows = tb.loc[idx_parameters].copy()

    # Update the system name for these new rows
    max_compute_rows["system"] = "Maximum compute"
    max_data_rows["system"] = "Maximum data"
    max_parameters_rows["system"] = "Maximum parameters"

    # Append the new rows to the original Table
    tb = pr.concat([tb, max_compute_rows, max_data_rows, max_parameters_rows], ignore_index=True)
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

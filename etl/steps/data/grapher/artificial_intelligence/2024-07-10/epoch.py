"""Load a garden dataset and create a grapher dataset."""

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

    # Create indicator columns
    tb["largest_compute"] = 0
    tb["largest_data"] = 0
    tb["largest_parameters"] = 0

    tb.loc[idx_compute, "largest_compute"] = 1
    tb.loc[idx_data, "largest_data"] = 1
    tb.loc[idx_parameters, "largest_parameters"] = 1
    tb = tb.drop("year", axis=1)
    for col in ["largest_compute", "largest_data", "largest_parameters"]:
        tb[col].metadata.origins = tb["domain"].metadata.origins

    tb = tb.rename(columns={"system": "country", "days_since_1949": "year"})
    # Rename for plotting model name as country in grapher
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

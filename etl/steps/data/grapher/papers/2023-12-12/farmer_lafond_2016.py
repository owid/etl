"""Load garden dataset for Farmer & Lafond (2016) data and create a grapher dataset.

"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("farmer_lafond_2016")
    tb_garden = ds_garden["farmer_lafond_2016"]

    #
    # Process data.
    #
    # Replace snake-case names by the original technology names.
    tb_garden = tb_garden.rename(
        columns={column: tb_garden[column].metadata.title for column in tb_garden.columns}, errors="raise"
    )

    # For better visualization, divide the costs of DNA sequencing by 1000, as done in the original paper by Farmer & Lafond (2016).
    tb_garden["Cost of DNA sequencing"] /= 1000

    # Remove units from each of the columns (that will be all put together in the same column).
    for column in tb_garden.columns:
        tb_garden[column].metadata.unit = None

    # Convert table to long format, and rename column so that it can be treated as a country in grapher.
    # This way, we can select technologies as we usually do with countries.
    tb_garden = (
        tb_garden.reset_index()
        .melt(id_vars="year", var_name="country", value_name="cost")
        .dropna()
        .reset_index(drop=True)
    )
    tb_garden["cost"].metadata.title = "Technology cost"
    tb_garden["cost"].metadata.unit = "various units"
    tb_garden[
        "cost"
    ].metadata.description_short = "Expressed in different units that have been chosen for visualization purposes."

    # Set an appropriate index and sort conveniently.
    tb_garden = tb_garden.set_index(["year", "country"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    dataset = create_dataset(dest_dir=dest_dir, tables=[tb_garden], check_variables_metadata=True)
    dataset.save()

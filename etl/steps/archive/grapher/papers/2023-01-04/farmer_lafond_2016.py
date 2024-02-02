"""Load garden dataset for Farmer & Lafond (2016) data and create a grapher dataset.

"""

from owid import catalog

from etl.helpers import PathFinder

# Get paths and naming conventions for current data step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load data.
    #
    ds_garden: catalog.Dataset = paths.load_dependency("farmer_lafond_2016")
    tb_garden = ds_garden["farmer_lafond_2016"]

    #
    # Process data.
    #
    # Replace snake-case names by the original technology names.
    tb_garden.columns = [tb_garden[column].metadata.title for column in tb_garden.columns]

    # For better visualization, divide the costs of DNA sequencing by 1000, as done in the original paper by Farmer & Lafond (2016).
    tb_garden["DNA sequencing"] /= 1000

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

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    dataset = catalog.Dataset.create_empty(dest_dir, ds_garden.metadata)

    # Add table to dataset and save dataset.
    dataset.add(tb_garden)
    dataset.save()

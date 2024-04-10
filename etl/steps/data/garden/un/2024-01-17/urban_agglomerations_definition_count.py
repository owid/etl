"""Load a meadow dataset and create a garden dataset."""


from owid.catalog import Table
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("urban_agglomerations_definition")

    # Read table from meadow dataset.
    tb = ds_garden["urban_agglomerations_definition"].reset_index()

    #
    # Process data.
    #

    # Get the count of each unique value in the 'minimum_inhabitants' column, including NaN values
    df_counts = tb["minimum_inhabitants"].value_counts(dropna=False).reset_index()

    # Rename the columns of the resulting DataFrame
    df_counts.columns = ["countries", "minimum_inhabitants"]

    # Add a 'year' column filled with 2018
    df_counts["year"] = 2018

    df_counts["countries"] = (
        df_counts["countries"].astype(object).apply(lambda x: f"{x:,} inhabitants" if isinstance(x, int) else x)
    )

    # Replace '<NA>' values in the 'countries' column with 'No minimum population threshold'
    df_counts["countries"] = df_counts["countries"].astype(str).replace("<NA>", "No minimum population threshold")

    tb_counts = Table(df_counts).copy_metadata(tb)
    tb_counts = tb_counts.set_index(["countries", "year"], verify_integrity=True)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_counts], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

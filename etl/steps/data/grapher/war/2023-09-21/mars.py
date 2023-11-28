"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("mars")

    # Read table from garden dataset.
    tb = ds_garden["mars"]
    tb_country = ds_garden["mars_country"]

    #
    # Process data.
    #
    # Rename index column `region` to `country`.
    tb = tb.reset_index().rename(columns={"region": "country"})
    # Remove suffixes in region names
    tb["country"] = tb["country"].str.replace(r" \(.+\)", "", regex=True)
    # Set index
    tb = tb.set_index(["year", "country", "conflict_type"])

    #
    # Save outputs.
    #
    tables = [
        tb,
        tb_country,
    ]
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Remove source description so that it doesn't get appended to the dataset description.
    # ds_grapher.metadata.sources[0].description = ""

    # Save changes in the new grapher dataset.
    ds_grapher.save()

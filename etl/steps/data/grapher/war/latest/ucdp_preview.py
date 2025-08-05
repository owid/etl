"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("ucdp_preview")

    # Read table from garden dataset.
    tb = ds_garden.read("ucdp_preview")

    # Process data.
    #
    # Remove suffixes in region names
    tb["country"] = tb["country"].str.replace(r" \(.+\)", "", regex=True)
    # Set index
    tb = tb.format(["year", "country", "conflict_type"])

    # Get country-level data
    tb_participants = ds_garden["ucdp_preview_country"]
    tb_locations = ds_garden["ucdp_preview_locations"]

    tables = [
        tb,
        tb_participants,
        tb_locations,
    ]

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_garden.metadata,
    )

    # Remove source description so that it doesn't get appended to the dataset description.
    # ds_grapher.metadata.sources[0].description = ""

    # Save changes in the new grapher dataset.
    ds_grapher.save()

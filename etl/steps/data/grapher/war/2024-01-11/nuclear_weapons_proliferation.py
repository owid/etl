"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its tables.
    ds_garden = paths.load_dataset("nuclear_weapons_proliferation")
    tb = ds_garden["nuclear_weapons_proliferation"]
    tb_counts = ds_garden["nuclear_weapons_proliferation_counts"]

    #
    # Process data.
    #
    # Add a country column to the index of the table of counts.
    tb_counts = (
        tb_counts.reset_index().assign(**{"country": "World"}).set_index(["country", "year"], verify_integrity=True)
    )

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb, tb_counts], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )
    ds_grapher.save()

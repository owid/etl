"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("nuclear_weapons_treaties")
    tb_counts = ds_garden["nuclear_weapons_treaties_country_counts"].reset_index()

    #
    # Process data.
    #
    # Rename status column to be able to use it in grapher.
    tb_counts = tb_counts.rename(columns={"status": "country"}, errors="raise")

    # Set an appropriate index for the counts table and sort conveniently.
    tb_counts = tb_counts.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_counts], check_variables_metadata=True)
    ds_grapher.save()

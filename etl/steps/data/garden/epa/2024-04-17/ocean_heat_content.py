"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("ocean_heat_content")
    tb = ds_meadow["ocean_heat_content"].reset_index()

    #
    # Process data.
    #
    # Instead of having a column for depth, create columns of heat content for each depth.
    tb["depth"] = tb["depth"].astype(str) + "m"
    tb = tb.pivot(index=["location", "year"], columns="depth", join_column_levels_with="_")

    # Delete columns with no data.
    tb = tb.dropna(how="all", axis=1).reset_index(drop=True)

    # Set an appropriate index to each table and sort conveniently.
    tb = tb.set_index(["location", "year"], verify_integrity=True).sort_index().sort_index(axis=1)

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()

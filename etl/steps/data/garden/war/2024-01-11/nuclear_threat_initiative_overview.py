"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset and read its main table.
    ds_meadow = paths.load_dataset("nuclear_threat_initiative_overview")
    tb = ds_meadow["nuclear_threat_initiative_overview"]

    # This table contains a "status" column, which corresponds to whether a country does not consider (0),
    # considers (1), pursues (2), or possesses (3) nuclear weapons.

    #
    # Save outputs.
    #
    # Create a new garden dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True)
    ds_garden.save()

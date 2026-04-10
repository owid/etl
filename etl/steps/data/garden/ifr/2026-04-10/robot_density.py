"""Load a meadow dataset and create a garden dataset combining 2023 and 2024 data."""

from owid.catalog import processing as pr

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset for 2024 data.
    ds_meadow = paths.load_dataset("robot_density", version="2026-04-10")
    tb_2024 = ds_meadow.read("robot_density")

    # Load garden dataset for 2023 data.
    ds_garden_2023 = paths.load_dataset("robot_density", namespace="ifr", version="2026-01-14")
    tb_2023 = ds_garden_2023.read("robot_density", reset_index=True)

    #
    # Process data.
    #
    # Harmonize country names for 2024 data.
    tb_2024 = paths.regions.harmonize_names(tb=tb_2024)
    tb_2024["robot_density"] = tb_2024["robot_density"] / 10

    # Combine 2023 and 2024 data.
    tb = pr.concat([tb_2023, tb_2024], ignore_index=True)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()

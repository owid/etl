"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("top500_supercomputers")

    # Read table from meadow dataset.
    tb = ds_meadow.read("top500_supercomputers").reset_index()

    #
    # Process data.
    #
    # Find the peak performance (maximum Rmax) for each list_year
    # Group by list_year and get the maximum rmax__tflop_s
    tb_world = tb.groupby("list_year", as_index=False)["rmax"].max()

    # Add country column with "World"
    tb_world["country"] = "World"

    # Rename list_year to year for consistency
    tb_world = tb_world.rename(columns={"list_year": "year"})

    # Rename column to be more descriptive
    tb_world = tb_world.rename(columns={"rmax": "computational_capacity_fastest_supercomputer"})

    # Improve table format.
    tb_world = tb_world.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_world], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()

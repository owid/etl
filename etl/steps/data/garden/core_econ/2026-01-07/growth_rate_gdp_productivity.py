"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("growth_rate_gdp_productivity")

    # Read table from meadow dataset.
    tb_productivity = ds_meadow.read("growth_rate_productivity")
    tb_gdp_pc = ds_meadow.read("growth_rate_gdp_pc")

    #
    # Process data.
    #
    # Harmonize country names.
    tb_productivity = paths.regions.harmonize_names(tb=tb_productivity, warn_on_unused_countries=False)
    tb_gdp_pc = paths.regions.harmonize_names(tb=tb_gdp_pc, warn_on_unused_countries=False)

    # Improve table format.
    tb_productivity = tb_productivity.format(["country", "year"])
    tb_gdp_pc = tb_gdp_pc.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb_productivity, tb_gdp_pc], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()

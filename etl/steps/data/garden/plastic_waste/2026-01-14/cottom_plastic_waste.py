"""Garden step for plastic waste data with country harmonization."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("cottom_plastic_waste")

    # Read tables from meadow dataset.
    tb_national = ds_meadow["cottom_plastic_waste_national"].reset_index()
    tb_regional = ds_meadow["cottom_plastic_waste_regional"].reset_index()

    #
    # Process data.
    #
    # Harmonize country names for national data
    tb_national = paths.regions.harmonize_names(tb_national)

    # Regional data doesn't need country harmonization as it contains regions/income groups
    # But we should ensure the country column is properly formatted
    tb_regional = tb_regional.copy()

    # Set index and format tables
    tb_national = tb_national.format(["country", "year"], short_name="cottom_plastic_waste_national")
    tb_regional = tb_regional.format(["country", "year"], short_name="cottom_plastic_waste_regional")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=[tb_national, tb_regional],
        check_variables_metadata=True,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

"""Load a garden dataset and create a grapher dataset.

"""


from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset and read its main table.
    ds_garden = paths.load_dataset("climate_change_impacts")
    tb_annual = ds_garden["climate_change_impacts_annual"].reset_index()
    # tb_monthly = ds_garden["climate_change_impacts_monthly"].reset_index()

    #
    # Process data.
    #
    # Create a country column (required by grapher).
    tb_annual = tb_annual.rename(columns={"location": "country"}, errors="raise")

    # Set an appropriate index and sort conveniently.
    tb_annual = tb_annual.set_index(["country", "year"], verify_integrity=True).sort_index()

    #
    # Save outputs.
    #
    # Create a new grapher dataset.
    ds_grapher = create_dataset(dest_dir, tables=[tb_annual], check_variables_metadata=True)
    ds_grapher.save()

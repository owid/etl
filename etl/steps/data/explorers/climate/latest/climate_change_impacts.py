"""Load a garden dataset and create an explorers dataset.

The output csv file will feed our Climate Change Impacts data explorer:
https://ourworldindata.org/explorers/climate-change
"""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load climate change impacts dataset and read its annual and monthly tables.
    ds = paths.load_dataset("climate_change_impacts")
    tb_annual = ds["climate_change_impacts_annual"]
    tb_monthly = ds["climate_change_impacts_monthly"]

    #
    # Save outputs.
    #
    # Create explorer dataset with combined table in csv format.
    ds_explorer = create_dataset(dest_dir, tables=[tb_annual, tb_monthly], formats=["csv"])
    ds_explorer.save()

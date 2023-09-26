"""Load a meadow dataset and create a garden dataset."""

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("geyer_2017")

    # Read table from meadow dataset.
    tb = ds_meadow["geyer_2017"].reset_index()

    #
    # Process data.
    #

    # Define the growth rate
    growth_rate = 0.05  # 5%

    # Add new rows for 2016-2018
    for year in range(2016, 2019):  # 2019 is the stop value and is not included
        last_value = tb.loc[tb.index[-1], "plastic_production"]  # Getting the last value in the 'Value' column
        new_value = last_value * (1 + growth_rate)  # Calculating the value for the new year
        new_row = {"country": "World", "year": year, "plastic_production": new_value}  # Creating a new row
        tb = tb.append(new_row, ignore_index=True)  # Adding the new row to the DataFrame

    tb["plastic_production"] = tb["plastic_production"] * 1e6  # Convert to millions
    tb["cumulative_production"] = tb["plastic_production"].cumsum()

    tb = tb.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

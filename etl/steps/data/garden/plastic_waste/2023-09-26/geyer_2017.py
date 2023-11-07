"""Load a meadow dataset and create a garden dataset."""
import owid.catalog.processing as pr

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

    # Add data from OECD for 2019
    ds_oecd = paths.load_dataset("plastic_use_application")

    # Read table from meadow dataset.
    tb_oecd = ds_oecd["plastic_use_application"]
    total_plastic_use_2019 = tb_oecd["total"].reset_index()
    total_prod = total_plastic_use_2019[["year", "country", "total"]][total_plastic_use_2019["year"] == 2019]
    total_prod = total_prod.rename(columns={"total": "plastic_production"})

    combined_df = pr.merge(tb, total_prod, on=["country", "year", "plastic_production"], how="outer").copy_metadata(
        from_table=tb
    )

    combined_df["cumulative_production"] = combined_df["plastic_production"].cumsum()

    tb = combined_df.set_index(["country", "year"], verify_integrity=True)

    for column in tb.columns:
        tb[column].metadata.origins.append(total_prod["plastic_production"].metadata.origins[0])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

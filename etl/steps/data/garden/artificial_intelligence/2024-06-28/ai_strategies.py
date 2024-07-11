"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Retrieve snapshot.
    snap = paths.load_snapshot("ai_strategies.csv")

    # Load data from snapshot.
    tb = snap.read()

    #
    # Process data.
    #

    # Harmonize the country names
    tb = tb.rename(columns={"Geographic area": "country", "Year": "year"})
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = tb.melt(id_vars=["country", "year"], var_name="strategy_released", value_name="value")

    # Remove rows with NaN values (that will create a dataframe with only the right values in the strategy_released column)
    tb = tb.dropna()
    tb = tb.drop(columns={"value"})

    # Determine the range of years for the entire dataset
    min_year = tb["year"].min()
    max_year = tb["year"].max()

    # Create a DataFrame that includes every year within the range
    all_years = Table({"year": range(min_year, max_year + 1)})

    # Load the regions dataset
    ds_regions = paths.load_dataset("regions")
    tb_regions = ds_regions["regions"]
    tb_regions = tb_regions[tb_regions["defined_by"] == "owid"]
    tb_regions = Table(tb_regions["name"])
    tb_regions = tb_regions.reset_index(drop=True)

    # Create a Cartesian product of all countries and all years
    countries = Table({"country": tb_regions["name"].unique()})
    full_tb = countries.merge(all_years, how="cross")

    # Merge the original Table with the new Table to include all years and all countries
    tb = pr.merge(tb, full_tb, on=["country", "year"], how="outer")

    # Fill NaN values using forward fill
    tb["strategy_released"] = tb.groupby("country")["strategy_released"].ffill()
    # Fill NaN values with "Not released"
    tb = tb.fillna("Not released")
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=snap.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

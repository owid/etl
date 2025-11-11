"""Garden step for WSTS Historical Billings Report dataset."""

import pandas as pd

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("wsts_historical_billings")

    # Read tables from meadow dataset.
    tb_monthly = ds_meadow["wsts_historical_billings_monthly"].reset_index()
    tb_3mma = ds_meadow["wsts_historical_billings_3mma"].reset_index()

    #
    # Process data.
    #
    # Harmonize country names
    tb_monthly = geo.harmonize_countries(df=tb_monthly, countries_file=paths.country_mapping_path, country_col="region")
    tb_3mma = geo.harmonize_countries(df=tb_3mma, countries_file=paths.country_mapping_path, country_col="region")
    print(tb_3mma)
    # Process monthly/quarterly data with 3 months running averages
    # Convert month name to month number
    tb_3mma["date"] = pd.to_datetime(tb_3mma["year"].astype(str) + "-" + tb_3mma["month"].astype(str), format="%Y-%B")

    # Drop the separate year and month columns since we now have date
    tb_3mma = tb_3mma.drop(columns=["year", "month"])

    tb_yearly = tb_monthly[tb_monthly["period"] == "Total Year"].copy()
    tb_yearly = tb_yearly.drop(columns=["period", "period_type"])

    # Format tables
    tb_yearly = tb_yearly.format(["region", "year"], short_name="wsts_historical_billings_yearly")
    tb_3mma = tb_3mma.format(["region", "date"], short_name="wsts_historical_billings_3mma")

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(tables=[tb_yearly, tb_3mma], check_variables_metadata=True)

    # Save changes in the new garden dataset.
    ds_garden.save()

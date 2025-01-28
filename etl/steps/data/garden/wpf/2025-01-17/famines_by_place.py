"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("famines")
    # Read table from meadow dataset.
    tb = ds_meadow["famines"].reset_index()
    tb = tb[["famine_name", "date", "country", "wpf_authoritative_mortality_estimate"]]

    #
    # Process data.
    #
    # Drop rows where the Chinese famine is not broken down by year (used for other datasets but for this one we need the breakdown)
    famine_names_to_drop = ["China 1958-1962"]
    tb = tb[~tb["famine_name"].isin(famine_names_to_drop)]
    tb = tb.drop(columns=["famine_name"])

    # Divide each row's 'wpf_authoritative_mortality_estimate' by the length of the corresponding 'Date' value to assume a uniform distribution of deaths over the period
    tb["wpf_authoritative_mortality_estimate"] = tb.apply(
        lambda row: row["wpf_authoritative_mortality_estimate"] / len(row["date"].split(","))
        if pd.notna(row["date"])
        else row["wpf_authoritative_mortality_estimate"],
        axis=1,
    )

    # Unravel the 'date' column so that there is only one value per row. Years separated by commas are split into separate rows.
    tb = tb.assign(date=tb["date"].str.split(",")).explode("date").drop_duplicates().reset_index(drop=True)

    tb = tb.rename(columns={"date": "year"})
    tb["year"] = tb["year"].astype(int)
    tb["country"] = tb["country"].astype(str)

    # Define the main countries with the most famines
    main_countries = [
        "China",
        "Ukraine",
        "India",
        "Russia",
        "Russia, Ukraine",
        "USSR",
        "Germany, USSR",
        "Moldova, Ukraine, Russia, Belarus",
        "Russia, Western Soviet States",
        "Russia, Kazakhstan",
        "India, Bangladesh",
    ]

    # Separate main countries and others
    tb_main = tb[tb["country"].isin(main_countries)].copy()
    tb_other = tb[~tb["country"].isin(main_countries)].copy()
    tb_main["country"] = tb_main["country"].replace(
        {
            "Russia": "USSR/Russia",
            "Ukraine": "USSR/Russia",
            "Russia, Ukraine": "USSR/Russia",
            "Germany, USSR": "USSR/Russia",
            "Moldova, Ukraine, Russia, Belarus": "USSR/Russia",
            "Russia, Western Soviet States": "USSR/Russia",
            "Russia, Kazakhstan": "USSR/Russia",
            "USSR": "USSR/Russia",
            "India, Bangladesh": "India",
            "Kazakhstan": "USSR/Russia",
        }
    )

    # Sum deaths for other countries by year
    tb_other = tb_other.groupby("year")["wpf_authoritative_mortality_estimate"].sum().reset_index()
    tb_other["country"] = "Other countries"

    # Combine main countries and others
    tb = pr.concat([tb_main, tb_other], ignore_index=True)

    # Sum the entries for each country and year
    tb = tb.groupby(["country", "year"])["wpf_authoritative_mortality_estimate"].sum().reset_index()

    # Create a DataFrame with all years from 1870 to 2023
    all_years = pd.DataFrame({"year": range(1870, 2024)})

    # Get all unique regions from the original data
    all_regions = tb["country"].unique()

    # Create a DataFrame with all combinations of years and regions - to ensure that where there are no data points for a year and region, the value is set to zero
    all_years_regions = pd.MultiIndex.from_product(
        [all_years["year"], all_regions], names=["year", "country"]
    ).to_frame(index=False)
    all_years_regions = Table(all_years_regions)

    # Merge this Table with the existing data to ensure all years are present
    tb = pr.merge(tb, all_years_regions, on=["year", "country"], how="right")
    tb["wpf_authoritative_mortality_estimate"] = tb["wpf_authoritative_mortality_estimate"].fillna(0)

    # Calculate the decade
    tb["decade"] = (tb["year"] // 10) * 10

    # Group the data by region and decade, then calculate the decadal sum
    tb["decadal_famine_deaths"] = tb.groupby(["country", "decade"], observed=False)[
        "wpf_authoritative_mortality_estimate"
    ].transform("sum")
    # Set NaN everywhere except the start of a decade
    tb["decadal_famine_deaths"] = tb["decadal_famine_deaths"].where(tb["year"] % 10 == 0, np.nan)
    tb = tb.drop(columns=["decade"])

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

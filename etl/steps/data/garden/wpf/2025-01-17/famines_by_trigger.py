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
    # Load the dataset.
    ds_garden = paths.load_dataset("famines")

    # Read table from the dataset.
    tb = ds_garden.read("famines")

    origins = tb["famine_name"].metadata.origins

    #
    # Process data.
    #
    # Select relevant columns.

    # Drop rows where the Chinese famine is not broken down by year (used for other datasets but for this one we need the breakdown)
    famine_names_to_drop = ["China 1958-1962"]
    tb = tb[~tb["famine_name"].isin(famine_names_to_drop)]
    tb = tb[
        [
            "date",
            "wpf_authoritative_mortality_estimate",
            "principal_cause",
            "region",
        ]
    ]

    # Divide each row's 'wpf_authoritative_mortality_estimate' by the length of the corresponding 'Date' value to assume a uniform distribution of deaths over the period.
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
    tb["region"] = tb["region"].astype("category")

    # Calculate the total number of famine deaths per year and region
    deaths_counts = (
        tb.groupby(["year", "region", "principal_cause"], observed=False)["wpf_authoritative_mortality_estimate"]
        .sum()
        .reset_index(name="famine_deaths")
    )
    deaths_counts_world_only = (
        tb.groupby(["year", "principal_cause"])["wpf_authoritative_mortality_estimate"]
        .sum()
        .reset_index(name="famine_deaths")
    )
    deaths_counts_world_only["region"] = "World"

    # Concatenate the number of famines per year and region
    tb = pr.concat([deaths_counts, deaths_counts_world_only], ignore_index=True)

    # Create a DataFrame with all years from 1870 to 2023 so we don't have 0s for years where there is no data
    all_years = pd.DataFrame({"year": range(1870, 2024)})

    # Get all unique regions from the original data
    all_regions = tb["region"].unique()
    # Get all unique principal causes from the original data
    all_principal_causes = tb["principal_cause"].unique()

    # Create a DataFrame with all combinations of years, regions, and principal causes
    all_years_regions = pd.MultiIndex.from_product(
        [all_years["year"], all_regions, all_principal_causes], names=["year", "region", "principal_cause"]
    ).to_frame(index=False)

    all_years_regions = Table(all_years_regions)

    # Merge this Table with the existing data to ensure all years are present
    tb = pr.merge(tb, all_years_regions, on=["year", "region", "principal_cause"], how="right")

    # Fill NaN values in the 'famine_deaths' columns with zeros
    tb["famine_deaths"] = tb["famine_deaths"].fillna(0)

    # Calculate decadal deaths
    tb["decade"] = (tb["year"] // 10) * 10
    tb["decadal_famine_deaths"] = tb.groupby(["region", "decade", "principal_cause"], observed=False)[
        "famine_deaths"
    ].transform("sum")

    # Set NaN everywhere except the start of a decade
    tb["decadal_famine_deaths"] = tb["decadal_famine_deaths"].where(tb["year"] % 10 == 0, np.nan)
    tb = tb.drop(columns=["decade"])
    tb = tb.rename(columns={"region": "country"})

    tb = tb.format(["year", "country", "principal_cause"], short_name=paths.short_name)
    for col in [
        "famine_deaths",
        "decadal_famine_deaths",
    ]:
        tb[col].metadata.origins = origins

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

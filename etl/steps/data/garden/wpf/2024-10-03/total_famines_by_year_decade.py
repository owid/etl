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
    origins = tb["conventional_title"].metadata.origins

    #
    # Process data.
    #
    # Divide each row's 'wpf_authoritative_mortality_estimate' by the length of the corresponding 'Date' value to assume a uniform distribution of deaths over the period
    tb["wpf_authoritative_mortality_estimate"] = tb.apply(
        lambda row: row["wpf_authoritative_mortality_estimate"] / len(row["date"])
        if pd.notna(row["date"])
        else row["wpf_authoritative_mortality_estimate"],
        axis=1,
    )

    # Unravel the 'date' column so that there is only one value per row. Years separated by commas are split into separate rows.
    tb = unravel_dates(tb)
    tb = tb.rename(columns={"date": "year"})
    tb["year"] = tb["year"].astype(int)
    tb["global_region"] = tb["global_region"].astype("category")

    # Grouping by 'year', 'global_region', 'conflict', 'government_policy_overall', 'external_factors' and counting the occurrences
    famine_counts = tb.groupby(["year", "global_region"], observed=False).size().reset_index(name="famine_count")
    # Creating a 'World' row by summing counts across unique regions for each group
    famine_counts_world_only = tb.groupby(["year"]).size().reset_index(name="famine_count")
    famine_counts_world_only["global_region"] = "World"
    # Concatenating the world row data with the regional data
    famine_counts_combined = pr.concat([famine_counts, famine_counts_world_only], ignore_index=True)

    # Grouping by relevant columns and summing the 'wpf_authoritative_mortality_estimate' for regional data
    deaths_counts = (
        tb.groupby(["year", "global_region"], observed=False)["wpf_authoritative_mortality_estimate"]
        .sum()
        .reset_index(name="famine_deaths")
    )

    # Creating a 'World' row by summing mortality estimates across all regions for each group
    deaths_counts_world_only = (
        tb.groupby(["year"])["wpf_authoritative_mortality_estimate"].sum().reset_index(name="famine_deaths")
    )
    deaths_counts_world_only["global_region"] = "World"

    # Concatenating the world row data with the regional data
    deaths_counts_combined = pr.concat([deaths_counts, deaths_counts_world_only], ignore_index=True)

    tb = pr.merge(
        famine_counts_combined,
        deaths_counts_combined,
        on=["year", "global_region"],
    )
    # Group the data by decade
    for column in ["famine_count", "famine_deaths"]:
        # Calculate the decadal sum
        tb["decadal_" + column] = tb.groupby(tb["year"] // 10 * 10)[column].transform("sum")
        # Set NaN everywhere except the start of a decade
        tb["decadal_" + column] = tb["decadal_" + column].where(tb["year"] % 10 == 0, np.nan)

    tb = tb.format(["year", "global_region"], short_name=paths.short_name)
    tb = Table(tb, short_name=paths.short_name)
    for col in ["famine_count", "famine_deaths", "decadal_famine_count", "decadal_famine_deaths"]:
        tb[col].metadata.origins = origins

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def unravel_dates(tb):
    """
    Unravel the 'date' column so that there is only one value per row. Years separated by commas are split into separate rows.
    """
    # Split the 'date' column into multiple rows
    tb = tb.assign(date=tb["date"].str.split(",")).explode("date").drop_duplicates().reset_index(drop=True)

    return tb

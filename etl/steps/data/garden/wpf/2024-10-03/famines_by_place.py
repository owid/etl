"""Load a meadow dataset and create a garden dataset."""


import owid.catalog.processing as pr
import pandas as pd

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
    tb = tb[["date", "country", "wpf_authoritative_mortality_estimate"]]

    #
    # Process data.
    #
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

    # Define the main countries with the most famines
    main_countries = ["China", "Ukraine", "India", "Russia", "Russia, Ukraine", "USSR"]

    # Separate main countries and others
    tb_main = tb[tb["country"].isin(main_countries)]
    tb_other = tb[~tb["country"].isin(main_countries)]

    # Sum deaths for other countries by year
    tb_other = tb_other.groupby("year")["wpf_authoritative_mortality_estimate"].sum().reset_index()
    tb_other["country"] = "Other"

    # Combine main countries and others
    tb_combined = pr.concat([tb_main, tb_other], ignore_index=True)

    # Sum the entries for each country and year
    tb_combined = tb_combined.groupby(["country", "year"])["wpf_authoritative_mortality_estimate"].sum().reset_index()

    tb_combined = tb_combined.format(["country", "year"], short_name=paths.short_name)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_combined], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

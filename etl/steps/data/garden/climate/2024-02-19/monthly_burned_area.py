"""Load a meadow dataset and create a garden dataset."""

import pandas as pd
from owid.catalog import processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
REGIONS = ["North America", "South America", "Europe", "Africa", "Asia", "Oceania", "World"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("monthly_burned_area")

    # Read table from meadow dataset.
    tb = ds_meadow["monthly_burned_area"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    # Sum the burned area by country, year, and month
    grouped_tb = (
        tb.groupby(["country", "year", "month"], observed=True)[
            ["forest", "savannas", "shrublands_grasslands", "croplands", "other"]
        ]
        .sum()
        .reset_index()
    )

    # Create a date column
    grouped_tb["date"] = pd.to_datetime(grouped_tb["year"].astype(str) + "-" + grouped_tb["month"].astype(str) + "-01")
    # Create a variable with days since column
    grouped_tb["days_since_2000"] = (grouped_tb["date"] - pd.to_datetime("2000-01-01")).dt.days
    grouped_tb = grouped_tb.drop(columns=["date"])

    aggregations = {
        "forest": "sum",
        "savannas": "sum",
        "shrublands_grasslands": "sum",
        "croplands": "sum",
        "other": "sum",
    }
    # Add region aggregates.
    grouped_tb = geo.add_regions_to_table(
        grouped_tb,
        aggregations=aggregations,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
        year_col="year",
    )
    grouped_tb["all"] = grouped_tb[["forest", "savannas", "shrublands_grasslands", "croplands", "other"]].sum(axis=1)

    grouped_tb = grouped_tb.set_index(["country", "year", "month", "days_since_2000"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[grouped_tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

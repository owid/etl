"""Load a meadow dataset and create a garden dataset."""

import pandas as pd

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
    ds_meadow = paths.load_dataset("monthly_fire_emissions")

    # Read table from meadow dataset.
    tb = ds_meadow["monthly_fire_emissions"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    emissions_cols = ["co2", "co", "tpm", "pm25", "tpc", "nmhc", "oc", "ch4", "so2", "bc", "nox"]
    # Sum the burned area by country, year, and month
    grouped_tb = tb.groupby(["country", "year", "month"], observed=True)[emissions_cols].sum().reset_index()

    # Create a date column
    grouped_tb["date"] = pd.to_datetime(grouped_tb["year"].astype(str) + "-" + grouped_tb["month"].astype(str) + "-01")

    aggregations = {emission: "sum" for emission in emissions_cols}
    # Add region aggregates.
    grouped_tb = geo.add_regions_to_table(
        grouped_tb,
        aggregations=aggregations,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
        year_col="date",
    )

    # Create a variable with days since column
    grouped_tb["days_since_2000"] = (grouped_tb["date"] - pd.to_datetime("2000-01-01")).dt.days
    # Make sure there are no NaN values in 'year' and 'month' columns after aggregations
    grouped_tb["year"] = grouped_tb["date"].dt.year
    grouped_tb["month"] = grouped_tb["date"].dt.month

    grouped_tb = grouped_tb.drop(columns=["date"])

    cols_to_keep = ["co2", "pm25"]
    grouped_tb = grouped_tb.set_index(["country", "year", "month", "days_since_2000"], verify_integrity=True)
    grouped_tb = grouped_tb[cols_to_keep]
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[grouped_tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

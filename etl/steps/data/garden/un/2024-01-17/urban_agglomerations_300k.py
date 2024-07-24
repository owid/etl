"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("urban_agglomerations_300k")

    # Read table from meadow dataset.
    tb = ds_meadow["urban_agglomerations_300k"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)
    columns_to_average = [
        "average_annual_rate_of_change_of_urban_agglomerations_with_300_000_inhabitants_or_more_in_2018__by_country__1950_2035__percent",
        "percentage_of_the_urban_population_residing_in_each_urban_agglomeration_with_300_000_inhabitants_or_more_in_2018__by_country__1950_2035",
        "percentage_of_the_total_population_residing_in_each_urban_agglomeration_with_300_000_inhabitants_or_more_in_2018__by_country__1950_2035",
        "annual_population_of_urban_agglomerations_with_300_000_inhabitants_or_more__by_country__1950_2035__thousands",
    ]
    # Find average of all cities 300k or more in each country
    tb_average = tb.groupby(["country", "year"])[columns_to_average].mean().reset_index()

    # Convert to thousands
    tb_average[
        "annual_population_of_urban_agglomerations_with_300_000_inhabitants_or_more__by_country__1950_2035__thousands"
    ] = (
        tb_average[
            "annual_population_of_urban_agglomerations_with_300_000_inhabitants_or_more__by_country__1950_2035__thousands"
        ]
        * 1000
    )
    # Remove 'thousands' from column name
    tb_average = tb_average.rename(
        columns={
            "annual_population_of_urban_agglomerations_with_300_000_inhabitants_or_more__by_country__1950_2035__thousands": "annual_population_of_urban_agglomerations_with_300_000_inhabitants_or_more__by_country__1950_2035__thousands".replace(
                "__thousands", ""
            )
        }
    )

    # Create two new dataframes to separate data into estimates and projections (pre-2019 and post-2019)
    past_estimates = tb_average[tb_average["year"] < 2019].copy()
    future_projections = tb_average[tb_average["year"] >= 2019].copy()

    # Now, for each column in the original dataframe, split it into two
    for col in tb_average.columns:
        if col not in ["country", "year"]:
            past_estimates[f"{col}_estimates"] = tb_average.loc[tb_average["year"] < 2019, col]
            future_projections[f"{col}_projections"] = tb_average.loc[tb_average["year"] >= 2019, col]
            past_estimates = past_estimates.drop(columns=[col])
            future_projections = future_projections.drop(columns=[col])

    tb_merged = pr.merge(past_estimates, future_projections, on=["country", "year"], how="outer")
    tb_merged = tb_merged.set_index(["country", "year"], verify_integrity=True)

    # Remove '__1950_2050' from column names
    for col in tb_merged.columns:
        if "__1950_2035" in col:
            tb_merged = tb_merged.rename(columns={col: col.replace("__1950_2035", "")})
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_merged], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

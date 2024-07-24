"""Load a garden dataset and create a grapher dataset."""

import owid.catalog.processing as pr

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("urban_agglomerations_largest_cities")

    # Read table from garden dataset.
    tb = ds_garden["urban_agglomerations_largest_cities"]
    #
    # Process data.
    #
    tb = tb.reset_index()
    # Rename urban_agglomeration to country for the grapher.
    tb = tb.rename(
        columns={
            "urban_agglomeration": "country",
            "country": "country_code",
            "time_series_of_the_population_of_the_30_largest_urban_agglomerations_in_2018_ranked_by_population_size": "population_30_largest_cities",
        }
    )
    tb = tb.drop(columns=["rank_order", "population_capital", "country_code"])

    # Create two new dataframes to separate data into estimates and projections (pre-2019 and post-2019)
    past_estimates = tb[tb["year"] < 2019].copy()
    future_projections = tb[tb["year"] >= 2019].copy()

    # Now, for each column in the original dataframe, split it into two
    for col in tb.columns:
        if col not in ["country", "year"]:
            past_estimates[f"{col}_estimates"] = tb.loc[tb["year"] < 2019, col]
            future_projections[f"{col}_projections"] = tb.loc[tb["year"] >= 2019, col]
            past_estimates = past_estimates.drop(columns=[col])
            future_projections = future_projections.drop(columns=[col])

    tb_merged = pr.merge(past_estimates, future_projections, on=["country", "year"], how="outer")
    tb_merged = tb_merged.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = create_dataset(
        dest_dir, tables=[tb_merged], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )
    ds_grapher.metadata.title = "World Urbanization Prospects Dataset - Population of the 30 largest cities"

    # Save changes in the new grapher dataset.
    ds_grapher.save()

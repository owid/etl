"""Load a meadow dataset and create a garden dataset."""
import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

START_OF_PROJECTIONS = 2025

# Regions for which aggregates will be created.
REGIONS = [
    "North America",
    "South America",
    "Europe",
    "Africa",
    "Asia",
    "Oceania",
    "Low-income countries",
    "Upper-middle-income countries",
    "Lower-middle-income countries",
    "High-income countries",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ghsl_urban_centers")
    # Read table from meadow dataset.
    tb = ds_meadow.read("ghsl_urban_centers")

    # Read GHSL main data from meadow dataset for calculating share of population living in capitals.
    ds_meadow_ghsl = paths.load_dataset("ghsl_degree_of_urbanisation")
    tb_ghsl = ds_meadow_ghsl.read("ghsl_degree_of_urbanisation")

    tb_ghsl = tb_ghsl.pivot(index=["country", "year"], columns="indicator", values="value").reset_index()
    tb_ghsl["total_population"] = tb_ghsl[
        ["rural_total_population", "urban_centre_population", "urban_cluster_population"]
    ].sum(axis=1)

    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")

    #
    # Process data.
    #
    tb_ghsl["urban_total"] = tb_ghsl["urban_centre_population"] + tb_ghsl["urban_cluster_population"]
    tb_ghsl["total_population"] = tb_ghsl[["rural_total_area", "urban_centre_area", "urban_cluster_area"]].sum(axis=1)
    tb_ghsl = tb_ghsl[["country", "year", "total_population", "urban_total"]]

    tb = geo.harmonize_countries(df=tb, countries_file=paths.country_mapping_path)

    tb = tb.drop(columns=["urban_center_name", "urban_area"])
    tb = pr.merge(tb, tb_ghsl, on=["country", "year"], how="outer")

    columns_to_aggregate = ["urban_pop", "urban_pop_1m", "total_population", "urban_total"]
    aggr_dict = {col: "sum" for col in columns_to_aggregate}
    # Add region aggregates.
    tb = geo.add_regions_to_table(
        tb,
        aggregations=aggr_dict,
        regions=REGIONS,
        ds_regions=ds_regions,
        ds_income_groups=ds_income_groups,
        min_num_values_per_year=1,
    )

    tb["share_1m_total"] = tb["urban_pop_1m"] / tb["total_population"]
    tb["share_1m_urban"] = tb["urban_pop_1m"] / tb["urban_total"]
    tb = tb.drop(columns=["total_population", "urban_total"])
    # Split data into estimates and projections.
    past_estimates = tb[tb["year"] < START_OF_PROJECTIONS].copy()
    future_projections = tb[tb["year"] >= START_OF_PROJECTIONS - 5].copy()

    # Now, for each column, split it into two (projections and estimates).
    for col in [
        "urban_pop",
        "urban_density",
        "urban_density_top_100",
        "urban_pop_top_100",
        "urban_pop_1m",
        "share_1m_total",
        "share_1m_urban",
    ]:
        if col not in ["country", "year"]:
            past_estimates[f"{col}_estimates"] = tb.loc[tb["year"] < START_OF_PROJECTIONS, col]
            future_projections[f"{col}_projections"] = tb.loc[tb["year"] >= START_OF_PROJECTIONS - 5, col]
            past_estimates = past_estimates.drop(columns=[col])
            future_projections = future_projections.drop(columns=[col])

    # Merge past estimates and future projections
    tb = pr.merge(past_estimates, future_projections, on=["country", "year"], how="outer")

    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

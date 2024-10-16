"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

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
START_OF_PROJECTIONS = 2025


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ghsl_degree_of_urbanisation")
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")
    # Read table from meadow dataset.
    tb = ds_meadow["ghsl_degree_of_urbanisation"].reset_index()

    #
    # Process data.
    #
    tb = geo.harmonize_countries(
        df=tb, countries_file=paths.country_mapping_path, excluded_countries_file=paths.excluded_countries_path
    )

    # Pivot the table to each indicator as a column.
    tb = tb.pivot(index=["country", "year"], columns="indicator", values="value").reset_index()
    columns_to_aggregate = [
        "rural_total_population",
        "urban_centre_population",
        "urban_cluster_population",
        "rural_total_area",
        "urban_centre_area",
        "urban_cluster_area",
    ]

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
    # Calculate population and area share from total within each urbanization level
    tb = calculate_shares(tb)

    # Split data into estimates and projections.
    tb = split_estimates_projections(tb)

    # Melt to make the metadata easier to generate
    tb = tb.melt(id_vars=["country", "year"], var_name="indicator", value_name="value")

    # Split the column into three parts: location, attribute, type for metadata generation
    tb[["location_type", "attribute", "type"]] = tb["indicator"].str.extract(
        r"(rural_total|urban_[\w]+)_(\w+?)_(estimates|projections)"
    )
    # Drop the original indicator column
    tb = tb.drop(columns=["indicator"])
    tb = tb.format(["country", "year", "location_type", "attribute", "type"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def calculate_shares(tb):
    # Calculate "share of" indicators
    tb["total_population"] = tb[["rural_total_population", "urban_centre_population", "urban_cluster_population"]].sum(
        axis=1
    )
    tb["total_area"] = tb[["rural_total_area", "urban_centre_area", "urban_cluster_area"]].sum(axis=1)

    tb["urban_total_popshare"] = (
        (tb["urban_centre_population"] + tb["urban_cluster_population"]) / tb["total_population"]
    ) * 100

    tb["urban_total_share"] = (tb["urban_centre_area"] + tb["urban_cluster_area"]) / tb["total_area"] * 100

    tb["rural_total_share"] = tb["rural_total_area"] / tb["total_area"] * 100
    tb["rural_total_popshare"] = tb["rural_total_population"] / tb["total_population"] * 100

    tb["urban_cluster_popshare"] = tb["urban_cluster_population"] / tb["total_population"] * 100
    tb["urban_centre_popshare"] = tb["urban_centre_population"] / tb["total_population"] * 100

    tb["urban_cluster_share"] = tb["urban_cluster_area"] / tb["total_area"] * 100
    tb["urban_centre_share"] = tb["urban_centre_area"] / tb["total_area"] * 100

    tb = tb.drop(columns=["total_population", "total_area"])
    return tb


def split_estimates_projections(tb):
    # Split data into estimates and projections.
    past_estimates = tb[tb["year"] < START_OF_PROJECTIONS].copy()
    future_projections = tb[tb["year"] >= START_OF_PROJECTIONS - 1].copy()

    # Now, for each column, split it into two (projections and estimates).
    for col in tb.columns:
        if col not in ["country", "year"]:
            past_estimates[f"{col}_estimates"] = tb.loc[tb["year"] < START_OF_PROJECTIONS, col]
            future_projections[f"{col}_projections"] = tb.loc[tb["year"] >= START_OF_PROJECTIONS - 1, col]
            past_estimates = past_estimates.drop(columns=[col])
            future_projections = future_projections.drop(columns=[col])

    # Merge past estimates and future projections
    tb = pr.merge(past_estimates, future_projections, on=["country", "year"], how="outer")

    return tb

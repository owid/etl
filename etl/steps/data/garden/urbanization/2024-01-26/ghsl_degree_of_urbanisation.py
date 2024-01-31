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
    "Low income",
    "Lower middle income",
    "Upper middle income",
    "High income",
    "World",
]


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
    tb = tb.pivot(index=["country", "year"], columns="indicator", values="value")

    tb = tb.underscore().reset_index()

    # Aggregate the table to the region level.
    columns_to_aggregate = [
        "degurba_l1_population_city",
        "degurba_l1_population_rural_area",
        "degurba_l1_population_town__and__suburbs",
        "degurba_l1_units_city",
        "degurba_l1_units_rural_area",
        "degurba_l1_units_town__and__suburbs",
        "degurba_l2_population_city",
        "degurba_l2_population_dense_town",
        "degurba_l2_population_mostly_uninhabited_area",
        "degurba_l2_population_rural_dispersed_area",
        "degurba_l2_population_semi_dense_town",
        "degurba_l2_population_suburbs_or_peri_urban_area",
        "degurba_l2_population_village",
        "degurba_l2_units_city",
        "degurba_l2_units_dense_town",
        "degurba_l2_units_mostly_uninhabited_area",
        "degurba_l2_units_rural_dispersed_area",
        "degurba_l2_units_semi_dense_town",
        "degurba_l2_units_suburbs_or_peri_urban_area",
        "degurba_l2_units_village",
        "total_pop",
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

    # Convert share of urban population to percentage.
    tb["share_of_urban_population"] = tb["share_of_urban_population"] * 100

    tb["share_of_urban_population_owid"] = (
        (tb["degurba_l1_population_city"] + tb["degurba_l1_population_town__and__suburbs"]) / tb["total_pop"]
    ) * 100

    # Create two new dataframes to separate data into estimates and projections (pre-2025 and post-2025 (five year intervals)))
    past_estimates = tb[tb["year"] < 2025].copy()
    future_projections = tb[tb["year"] >= 2025].copy()

    # Now, for each column in the original dataframe, split it into two (projections and estimates)
    for col in tb.columns:
        if col not in ["country", "year"]:
            past_estimates[f"{col}_estimates"] = tb.loc[tb["year"] < 2025, col]
            future_projections[f"{col}_projections"] = tb.loc[tb["year"] >= 2025, col]
            past_estimates = past_estimates.drop(columns=[col])
            future_projections = future_projections.drop(columns=[col])

    tb_merged = pr.merge(past_estimates, future_projections, on=["country", "year"], how="outer")
    tb_merged = tb_merged.set_index(["country", "year"], verify_integrity=True)

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb_merged], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

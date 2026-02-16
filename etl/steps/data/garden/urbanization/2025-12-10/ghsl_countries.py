"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

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
    "World",
]
START_OF_PROJECTIONS = 2025


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ghsl_countries")
    # Load regions dataset.
    ds_regions = paths.load_dataset("regions")

    # Load income groups dataset.
    ds_income_groups = paths.load_dataset("income_groups")
    # Read table from meadow dataset.
    tb = ds_meadow.read("ghsl_countries")

    #
    # Process data.
    #
    tb = paths.regions.harmonize_names(tb)

    # Pivot the table so each combination of urbanization_level and metric becomes a column.
    tb = tb.pivot(index=["country", "year"], columns="urbanization_level").reset_index()

    # Flatten column names (e.g., ('area', 'UC') becomes 'area_uc').
    tb.columns = ["_".join(str(col).lower() for col in cols if col) for cols in tb.columns.values]
    tb = tb.rename(columns={"country_": "country", "year_": "year"})

    # Rename urbanization levels to more readable names.
    column_mapping = {}
    for col in tb.columns:
        if col.endswith("_uc"):
            column_mapping[col] = col.replace("_uc", "_urban_centre")
        elif col.endswith("_ucl"):
            column_mapping[col] = col.replace("_ucl", "_urban_cluster")
        elif col.endswith("_rur"):
            column_mapping[col] = col.replace("_rur", "_rural_total")

    tb = tb.rename(columns=column_mapping)

    # Aggregate columns for summation.
    columns_to_aggregate = [col for col in tb.columns if col not in ["country", "year"]]
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

    # Calculate shares and densities.
    tb = calculate_shares_and_densities(tb)

    # Calculate population share change.
    tb = calculate_population_share_change(tb)

    # Create separate table for dominant population type before splitting.
    tb_dominant = create_dominant_population_table(tb.copy())

    # Split data into estimates and projections.
    tb = split_estimates_projections(tb)

    # Melt to make the metadata easier to generate.
    tb = tb.melt(id_vars=["country", "year"], var_name="indicator", value_name="value")

    # Split the indicator column for easier metadata generation.
    # Pattern: metric_urbanization_level_type (e.g., population_urban_centre_estimates)
    tb[["metric", "location_type", "data_type"]] = tb["indicator"].str.extract(
        r"(area|population|built_up_area|popshare|share|density|popshare_change)_(urban_centre|urban_cluster|rural_total|urban_total)_(estimates|projections)"
    )

    # Drop rows with NaN in index columns (non-matching indicators)
    tb = tb.dropna(subset=["metric", "location_type", "data_type"])

    # Drop the original indicator column.
    tb = tb.drop(columns=["indicator"])

    # Format the main table.
    tb = tb.format(["country", "year", "metric", "location_type", "data_type"])

    # Add metric dimension to dominant table to match main table structure
    tb_dominant["metric"] = "dominant_type"

    # Format the dominant population table.
    tb_dominant = tb_dominant.format(
        [
            "country",
            "year",
            "location_type",
            "data_type",
            "metric",
        ],
        short_name="ghsl_countries_dominant_type",
    )

    #
    # Save outputs.
    #
    # Create a new garden dataset with both tables.
    ds_garden = paths.create_dataset(
        tables=[tb, tb_dominant], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def calculate_shares_and_densities(tb):
    """Calculate share of population/area indicators and population densities."""
    # Calculate total population and area.
    tb["total_population"] = tb[["population_rural_total", "population_urban_centre", "population_urban_cluster"]].sum(
        axis=1
    )
    tb["total_area"] = tb[["area_rural_total", "area_urban_centre", "area_urban_cluster"]].sum(axis=1)

    # Urban total metrics (UC + UCL).
    tb["population_urban_total"] = tb["population_urban_centre"] + tb["population_urban_cluster"]
    tb["area_urban_total"] = tb["area_urban_centre"] + tb["area_urban_cluster"]

    # Population shares.
    tb["popshare_urban_total"] = (tb["population_urban_total"] / tb["total_population"]) * 100
    tb["popshare_rural_total"] = (tb["population_rural_total"] / tb["total_population"]) * 100
    tb["popshare_urban_cluster"] = (tb["population_urban_cluster"] / tb["total_population"]) * 100
    tb["popshare_urban_centre"] = (tb["population_urban_centre"] / tb["total_population"]) * 100

    # Area shares.
    tb["share_urban_total"] = (tb["area_urban_total"] / tb["total_area"]) * 100
    tb["share_rural_total"] = (tb["area_rural_total"] / tb["total_area"]) * 100
    tb["share_urban_cluster"] = (tb["area_urban_cluster"] / tb["total_area"]) * 100
    tb["share_urban_centre"] = (tb["area_urban_centre"] / tb["total_area"]) * 100

    # Population densities.
    tb["density_urban_centre"] = tb["population_urban_centre"] / tb["area_urban_centre"]
    tb["density_urban_cluster"] = tb["population_urban_cluster"] / tb["area_urban_cluster"]
    tb["density_rural_total"] = tb["population_rural_total"] / tb["area_rural_total"]
    tb["density_urban_total"] = tb["population_urban_total"] / tb["area_urban_total"]

    # Drop temporary total columns.
    tb = tb.drop(columns=["total_population", "total_area"])

    return tb


def calculate_population_share_change(tb):
    """Calculate population share change for each location type.

    This metric represents the change in population share (in percentage points)
    over 5-year intervals. For example, if a city's population share goes from
    30% to 35%, the popshare_change would be +5 percentage points.

    Note: Values can be volatile for small territories where entire areas may
    shift classification categories.
    """
    # Sort by country and year to ensure proper ordering for growth calculations.
    tb = tb.sort_values(["country", "year"]).reset_index(drop=True)

    # Calculate population share change for each location type.
    # Note: data is at 5-year intervals, so this represents the change over 5 years.
    for location_type in ["urban_centre", "urban_cluster", "rural_total", "urban_total"]:
        popshare_col = f"popshare_{location_type}"

        # Calculate change in population share: popshare_t - popshare_t-1 (in percentage points)
        tb[f"popshare_change_{location_type}"] = tb.groupby("country")[popshare_col].diff()

    return tb


def create_dominant_population_table(tb):
    """Create a separate table for dominant population type indicator."""
    # Determine which location type has the highest population share.
    dominant_col = tb[["popshare_urban_centre", "popshare_urban_cluster", "popshare_rural_total"]].idxmax(axis=1)

    # Map the column names to more readable labels.
    tb["value"] = dominant_col.map(
        {
            "popshare_urban_centre": "Cities",
            "popshare_urban_cluster": "Towns",
            "popshare_rural_total": "Rural areas",
        }
    )

    # Keep only country, year, and dominant_population_type columns.
    tb_dominant = tb[["country", "year", "value"]].copy()

    # Add data_type dimension column.
    # Split into estimates and projections.
    tb_dominant_estimates = tb_dominant[tb_dominant["year"] < START_OF_PROJECTIONS].copy()
    tb_dominant_estimates["data_type"] = "estimates"
    tb_dominant_estimates["location_type"] = (
        "by_dominant_type"  # Add a location_type value to allow merging with main table views later
    )

    tb_dominant_projections = tb_dominant[tb_dominant["year"] >= START_OF_PROJECTIONS - 5].copy()
    tb_dominant_projections["data_type"] = "projections"
    tb_dominant_projections["location_type"] = (
        "by_dominant_type"  # Add a location_type value to allow merging with main table views later
    )

    # Combine both.
    tb_dominant = pr.concat([tb_dominant_estimates, tb_dominant_projections], ignore_index=True)

    # Copy origins from source columns.
    if "popshare_urban_centre" in tb.columns and tb["popshare_urban_centre"].metadata.origins:
        tb_dominant["value"].metadata.origins = tb["popshare_urban_centre"].metadata.origins

    return tb_dominant


def split_estimates_projections(tb):
    """Split data into estimates (past) and projections (future)."""
    # Split data into estimates and projections.
    past_estimates = tb[tb["year"] < START_OF_PROJECTIONS].copy()
    future_projections = tb[tb["year"] >= START_OF_PROJECTIONS - 5].copy()

    # For each column, split it into two (projections and estimates).
    for col in tb.columns:
        if col not in ["country", "year"]:
            past_estimates[f"{col}_estimates"] = tb.loc[tb["year"] < START_OF_PROJECTIONS, col]
            future_projections[f"{col}_projections"] = tb.loc[tb["year"] >= START_OF_PROJECTIONS - 5, col]
            past_estimates = past_estimates.drop(columns=[col])
            future_projections = future_projections.drop(columns=[col])

    # Merge past estimates and future projections.
    tb = pr.merge(past_estimates, future_projections, on=["country", "year"], how="outer")

    return tb

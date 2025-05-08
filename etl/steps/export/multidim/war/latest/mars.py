from etl.collection import multidim

# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# INDICATOR dimension (columns starting with this prefix)
DIMENSION_INDICATOR = {
    # Deaths
    "number_deaths_ongoing_conflicts_high__": "deaths",
    "number_deaths_ongoing_conflicts_low__": "deaths",
    # Death rate
    "number_deaths_ongoing_conflicts_high_per_capita": "death_rate",
    "number_deaths_ongoing_conflicts_low_per_capita": "death_rate",
    # New wars: number
    "number_new_conflicts__": "wars_new",
    "number_new_conflicts_per_country": "wars_new_country_rate",
    "number_new_conflicts_per_country_pair": "wars_new_country_pair_rate",
    # Ongoing wars: number
    "number_ongoing_conflicts__": "wars_ongoing",
    "number_ongoing_conflicts_per_country": "wars_ongoing_country_rate",
    "number_ongoing_conflicts_per_country_pair": "wars_ongoing_country_pair_rate",
}
# ESTIMATE dimension
DIMENSION_ESTIMATE = {
    "high": "high",
    "low": "low",
}


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_mdim_config()

    # load table using load_data=False which only loads metadata significantly speeds this up
    ds = paths.load_dataset("mars")
    tb = ds.read("mars", load_data=False)

    # Adjust dimension metadata
    tb = adjust_dimensions(tb)

    config_new = multidim.expand_config(
        tb,
        indicator_names=[
            "deaths",
            "death_rate",
            "wars_ongoing",
            "wars_ongoing_country_rate",
        ],
    )
    # Combine dimension info from YAML + programmatically obtained
    config["dimensions"] = multidim.combine_config_dimensions(
        config_dimensions=config_new["dimensions"],
        config_dimensions_yaml=config["dimensions"],
    )

    # Combine views info from YAML + programmatically obtained
    config["views"] = config["views"] + config_new["views"]

    # Create mdim
    mdim = paths.create_mdim(
        config=config,
        mdim_name="mars",
    )

    # Save & upload
    mdim.save()


def adjust_dimensions(tb):
    # 1. Adjust indicators dictionary reference (maps full column name to actual indicator)
    dims = {}
    for prefix, indicator_name in DIMENSION_INDICATOR.items():
        columns = list(tb.filter(regex=prefix).columns)
        dims = {**dims, **{c: indicator_name for c in columns}}

    # 2. Iterate over columns and adjust dimensions
    columns = [col for col in tb.columns if col not in {"year", "country"}]
    for col in columns:
        # Overwrite original_short_name to actual indicator name
        if col not in dims:
            raise Exception(f"Column {col} not in indicator mapping")
        tb[col].metadata.original_short_name = dims[col]

        # Add high estimate dimension
        if "high" in col:
            tb[col].metadata.dimensions["estimate"] = "high"
        # Add low estimate dimension
        elif "low" in col:
            tb[col].metadata.dimensions["estimate"] = "low"
        # Add 'NA'
        else:
            tb[col].metadata.dimensions["estimate"] = "na"

    # 3. Adjust table-level dimension metadata
    if isinstance(tb.metadata.dimensions, list):
        tb.metadata.dimensions.append(
            {
                "name": "estimate",
                "slug": "estimate",
            }
        )
    return tb

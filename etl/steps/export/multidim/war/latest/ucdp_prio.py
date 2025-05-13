# from etl.db import get_engine
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# INDICATOR dimension (columns starting with this prefix)
DIMENSION_INDICATOR = {
    # Deaths
    "number_deaths_ongoing_conflicts__": "deaths",
    "number_deaths_ongoing_conflicts_high__": "deaths",
    "number_deaths_ongoing_conflicts_low__": "deaths",
    # Death rate
    "number_deaths_ongoing_conflicts_per_capita": "death_rate",
    "number_deaths_ongoing_conflicts_high_per_capita": "death_rate",
    "number_deaths_ongoing_conflicts_low_per_capita": "death_rate",
    # New wars: number
    # "number_new_conflicts__": "wars_new",
    # "number_new_conflicts_per_country": "wars_new_country_rate",
    # "number_new_conflicts_per_country_pair": "wars_new_country_pair_rate",
    # # Ongoing wars: number
    # "number_ongoing_conflicts__": "wars_ongoing",
    # "number_ongoing_conflicts_per_country": "wars_ongoing_country_rate",
    # "number_ongoing_conflicts_per_country_pair": "wars_ongoing_country_pair_rate",
}


def run() -> None:
    # Load configuration from adjacent yaml file.
    config = paths.load_collection_config()

    # load table using load_data=False which only loads metadata significantly speeds this up
    ds_up = paths.load_dataset("ucdp_prio")
    ds_u = paths.load_dataset("ucdp")
    tb_up = ds_up.read("ucdp_prio", load_data=False)
    tb_u = ds_u.read("ucdp", load_data=False)

    tb = adjust_dimensions(tb_up)

    # Create collection
    c = paths.create_collection(
        config=config,
        tb=tb_up,
        indicator_names=[
            "deaths",
            "death_rate",
            # "wars_ongoing",
            # "wars_ongoing_country_rate",
        ],
        dimensions={
            "conflict_type": [
                "state-based",
                "interstate",
                "intrastate (non-internationalized)",
                "intrastate (internationalized)",
                "extrasystemic",
            ],
            "estimate": "*",
        },
    )

    # Save & upload
    c.save()


def adjust_dimensions(tb):
    """Add dimensions to table columns.

    It adds field `indicator` and `estimate`.
    """
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
        if "_high_" in col:
            tb[col].metadata.dimensions["estimate"] = "high"
        # Add low estimate dimension
        elif "_low_" in col:
            tb[col].metadata.dimensions["estimate"] = "low"
        # Add 'NA'
        else:
            tb[col].metadata.dimensions["estimate"] = "best"

    # 3. Adjust table-level dimension metadata
    if isinstance(tb.metadata.dimensions, list):
        tb.metadata.dimensions.append(
            {
                "name": "estimate",
                "slug": "estimate",
            }
        )
    return tb

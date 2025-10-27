"""Garden step that combines OECD family database sources into a single dataset."""

import owid.catalog.processing as pr

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow datasets
    ds_marriage_divorce = paths.load_dataset("marriage_divorce_rates")
    ds_births_outside_marriage = paths.load_dataset("births_outside_marriage")
    ds_children_in_families = paths.load_dataset("children_in_families")
    ds_garden_oecd_hist = paths.load_dataset("family_database")

    # Get tables from each dataset
    tb_marriage_divorce = ds_marriage_divorce.read("marriage_divorce_rates")
    tb_births_outside_marriage = ds_births_outside_marriage.read("births_outside_marriage")
    tb_children_in_families = ds_children_in_families.read("children_in_families")
    tb_garden_oecd_hist = ds_garden_oecd_hist.read("family_database")

    # Extract historical data columns
    tb_garden_oecd_hist = tb_garden_oecd_hist[
        ["country", "year", "marriage_rate", "divorce_rate", "share_of_births_outside_of_marriage__pct_of_all_births"]
    ]

    #
    # Process data.
    #

    # Harmonize country names for all tables
    tb_marriage_divorce = geo.harmonize_countries(
        tb_marriage_divorce, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb_births_outside_marriage = geo.harmonize_countries(
        tb_births_outside_marriage, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )
    tb_children_in_families = geo.harmonize_countries(
        tb_children_in_families, countries_file=paths.country_mapping_path, warn_on_unused_countries=False
    )

    # Process marriage/divorce rates - merge historical data with new data
    # Filter for marriage and divorce rates only from new data
    tb_marriage_new = tb_marriage_divorce[
        (tb_marriage_divorce["indicator"].isin(["marriage_rate", "divorce_rate"]))
        & (tb_marriage_divorce["gender"] == "Both")
    ][["country", "year", "indicator", "value"]].copy()

    # Pivot to get marriage_rate and divorce_rate as columns
    tb_marriage_new = tb_marriage_new.pivot(
        index=["country", "year"], columns="indicator", values="value"
    ).reset_index()
    tb_marriage_new.columns.name = None

    # Validate historical data has expected columns
    assert "marriage_rate" in tb_garden_oecd_hist.columns, "Historical data missing marriage_rate column"
    assert "divorce_rate" in tb_garden_oecd_hist.columns, "Historical data missing divorce_rate column"

    # Merge historical data with new data - new data takes precedence where it exists
    tb_marriage_combined = pr.merge(
        tb_garden_oecd_hist[["country", "year", "marriage_rate", "divorce_rate"]],
        tb_marriage_new,
        on=["country", "year"],
        how="outer",
        suffixes=("_hist", "_new"),
    )

    # Validate merge created expected columns
    required_cols = ["marriage_rate_hist", "marriage_rate_new", "divorce_rate_hist", "divorce_rate_new"]
    missing_cols = [col for col in required_cols if col not in tb_marriage_combined.columns]
    if missing_cols:
        raise ValueError(f"Merge failed - missing columns: {missing_cols}")

    # Use new data where available, otherwise use historical data
    tb_marriage_combined["marriage_rate"] = tb_marriage_combined["marriage_rate_new"].fillna(
        tb_marriage_combined["marriage_rate_hist"]
    )
    tb_marriage_combined["divorce_rate"] = tb_marriage_combined["divorce_rate_new"].fillna(
        tb_marriage_combined["divorce_rate_hist"]
    )
    tb_marriage_combined = tb_marriage_combined[["country", "year", "marriage_rate", "divorce_rate"]]

    # Convert back to long format for consistency with the rest of the data
    tb_marriage_combined_long = tb_marriage_combined.melt(
        id_vars=["country", "year"],
        value_vars=["marriage_rate", "divorce_rate"],
        var_name="indicator",
        value_name="value",
    )
    # Add gender column
    tb_marriage_combined_long["gender"] = "Both"

    # Process births outside marriage - merge with historical data
    tb_births_new = tb_births_outside_marriage[["country", "year", "births_outside_marriage"]].copy()

    # Validate historical data has expected column
    hist_col = "share_of_births_outside_of_marriage__pct_of_all_births"
    assert hist_col in tb_garden_oecd_hist.columns, f"Historical data missing {hist_col} column"

    # Rename columns to match for cleaner merge
    tb_births_hist = tb_garden_oecd_hist[["country", "year", hist_col]].copy()
    tb_births_hist = tb_births_hist.rename(columns={hist_col: "births_outside_marriage"})

    # Merge historical births data with new data
    tb_births_combined = pr.merge(
        tb_births_hist,
        tb_births_new,
        on=["country", "year"],
        how="outer",
        suffixes=("_hist", "_new"),
    )

    # Use new data where available, otherwise use historical data
    tb_births_combined["births_outside_marriage"] = tb_births_combined["births_outside_marriage_new"].fillna(
        tb_births_combined["births_outside_marriage_hist"]
    )
    tb_births_combined = tb_births_combined[["country", "year", "births_outside_marriage"]]

    # Keep mean age data from new dataset only (no historical equivalent)
    tb_mean_age = tb_marriage_divorce[tb_marriage_divorce["indicator"] == "mean_age_first_marriage"][
        ["country", "year", "gender", "indicator", "value"]
    ].copy()

    #
    # Save outputs.
    #
    # Create a new garden dataset with multiple tables
    tables = [
        tb_mean_age.format(["country", "year", "gender", "indicator"], short_name="mean_age_first_marriage"),
        tb_marriage_combined_long.format(
            ["country", "year", "gender", "indicator"], short_name="marriage_divorce_rates"
        ),
        tb_births_combined.format(["country", "year"], short_name="births_outside_marriage"),
        tb_children_in_families.format(["country", "year", "indicator"]),
    ]
    ds_garden = paths.create_dataset(tables=tables, check_variables_metadata=True)

    # Save the dataset
    ds_garden.save()

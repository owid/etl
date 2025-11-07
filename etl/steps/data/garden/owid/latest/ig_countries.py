"""Load a meadow dataset and create a garden dataset."""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.data_helpers.misc import expand_time_column
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("ig_countries")
    ds_regions = paths.load_dataset("regions")

    # Read table from meadow dataset.
    tb = ds_meadow.read("ig_countries")
    tb_regions = ds_regions.read("regions")

    #
    # Process data.
    #
    ## Dtypes
    tb = tb.astype(
        {
            "country": "string",
            "date": "datetime64[ns]",
        }
    )

    tb = tb.sort_values("date")

    # Get share of post
    x = tb.groupby("date").country.transform("nunique")
    tb["share_post"] = 1 / x

    ##################
    # SUMMARY TABLE
    # Sanity check
    assert tb[["country", "date"]].value_counts().max() == 1, "Duplicate country-date entries found!"
    # Get total number of posts
    num_posts = tb["date"].nunique()

    # Get share of posts with country X
    tb_summary = make_tb_summary(tb, num_posts)

    ##################
    # MAIN TABLE EDITS
    tb = tb.rename(
        columns={
            "count": "is_mentioned",
            "share_post": "is_mentioned_weighted",
        }
    )

    # Add missing countries
    mask = tb_regions["region_type"] == "country"
    countries = tb_regions.loc[mask, "name"].unique().tolist()
    countries_missing = [c for c in countries if c not in tb["country"].unique()]
    tb_missing = Table({"country": countries_missing, "date": tb["date"].min(), "count": 0})
    tb = pr.concat([tb, tb_missing], ignore_index=True)

    # Expand time column
    tb = expand_time_column(tb, time_col="date", dimension_col="country", method="observed", fillna_method="zero")

    # Add rolling window indicators
    # For each window (30, 90, 365 days) and overall, calculate:
    # - count: Number of posts mentioning the country
    # - share: Share of total posts mentioning the country
    # - share_weighted: Weighted share accounting for multiple countries per post
    tb = add_cum_values(tb)

    # Define index columns for reuse
    index_cols = ["country", "date"]

    # Step 1: Split into two tables
    # tb_mentioned: contains daily mention indicators
    tb_mentioned = tb[index_cols + ["is_mentioned", "is_mentioned_weighted", "is_mentioned_ever"]].copy()

    # Step 2: Transform main table to have time_window as dimension
    tb_windows = transform_to_long_format(tb, index_cols)

    # Metadata
    tb_mentioned["is_mentioned"] = tb_mentioned["is_mentioned"].copy_metadata(tb["date"])
    tb_mentioned["is_mentioned_ever"] = tb_mentioned["is_mentioned_ever"].copy_metadata(tb["date"])
    tb_summary["proportion"] = tb_summary["proportion"].copy_metadata(tb["date"])
    tb_summary["proportion_weighed"] = tb_summary["proportion_weighed"].copy_metadata(tb["date"])
    tb_windows["share"] = tb_windows["share"].copy_metadata(tb["date"])
    tb_windows["share_weighted"] = tb_windows["share_weighted"].copy_metadata(tb["date"])
    tb_windows["count"] = tb_windows["count"].copy_metadata(tb["date"])

    # Format tables with appropriate indices
    tables = [
        tb_mentioned.format(index_cols, short_name="mentions"),
        tb_windows.format(index_cols + ["time_window"], short_name="ig_countries"),
        tb_summary.format(["country"], short_name="summary"),
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_cum_values(tb):
    windows = [30, 90, 365, None]  # None = all time

    # Ensure date column is datetime and sort
    tb = tb.sort_values(["country", "date"])

    for window in windows:
        suffix = f"_{window}d" if window else "_all"

        if window:
            # Time-based rolling window - use pandas built-in with date index
            # We need to temporarily set date as index for time-based rolling
            def apply_time_rolling(group):
                # Set date as index for time-based rolling
                g = group.set_index("date")

                # Use pandas' time-based rolling with window specified as offset string
                window_str = f"{window}D"

                # Calculate rolling sums for mentions
                g[f"count{suffix}"] = g["is_mentioned"].rolling(window_str, closed="both").sum()
                g[f"share_weighted{suffix}"] = g["is_mentioned_weighted"].rolling(window_str, closed="both").sum()

                # For total posts, we need to count unique dates in the window
                # Use a trick: create a column of 1s and sum them in rolling window
                g[f"total_posts{suffix}"] = g.index.to_series().rolling(window_str, closed="both").count()

                return g.reset_index()

            # Apply to each country group
            result = tb.groupby("country", group_keys=False).apply(apply_time_rolling)

            # Update the main dataframe with calculated columns
            tb[f"count{suffix}"] = result[f"count{suffix}"].values
            tb[f"share_weighted{suffix}"] = result[f"share_weighted{suffix}"].values
            tb[f"total_posts{suffix}"] = result[f"total_posts{suffix}"].values

        else:
            # Cumulative calculations (all time)
            tb[f"count{suffix}"] = tb.groupby("country")["is_mentioned"].cumsum()
            tb[f"share_weighted{suffix}"] = tb.groupby("country")["is_mentioned_weighted"].cumsum()
            tb[f"total_posts{suffix}"] = tb.groupby("country").cumcount() + 1

        # Calculate share as percentage (normalize by total posts in window)
        tb[f"share{suffix}"] = 100 * tb[f"count{suffix}"] / tb[f"total_posts{suffix}"]
        tb[f"share_weighted{suffix}"] = 100 * tb[f"share_weighted{suffix}"] / tb[f"total_posts{suffix}"]

        # Clean up temporary total_posts column
        tb = tb.drop(columns=[f"total_posts{suffix}"])

        # Copy metadata from the original columns
        # tb[f"count{suffix}"].m.origins = origins
        # tb[f"share{suffix}"].m.origins = origins
        # tb[f"share_weighted{suffix}"].m.origins = origins

    tb["is_mentioned_ever"] = (tb["count_all"] > 0).astype(int)
    # tb["is_mentioned_ever"].m.origins = origins
    return tb


def transform_to_long_format(tb: Table, index_cols: list[str]) -> Table:
    """Transform table with rolling window columns to long format with time_window dimension.

    Parameters
    ----------
    tb : Table
        Input table with columns like count_30d, share_30d, share_weighted_30d for different windows
    index_cols : list[str]
        List of index column names (e.g., ["country", "date"])

    Returns
    -------
    Table
        Long format table with time_window as additional dimension
    """
    # Extract rolling window columns for melting
    window_suffixes = ["30d", "90d", "365d", "all"]

    # Build list of dataframes for each window
    dfs = []
    for suffix in window_suffixes:
        # Select columns for this window
        df_window = tb[index_cols + [f"count_{suffix}", f"share_{suffix}", f"share_weighted_{suffix}"]].copy()
        # Add time_window column
        df_window["time_window"] = suffix
        # Rename columns to remove suffix
        df_window = df_window.rename(
            columns={
                f"count_{suffix}": "count",
                f"share_{suffix}": "share",
                f"share_weighted_{suffix}": "share_weighted",
            }
        )
        dfs.append(df_window)

    # Concatenate all windows into single long-format table
    tb_windows = pr.concat(dfs, ignore_index=True)

    return tb_windows


def make_tb_summary(tb, num_posts):
    tb_summary_a = tb["country"].value_counts().to_frame().reset_index()
    tb_summary_a["proportion"] = 100 * tb_summary_a["count"] / num_posts
    tb_summary_a = tb_summary_a.drop(columns=["count"])
    # Get average country-mention per post
    tb_summary_b = tb.groupby("country", as_index=False)["share_post"].sum()
    tb_summary_b["proportion_weighed"] = 100 * tb_summary_b["share_post"] / num_posts
    tb_summary_b = tb_summary_b.drop(columns=["share_post"])
    # Merge
    tb_summary = tb_summary_a.merge(tb_summary_b, on="country", how="outer")
    tb_summary["year"] = 2025
    tb_summary = Table(tb_summary)

    return tb_summary

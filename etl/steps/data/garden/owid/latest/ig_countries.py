"""Load a meadow dataset and create a garden dataset."""

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

    # Read table from meadow dataset.
    tb = ds_meadow["ig_countries"].reset_index()

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

    ##################
    # MAIN TABLE EDITS
    tb = tb.rename(
        columns={
            "count": "is_mentioned",
            "share_post": "is_mentioned_weighed",
        }
    )
    # Expand time column
    tb = expand_time_column(tb, time_col="date", dimension_col="country", method="observed", fillna_method="zero")

    # Add rolling window indicators
    # For each window (30, 90, 365 days) and overall, calculate:
    # - count: Number of posts mentioning the country
    # - share: Share of total posts mentioning the country
    # - share_weighed: Weighted share accounting for multiple countries per post
    tb = add_cum_values(tb, origins=tb["date"].origins)

    # Metadata
    tb["is_mentioned"] = tb["date"].copy_metadata(tb["date"])
    tb_summary["proportion"] = tb_summary["proportion"].copy_metadata(tb["date"])
    tb_summary["proportion_weighed"] = tb_summary["proportion_weighed"].copy_metadata(tb["date"])

    # expand_time_column(tb)
    tables = [
        tb.format(["country", "date"]),
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


def add_cum_values(tb, origins):
    windows = [30, 90, 365, None]  # None = all time

    for window in windows:
        suffix = f"_{window}d" if window else "_all"
        # Group by country and calculate rolling/cumulative metrics
        if window:
            # Rolling window calculations
            tb[f"count{suffix}"] = (
                tb.groupby("country")["is_mentioned"]
                .rolling(window=window, min_periods=1)
                .sum()
                .reset_index(level=0, drop=True)
            )
            tb[f"share_weighed{suffix}"] = (
                tb.groupby("country")["is_mentioned_weighed"]
                .rolling(window=window, min_periods=1)
                .sum()
                .reset_index(level=0, drop=True)
            )

            # Get total number of posts in the rolling window for share calculation
            # Count rows in the rolling window
            tb[f"total_posts{suffix}"] = (
                tb.groupby("country")["is_mentioned"]
                .rolling(window=window, min_periods=1)
                .count()
                .reset_index(level=0, drop=True)
            )
        else:
            # Cumulative calculations (all time)
            tb[f"count{suffix}"] = tb.groupby("country")["is_mentioned"].cumsum()
            tb[f"share_weighed{suffix}"] = tb.groupby("country")["is_mentioned_weighed"].cumsum()

            # Total posts up to current date (cumulative row count per country)
            tb[f"total_posts{suffix}"] = tb.groupby("country").cumcount() + 1

        # Calculate share as percentage (normalize by total posts in window)
        tb[f"share{suffix}"] = 100 * tb[f"count{suffix}"] / tb[f"total_posts{suffix}"]
        tb[f"share_weighed{suffix}"] = 100 * tb[f"share_weighed{suffix}"] / tb[f"total_posts{suffix}"]

        # Clean up temporary total_posts column
        tb = tb.drop(columns=[f"total_posts{suffix}"])

        # Copy metadata from the original columns
        tb[f"count{suffix}"].m.origins = origins
        tb[f"share{suffix}"].m.origins = origins
        tb[f"share_weighed{suffix}"].m.origins = origins

    tb["is_mentioned_ever"] = (tb["count_all"] > 0).astype(int)
    tb["is_mentioned_ever"].m.origins = origins
    return tb

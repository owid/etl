"""Load a meadow dataset and create a garden dataset."""

from owid.catalog import Table

from etl.data_helpers.misc import expand_time_column
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
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
    # Get number of posts with country X
    tb_summary_a = tb["country"].value_counts(normalize=True).to_frame().reset_index()

    num_posts = tb["date"].nunique()
    tb_summary_b = tb.groupby("country", as_index=False)["share_post"].sum()
    tb_summary_b["proportion_weighed"] = tb_summary_b["share_post"] / num_posts
    tb_summary_b = tb_summary_b.drop(columns=["share_post"])

    tb_summary = tb_summary_a.merge(tb_summary_b, on="country", how="outer")
    tb_summary["year"] = 2024

    tb_summary = Table(tb_summary)

    # Get
    # Expand time column
    tb = expand_time_column(tb, time_col="date", dimension_col="country", method="observed", fillna_method="zero")

    # Cumulative
    tb["counts_cum"] = tb.groupby("country")["count"].cumsum()
    tb["count"] = (tb["counts_cum"] > 0).astype(int)

    # Metadata
    tb["counts_cum"] = tb["counts_cum"].copy_metadata(tb["date"])
    tb["count"] = tb["count"].copy_metadata(tb["date"])
    tb_summary["proportion"] = 100 * tb_summary["proportion"].copy_metadata(tb["date"])
    tb_summary["proportion_weighed"] = 100 * tb_summary["proportion_weighed"].copy_metadata(tb["date"])

    # expand_time_column(tb)
    tables = [
        tb.format(["country", "date"]),
        tb_summary.format(["country"], short_name="summary"),
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()

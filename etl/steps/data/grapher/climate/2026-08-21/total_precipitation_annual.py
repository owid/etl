"""Load a garden dataset and create a grapher dataset."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    #
    # Load inputs.
    #
    # Load garden dataset.
    ds_garden = paths.load_dataset("total_precipitation")
    tb = ds_garden.read("total_precipitation")

    #
    # Process data.
    #

    # Get the year and month.
    tb["year"] = tb["time"].astype(str).str[0:4].astype(int)
    tb["month"] = tb["time"].astype(str).str[5:7].astype(int)

    # Annual totals are only meaningful for years the source has reported in full, so drop any
    # year with fewer than 12 months. Derived from the data rather than hardcoded, otherwise the
    # series silently freezes as soon as the calendar moves on.
    months_per_year = tb.groupby("year")["month"].nunique()
    incomplete_years = months_per_year[months_per_year < 12].index.tolist()
    if incomplete_years:
        paths.log.info("Dropping incomplete years", years=sorted(incomplete_years))
    tb = tb[~tb["year"].isin(incomplete_years)].drop(columns=["month"])

    # Group by year and sum the specified columns.
    tb = (
        tb.groupby(["year", "country"])
        .agg({"total_precipitation": "sum", "precipitation_anomaly": "sum"})
        .reset_index()
    )

    tb = tb.format(["year", "country"])

    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds_grapher = paths.create_dataset(tables=[tb], default_metadata=ds_garden.metadata, check_variables_metadata=True)

    ds_grapher.save()

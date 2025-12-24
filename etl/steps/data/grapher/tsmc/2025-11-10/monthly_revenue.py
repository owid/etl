"""Load garden dataset and create grapher views."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Create grapher dataset."""
    # Load garden dataset.
    ds_garden = paths.load_dataset("monthly_revenue")

    # Load the long format table which is better for time series analysis
    tb_monthly = ds_garden.read("tsmc_monthly_revenue", reset_index=True)
    tb_yearly = ds_garden.read("tsmc_yearly_revenue", reset_index=True)

    tb_monthly["country"] = "Taiwan"
    tb_yearly["country"] = "Taiwan"

    tb_monthly = tb_monthly.format(["country", "date"])
    tb_yearly = tb_yearly.format(["country", "year"])

    # Save outputs.
    ds_grapher = paths.create_dataset(
        tables=[tb_monthly, tb_yearly], check_variables_metadata=True, default_metadata=ds_garden.metadata
    )
    ds_grapher.save()

"""Load meadow dataset and create garden dataset with enhanced indicators."""

from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run() -> None:
    """Create garden dataset with additional calculated indicators."""
    # Load inputs.
    ds_meadow = paths.load_dataset("monthly_revenue")

    # Load the long format table which is better for time series analysis
    tb_monthly = ds_meadow.read("tsmc_monthly_revenue", reset_index=False)
    tb_yearly = ds_meadow.read("tsmc_yearly_revenue", reset_index=False)

    # Save outputs.
    ds_garden = paths.create_dataset(
        tables=[tb_monthly, tb_yearly], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )
    ds_garden.save()

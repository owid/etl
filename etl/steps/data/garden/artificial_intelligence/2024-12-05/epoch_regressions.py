"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from sklearn.linear_model import LinearRegression

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Constants for defining the time periods
DL_ERA_START = 2010
START_DATE = 1950
END_DATE = 2025.2


def run(dest_dir: str) -> None:
    paths.log.info("epoch.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("epoch")

    # Read table from meadow dataset.
    tb = ds_meadow["epoch"].reset_index()

    # Run regression analysis and concatenate results
    tb_trend = run_regression(tb)
    tb = tb.drop("frac_year", axis=1)
    tb = pr.concat([tb_trend, tb])

    # Format the table
    tb = tb.format(["days_since_1949", "model"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("epoch.end")


def fit_exponential(models, metric):
    """Fit an exponential model to the given metric data. Code provided by Epoch AI team."""
    x = models["frac_year"].values.reshape(-1, 1)
    y = models[metric]

    # Filter out non-positive values
    positive_mask = y > 0
    x = x[positive_mask]
    y = y[positive_mask]

    # Apply log10 transformation
    y = np.log10(y)

    # Filter out infinite and extremely large values
    finite_mask = np.isfinite(y) & (y < np.finfo(np.float32).max)
    x = x[finite_mask]
    y = y[finite_mask]

    # Fit linear regression model
    reg = LinearRegression().fit(x, y)
    return reg.intercept_, reg.coef_[0]


def run_regression(tb):
    """Run regression analysis on the given table and return the updated table."""
    # Add fractional year for sorting and processing
    publication_dates = tb["publication_date"]
    tb.loc[:, "frac_year"] = (
        publication_dates.dt.year + (publication_dates.dt.month - 1) / 12 + (publication_dates.dt.day - 1) / 365
    )
    tb = tb.sort_values(by="frac_year")

    # Define periods dynamically
    periods = {
        f"{START_DATE}–{DL_ERA_START}": (tb["frac_year"] < DL_ERA_START),
        f"{DL_ERA_START}–{int(END_DATE)}": ((tb["frac_year"] >= DL_ERA_START) & (tb["frac_year"] < END_DATE)),
    }
    # Define year grids dynamically
    year_grids = {
        f"{START_DATE}–{DL_ERA_START}": np.array([START_DATE, DL_ERA_START]),
        f"{DL_ERA_START}–{int(END_DATE)}": np.array([DL_ERA_START, END_DATE]),
    }

    metrics = ["training_computation_petaflop", "parameters", "training_dataset_size__datapoints"]
    new_tables = []

    for metric in metrics:
        # Filter out models without the metric information
        tb_metric = tb[pd.notnull(tb[metric])]
        dfs = []

        for period_name, condition in periods.items():
            # Subset data for the current period
            period_data = tb_metric[condition]

            # Fit exponential model
            fit = fit_exponential(period_data, metric)
            oom_per_year = fit[1]
            info = f"{10**oom_per_year:.1f}x/year"

            # Log the results
            paths.log.info(f"{period_name} ({metric}): {info}")

            # Calculate the regression line for the current period
            year_grid = year_grids[period_name]
            line = 10 ** (fit[0] + year_grid * fit[1])

            # Create DataFrame for the current period
            df = pd.DataFrame(
                {
                    "days_since_1949": [
                        period_data["days_since_1949"].min(),
                        period_data["days_since_1949"].max(),
                    ],
                    f"{metric}": [line[0], line[-1]],
                    "model": [f"{info} between {period_name}"] * 2,
                }
            )
            dfs.append(df)

        # Combine the DataFrames for all periods for the current metric
        df_combined = pd.concat(dfs, ignore_index=True)
        new_tables.append(df_combined)

    # Merge all the new DataFrames
    tb_new = new_tables[0]
    for tb_m in new_tables[1:]:
        tb_new = pd.merge(tb_new, tb_m, on=["model", "days_since_1949"], how="outer")

    # Convert to OWID Table and add metadata
    tb_new = Table(tb_new, short_name=paths.short_name)
    for column in tb_new.columns:
        tb_new[column].metadata.origins = tb["publication_date"].metadata.origins

    return tb_new

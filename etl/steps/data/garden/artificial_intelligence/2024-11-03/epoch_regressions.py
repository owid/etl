"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Table
from sklearn.linear_model import LinearRegression

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
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
    tb = tb.format(["days_since_1949", "system"])

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
    publication_dates = tb["publication_date"]
    tb.loc[:, "frac_year"] = (
        publication_dates.dt.year + (publication_dates.dt.month - 1) / 12 + (publication_dates.dt.day - 1) / 365
    )
    tb = tb.sort_values(by="frac_year")

    metrics = ["training_computation_petaflop", "parameters", "training_dataset_size__datapoints"]
    new_tables = []

    for m, metric in enumerate(metrics):
        # Filter out models without the metric information
        tb_metric = tb[pd.notnull(tb[metric])]

        # Define time periods
        period1 = tb_metric[tb_metric["frac_year"] < 2010]
        period2 = tb_metric[(tb_metric["frac_year"] >= 2010) & (tb_metric["frac_year"] < 2018)]
        period3 = tb_metric[tb_metric["frac_year"] >= 2018]

        # Fit exponential models for each period
        fit1 = fit_exponential(period1, metric)
        fit2 = fit_exponential(period2, metric)
        fit3 = fit_exponential(period3, metric)

        # Calculate OOM per year for each period
        oom1 = fit1[1]
        oom2 = fit2[1]
        oom3 = fit3[1]

        # Log the results
        info1 = f"{10**oom1:.1f}x/year"
        info2 = f"{10**oom2:.1f}x/year"
        info3 = f"{10**oom3:.1f}x/year"

        paths.log.info(f"1950–2010 ({metric}): {info1}")
        paths.log.info(f"2010–2018 ({metric}): {info2}")
        paths.log.info(f"2018–2025 ({metric}): {info3}")

        # Define year grids for each period
        year_grid1 = np.array([1950, 2010])
        year_grid2 = np.array([2010, 2018])
        year_grid3 = np.array([2018, 2025])

        # Calculate lines for each period
        line1 = 10 ** (fit1[0] + year_grid1 * fit1[1])
        line2 = 10 ** (fit2[0] + year_grid2 * fit2[1])
        line3 = 10 ** (fit3[0] + year_grid3 * fit3[1])

        # Create DataFrames for each period
        df1 = pd.DataFrame(
            {
                "days_since_1949": [
                    tb_metric["days_since_1949"].min(),
                    tb_metric[tb_metric["frac_year"] < 2010]["days_since_1949"].max(),
                ],
                f"{metric}": [line1[0], line1[-1]],
                "system": [f"{info1}"] * 2,
            }
        )

        df2 = pd.DataFrame(
            {
                "days_since_1949": [
                    tb_metric[tb_metric["frac_year"] >= 2010]["days_since_1949"].min(),
                    tb_metric[tb_metric["frac_year"] < 2018]["days_since_1949"].max(),
                ],
                f"{metric}": [line2[0], line2[-1]],
                "system": [f"{info2}"] * 2,
            }
        )

        df3 = pd.DataFrame(
            {
                "days_since_1949": [
                    tb_metric[tb_metric["frac_year"] >= 2018]["days_since_1949"].min(),
                    tb_metric["days_since_1949"].max(),
                ],
                f"{metric}": [line3[0], line3[-1]],
                "system": [f"{info3}"] * 2,
            }
        )

        # Combine all three DataFrames
        df_combined = pd.concat([df1, df2, df3], ignore_index=True)
        new_tables.append(df_combined)

    # Merge all the new DataFrames
    tb_new = new_tables[0]
    for tb_m in new_tables[1:]:
        tb_new = pd.merge(tb_new, tb_m, on=["system", "days_since_1949"], how="outer")

    # Convert to OWID Table and add metadata
    tb_new = Table(tb_new, short_name=paths.short_name)
    for column in tb_new.columns:
        tb_new[column].metadata.origins = tb["publication_date"].metadata.origins

    return tb_new

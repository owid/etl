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

        # Fit exponential models for pre-DL and DL eras
        pre_dl_models = tb_metric[tb_metric["frac_year"] < DL_ERA_START]
        pre_dl_fit = fit_exponential(pre_dl_models, metric)
        pre_dl_oom_per_year = pre_dl_fit[1]

        dl_models = tb_metric[tb_metric["frac_year"] >= DL_ERA_START]
        dl_fit = fit_exponential(dl_models, metric)
        dl_oom_per_year = dl_fit[1]

        # Log the results
        pre_dl_info = f"{10**pre_dl_oom_per_year:.1f}x/year"
        dl_info = f"{10**dl_oom_per_year:.1f}x/year"
        paths.log.info(f"Pre Deep Learning Era ({metric}): {pre_dl_info}")
        paths.log.info(f"Deep Learning Era ({metric}): {dl_info}")

        # Define the year grids for the periods 1950 to 2010 and 2010 to 2025 with just two points
        pre_dl_year_grid = np.array([START_DATE, DL_ERA_START])
        dl_year_grid = np.array([DL_ERA_START, END_DATE])

        # Calculate the lines for each period using the fitted exponential models
        pre_dl_line = 10 ** (pre_dl_fit[0] + pre_dl_year_grid * pre_dl_fit[1])
        dl_line = 10 ** (dl_fit[0] + dl_year_grid * dl_fit[1])

        # Create new DataFrames for pre-deep learning and deep learning era trends with only necessary columns
        pre_dl_df = pd.DataFrame(
            {
                "days_since_1949": [
                    tb_metric["days_since_1949"].min(),
                    tb_metric[tb_metric["frac_year"] < DL_ERA_START]["days_since_1949"].max(),
                ],
                f"{metric}": [pre_dl_line[0], pre_dl_line[-1]],
                "system": [f"{pre_dl_info}"] * 2,
            }
        )

        dl_df = pd.DataFrame(
            {
                "days_since_1949": [
                    tb_metric[tb_metric["frac_year"] >= DL_ERA_START]["days_since_1949"].min(),
                    tb_metric["days_since_1949"].max(),
                ],
                f"{metric}": [dl_line[0], dl_line[-1]],
                "system": [f"{dl_info}"] * 2,
            }
        )

        # Combine the pre-deep learning and deep learning era DataFrames
        df_combined = pd.concat([pre_dl_df, dl_df], ignore_index=True)
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

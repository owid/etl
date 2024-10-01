"""Load a meadow dataset and create a garden dataset."""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
DL_ERA_START = 2010
START_DATE = 1950
END_DATE = 2025.2

BOOTSTRAP_SAMPLE_SIZE = 1000
BOOTSTRAP_CI_WIDTH = 90


def run(dest_dir: str) -> None:
    paths.log.info("epoch.start")

    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("epoch")

    # Read table from meadow dataset.
    tb = ds_meadow["epoch"]
    tb = tb.reset_index()
    tb = run_regression(tb)
    tb = tb.format(["days_since_1949", "system"])
    tb = tb.drop("publication_date", axis=1)
    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata)

    # Save changes in the new garden dataset.
    ds_garden.save()

    paths.log.info("epoch.end")


def fit_exponential(models, metric):
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
    reg = LinearRegression().fit(x, y)
    return reg.intercept_, reg.coef_[0]


def bootstrap(models, metric):
    rng = np.random.default_rng(42)

    oom_per_year = []

    for bootstrap_index in range(BOOTSTRAP_SAMPLE_SIZE):
        sample = models.sample(len(models), replace=True, random_state=rng)
        sample = sample.sort_values("frac_year")

        fit = fit_exponential(sample, metric)
        oom_per_year.append(fit[1])

    low, high = np.percentile(oom_per_year, [50 - BOOTSTRAP_CI_WIDTH / 2, 50 + BOOTSTRAP_CI_WIDTH / 2])
    return low, high


def run_regression(tb):
    publication_dates = tb["publication_date"]
    tb.loc[:, "frac_year"] = (
        publication_dates.dt.year + (publication_dates.dt.month - 1) / 12 + (publication_dates.dt.day - 1) / 365
    )
    tb = tb.sort_values(by="frac_year")

    metrics = ["training_computation_petaflop", "parameters", "training_dataset_size__datapoints"]
    # metrics = ["training_computation_petaflop"]

    for metric in metrics:
        # Filter out models without the metric information
        tb_metric = tb[pd.notnull(tb[metric])]

        pre_dl_models = tb_metric[tb_metric["frac_year"] < DL_ERA_START]
        pre_dl_fit = fit_exponential(pre_dl_models, metric)
        pre_dl_oom_per_year = pre_dl_fit[1]
        pre_dl_fit_ci = bootstrap(pre_dl_models, metric)

        dl_models = tb_metric[tb_metric["frac_year"] >= DL_ERA_START]
        dl_fit = fit_exponential(dl_models, metric)
        dl_oom_per_year = dl_fit[1]
        dl_fit_ci = bootstrap(dl_models, metric)

        print(
            f"Pre Deep Learning Era ({metric}): {10**pre_dl_oom_per_year:.1f}x/year ({10**pre_dl_fit_ci[0]:.1f} to {10**pre_dl_fit_ci[1]:.1f})"
        )
        print(
            f"Deep Learning Era ({metric}): {10**dl_oom_per_year:.1f}x/year ({10**dl_fit_ci[0]:.1f} to {10**dl_fit_ci[1]:.1f})"
        )

        pre_dl_year_grid = pre_dl_models["frac_year"]
        pre_dl_line = 10 ** (pre_dl_fit[0] + pre_dl_year_grid * pre_dl_fit[1])

        dl_year_grid = dl_models["frac_year"]
        dl_line = 10 ** (dl_fit[0] + dl_year_grid * dl_fit[1])

        # Add the lines back into the table as separate columns
        tb.loc[tb["frac_year"] < DL_ERA_START, f"pre_dl_line_{metric}"] = pre_dl_line.reindex(
            tb.index[tb["frac_year"] < DL_ERA_START]
        ).values
        tb.loc[tb["frac_year"] >= DL_ERA_START, f"dl_line_{metric}"] = dl_line.reindex(
            tb.index[tb["frac_year"] >= DL_ERA_START]
        ).values

    tb = tb.drop("frac_year", axis=1)

    return tb

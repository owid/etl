import datetime

import numpy as np
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    # Load table
    ds_meadow = paths.load_dataset()
    tb = ds_meadow["excess_mortality_economist"].reset_index()

    # Set type
    tb = tb.astype({"country": "string", "date": "datetime64[ns]"})

    # Harmonize country names
    tb = geo.harmonize_countries(
        tb,
        paths.country_mapping_path,
    )

    # Add last 12 months
    tb = add_last12m_values(tb)
    # Drop unused column
    tb = tb.drop(columns=["known_excess_deaths"])

    # Set index
    tb = tb.format(["country", "date"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the snapshot.
    ds_garden = create_dataset(dest_dir, tables=[tb], default_metadata=ds_meadow.metadata, formats=["csv", "feather"])

    # Save changes in the new garden dataset.
    ds_garden.save()


def add_last12m_values(tb: Table) -> Table:
    """Add last 12 month data."""
    columns = [
        "cumulative_estimated_daily_excess_deaths",
        "cumulative_estimated_daily_excess_deaths_ci_95_bot",
        "cumulative_estimated_daily_excess_deaths_ci_95_top",
    ]
    for col in columns:
        tb = add_last12m_to_metric(tb, col)
        tb = add_last12m_to_metric(tb, f"{col}_per_100k")
    return tb


def add_last12m_to_metric(tb: Table, column_metric: str) -> Table:
    """Add last 12 month data for an indicator."""
    column_metric_12m = f"{column_metric}_last12m"

    # Get only last 12 month of data
    date_cutoff = datetime.datetime.now() - datetime.timedelta(days=365.2425)

    # Get metric value 12 months ago
    tb_tmp = (
        tb.loc[tb["date"] > date_cutoff]
        .dropna(subset=[column_metric])
        .sort_values(["country", "date"])
        .drop_duplicates("country")[["country", column_metric]]
        .rename(columns={column_metric: column_metric_12m})
    )

    # Compute the difference, obtain last12m metric
    tb = tb.merge(tb_tmp, on=["country"], how="left")
    tb[column_metric_12m] = tb[column_metric] - tb[column_metric_12m]

    # Assign NaN to >1 year old data
    tb[column_metric_12m].loc[tb["date"] < date_cutoff] = np.nan

    return tb

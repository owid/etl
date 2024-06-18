"""Media attention to countries.

We have estimated various indicators.

- num_pages_tags: Number of pages tagged with a country name.
- relative_pages_tags: Share of pages tagged with a country name.
- relative_pages_tags_excluded: Share of pages tagged with a country name. It excludes COUNTRIES_EXCLUDED from share-estimation.
- num_pages_mentions: Number of pages mentioning a country name.
- relative_pages_mentions: Share of pages mentioning a country name.
- relative_pages_mentions_excluded: Share of pages tagged with a country name. It excludes COUNTRIES_EXCLUDED from share-estimation.
"""

import numpy as np

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Countries to exclude in some indicators
COUNTRIES_EXCLUDED = [
    "United States",
    "United Kingdom",
    "Australia",
]
# Index columns
COLUMN_INDEX = ["country", "year"]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("guardian_mentions")

    # Read table from meadow dataset.
    tb = ds_meadow["guardian_mentions"].reset_index()

    #
    # Process data.
    #
    ## Harmonize countries
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    ## Create indicators w
    ## Get relative values
    tb = add_relative_indicators(tb, ["num_pages_tags", "num_pages_mentions"])

    # Estimate 10-year average
    tb_10y_avg = tb[(tb["year"] >= 2014) & (tb["year"] <= 2023)].copy()
    columns_indicators = [col for col in tb_10y_avg.columns if col not in COLUMN_INDEX]
    tb_10y_avg = tb_10y_avg.groupby("country")[columns_indicators].mean().reset_index()
    tb_10y_avg["year"] = 2023

    # Estimate log(10-year average)
    tb_10y_avg_log = tb_10y_avg.copy()
    tb_10y_avg_log[columns_indicators] = np.log(tb_10y_avg[columns_indicators] + 1)
    tb_10y_avg_log["year"] = 2023

    ## Format
    tb = tb.format(COLUMN_INDEX)
    tb_10y_avg = tb_10y_avg.format(["country", "year"], short_name="avg_10y")
    tb_10y_avg_log = tb_10y_avg_log.format(["country", "year"], short_name="avg_log_10y")

    tables = [
        tb,
        tb_10y_avg,
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


def add_relative_indicators(tb, colnames):
    """Add relative indicators (including excluding versions)"""
    for colname in colnames:
        tb = add_relative_indicator(tb, colname)
    return tb


def add_relative_indicator(tb, colname):
    """Add relative indicator.

    E.g. Global share of ? for a given year. Note that we use 'per 100,000' factor.
    """
    tb_exc = tb[~tb["country"].isin(COUNTRIES_EXCLUDED)].copy()

    tb[f"{colname.replace('num_', 'relative_')}"] = get_relative_indicator(tb, colname)
    colname_excluded = f"{colname.replace('num_', 'relative_')}_excluded"
    tb_exc[colname_excluded] = get_relative_indicator(tb_exc, colname)

    # Combine
    tb = tb.merge(tb_exc[COLUMN_INDEX + [colname_excluded]], on=COLUMN_INDEX, how="left")

    return tb


def get_relative_indicator(tb, colname):
    """Add relative indicator.

    E.g. Global share of ? for a given year. Note that we use 'per 100,000' factor.
    """
    tb_ = tb.copy()

    tb_["total"] = tb_.groupby("year")[colname].transform(sum)
    return tb_[colname] / tb_["total"] * 100_000

    # tb[f"{colname.replace('num_', 'relative_')}"] = tb[colname] / tb["total"] * 100_000
    # tb = tb.drop(columns=["total"])
    # return tb

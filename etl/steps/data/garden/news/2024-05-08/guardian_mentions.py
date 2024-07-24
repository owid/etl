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
from owid.catalog import Table

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
# Years: Minimum and maximum of the 10-year average period.
YEAR_DEC_MIN = 2014
YEAR_DEC_MAX = 2023


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("guardian_mentions")
    ds_population = paths.load_dataset("population")

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

    ## Get relative values
    tb = add_relative_indicators(tb, ["num_pages_tags", "num_pages_mentions"])

    ## Add per-capita indicators
    tb = geo.add_population_to_table(tb, ds_population)
    for column in ["num_pages_tags", "num_pages_mentions"]:
        tb[f"{column}_per_million"] = tb[column] / tb["population"] * 1_000_000
    tb = tb.drop(columns="population")

    # Estimate 10-year average
    tb_10y_avg = tb[(tb["year"] >= YEAR_DEC_MIN) & (tb["year"] <= YEAR_DEC_MAX)].copy()
    tb_10y_avg = tb_10y_avg.rename(
        columns={col: f"{col}_10y_avg" for col in tb_10y_avg.columns if col not in COLUMN_INDEX}
    )
    columns_indicators = [col for col in tb_10y_avg.columns if col not in COLUMN_INDEX]
    tb_10y_avg = tb_10y_avg.groupby("country")[columns_indicators].mean().reset_index()
    tb_10y_avg["year"] = YEAR_DEC_MAX

    # Estimate log(10-year average)
    tb_10y_avg_log = tb_10y_avg.copy()
    tb_10y_avg_log[columns_indicators] = np.log(tb_10y_avg[columns_indicators] + 1)
    tb_10y_avg_log = tb_10y_avg_log.rename(columns={col: f"{col}_10y_avg_log" for col in columns_indicators})
    tb_10y_avg_log["year"] = YEAR_DEC_MAX

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


def add_relative_indicators(tb: Table, colnames):
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

    tb_total = tb_[tb_["country"] == "Total"]
    tb_ = tb_.merge(tb_total[["year", colname]], on="year", suffixes=["", "_total"])

    return tb_[colname] / tb_[f"{colname}_total"] * 100_000

    # tb[f"{colname.replace('num_', 'relative_')}"] = tb[colname] / tb["total"] * 100_000
    # tb = tb.drop(columns=["total"])
    # return tb

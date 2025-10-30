"""Media attention to countries.

Indicators estimated:

- num_pages: Number of pages mentioning a country name.
- relative_pages: Share of pages mentioning a country name.
- relative_pages_excluded: Share of pages tagged with a country name. It excludes COUNTRIES_EXCLUDED from share-estimation.
"""

import numpy as np
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)
# Countries to exclude in some indicators
COUNTRIES_EXCLUDED = {
    "United States",
    "United Kingdom",
    "Australia",
}
# Index columns
COLUMN_INDEX = ["country", "year"]
# Years: Minimum and maximum of the 10-year average period.
YEAR_DEC_MAX = 2024
YEAR_DEC_MIN = YEAR_DEC_MAX - 9


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("guardian_mentions")
    ds_population = paths.load_dataset("population")
    ds_regions = paths.load_dataset("regions")

    # Read table from meadow dataset.
    tb = ds_meadow.read("guardian_mentions")

    #
    # Process data.
    #
    ## Harmonize countries
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )

    # Remove expected NaNs
    tb = check_and_filter_nans(tb)

    ## Get relative values
    tb = add_relative_indicators(tb, ["num_pages"])

    ## Add data for regions
    tb = geo.add_regions_to_table(
        tb=tb,
        ds_regions=ds_regions,
        regions=[
            "Europe",
            "Asia",
            "Africa",
            "North America",
            "South America",
            "Oceania",
            "North America (WB)",
            "Latin America and Caribbean (WB)",
            "Middle East, North Africa, Afghanistan and Pakistan (WB)",
            "Sub-Saharan Africa (WB)",
            "Europe and Central Asia (WB)",
            "South Asia (WB)",
            "East Asia and Pacific (WB)",
        ],
    )

    ## Add per-capita indicators
    tb = geo.add_population_to_table(tb, ds_population)
    tb["num_pages_per_million"] = tb["num_pages"] / tb["population"] * 1_000_000
    tb = tb.drop(columns="population")

    # Estimate 10-year average
    tb_10y_avg, tb_10y_avg_log = make_decadal_avg_table(tb)

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
    ds_garden = paths.create_dataset(
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
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
    # Add relative indicator
    colname_new = f"{colname.replace('num_', 'relative_')}"
    tb.loc[:, colname_new] = get_relative_indicator(tb, colname).tolist()
    tb[colname_new] = tb[colname_new].copy_metadata(tb[colname])

    # Add relative indicator excluding some countries
    tb_exc = tb.loc[~tb["country"].isin(COUNTRIES_EXCLUDED)].copy()
    colname_excluded = f"{colname.replace('num_', 'relative_')}_excluded"
    tb_exc[colname_excluded] = get_relative_indicator(tb_exc, colname).tolist()
    tb = tb.merge(tb_exc[COLUMN_INDEX + [colname_excluded]], on=COLUMN_INDEX, how="left")
    tb[colname_excluded] = tb[colname_excluded].copy_metadata(tb[colname])

    # Ensure no NA in columns but excluded
    cols = [col for col in tb.columns if col != colname_excluded]
    assert not tb[cols].isna().any().any(), f"NA values found in unexpected columns {cols}"
    countries_with_nas = set(tb.loc[tb[colname_excluded].isna(), "country"].unique())
    assert (
        countries_with_nas == COUNTRIES_EXCLUDED
    ), f"Unexpected countries with NA in {colname_excluded}: {countries_with_nas}"

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


def check_and_filter_nans(tb):
    nas_country = tb[tb["num_pages"].isna()].groupby("country").size().sort_values().to_dict()
    nas_country_expected = {
        "Aland Islands": 1,
        "Cocos Islands": 1,
        "Sao Tome and Principe": 1,
        "Niue": 1,
        "Tokelau": 2,
        "Wallis and Futuna": 4,
        "Heard Island and McDonald Islands": 6,
        "Saint Pierre and Miquelon": 6,
        "Bouvet Island": 12,
        "French Southern Territories": 12,
    }
    assert nas_country == nas_country_expected, f"Unexpected NaNs in countries: {nas_country}"

    return tb.dropna(subset=["num_pages"])


def make_decadal_avg_table(tb: Table):
    """Get table with 10-year average and log(10-year average) indicators."""
    tb_10y_avg = tb[(tb["year"] >= YEAR_DEC_MIN) & (tb["year"] <= YEAR_DEC_MAX)].copy()
    tb_10y_avg = tb_10y_avg.rename(
        columns={col: f"{col}_10y_avg" for col in tb_10y_avg.columns if col not in COLUMN_INDEX}
    )
    columns_indicators = [col for col in tb_10y_avg.columns if col not in COLUMN_INDEX]
    tb_10y_avg = tb_10y_avg.groupby("country", observed=False)[columns_indicators].mean().reset_index()
    tb_10y_avg["year"] = YEAR_DEC_MAX

    # Copy metadata
    col_og = [col for col in tb.columns if col not in COLUMN_INDEX][0]
    for col in [col for col in tb_10y_avg.columns if col not in COLUMN_INDEX]:
        tb_10y_avg[col] = tb_10y_avg[col].copy_metadata(tb[col_og])

    # Estimate log(10-year average)
    tb_10y_avg_log = tb_10y_avg.copy()
    tb_10y_avg_log[columns_indicators] = np.log(tb_10y_avg[columns_indicators] + 1)
    tb_10y_avg_log = tb_10y_avg_log.rename(columns={col: f"{col}_10y_avg_log" for col in columns_indicators})
    tb_10y_avg_log["year"] = YEAR_DEC_MAX

    return tb_10y_avg, tb_10y_avg_log

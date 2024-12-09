"""Load a meadow dataset and create a garden dataset.

NOTES
This dataset relies on various sources. In this step, we consolidate them to have a single time-series. These are the distributions:

tb_counts:
source                              %
UN DYB                           71.823204
National Statistic Bureau        25.839354
Statistics Iceland                1.699958
US Census Bureau - all births     0.637484

tb_rates:
source                                                      %
UN DYB                                                65.786933
HFC                                                   15.803025
Insee, Etat civil                                      7.209527
National Statistic Bureau                              2.413904
Brouard, N. (1977) doi:10.2307/1531392                 2.413904
Statistics Iceland                                      2.09205
NSSEC - Institute for Social and Economic Research     1.834567
Lognard (2010)                                         1.802382
Statistical Yearbook of Belgium                        0.643708

"""

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

COLUMNS_INDEX = ["country", "year", "source", "type"]
COLUMNS_COUNTS_METRICS = {
    "mean_age_at_childbirth_based_demographic_rates": "mpac_fr",
    "mean_age_at_childbirth_as_arithmetic_mean": "mpac_mean",
}
COLUMNS_RATES_METRICS = {
    "meanage": "mpac_rates",
}

TYPES_EXPECTED = [
    "marital births",
    "all births",
]


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("paternal_ages")

    # Read table from meadow dataset.
    tb_counts = ds_meadow.read("counts")
    tb_rates = ds_meadow.read("rates")

    ## Keep relevant columns
    tb_counts = clean_table(
        tb_counts,
        COLUMNS_COUNTS_METRICS,
    )
    tb_rates = clean_table(
        tb_rates,
        COLUMNS_RATES_METRICS,
    )

    # Harmonize country names
    tb_counts = geo.harmonize_countries(
        df=tb_counts,
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
    )
    tb_rates = geo.harmonize_countries(
        df=tb_rates,
        countries_file=paths.country_mapping_path,
        warn_on_unused_countries=False,
    )

    # Remove spurious categories
    tb_counts = tb_counts.loc[tb_counts["type"].isin(TYPES_EXPECTED)]
    tb_rates = tb_rates.loc[tb_rates["type"].isin(TYPES_EXPECTED)]

    # Average different sources
    tb_counts = average_values_across_sources(tb_counts, ["mpac_fr", "mpac_mean"])
    tb_rates = average_values_across_sources(tb_rates, ["mpac_rates"])

    # Combine tables
    tb = merge_tables(tb_counts, tb_rates)

    # Process data.
    tables = [
        tb.format(["country", "year", "type"]),
    ]

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir,
        tables=tables,
        check_variables_metadata=True,
        default_metadata=ds_meadow.metadata,
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def clean_table(tb, columns_metrics_rename):
    # Drop column 'country' if exists
    if "country" in tb.columns:
        tb = tb.drop(columns="country")

    # Rename columns
    columns_rename = {"code": "country", **columns_metrics_rename}
    tb = tb.rename(columns=columns_rename)

    # Keep relevant columns
    columns_keep = COLUMNS_INDEX + list(columns_metrics_rename.values())
    tb = tb.loc[:, columns_keep]

    # Drop NaNs
    tb = tb.dropna(subset=columns_metrics_rename.values(), how="all")

    return tb


def average_values_across_sources(tb, columns):
    tb = tb.groupby(["country", "year", "type"], as_index=False).agg(
        {
            **{col: "mean" for col in columns},
            "source": set,
        }
    )
    return tb


def merge_tables(tb_counts, tb_rates):
    tb = tb_counts.merge(
        tb_rates,
        on=["country", "year", "type"],
        how="outer",
        suffixes=[
            "_counts",
            "_rates",
        ],
        short_name="paternal_age",
    )
    tb = tb[
        [col for col in COLUMNS_INDEX if col != "source"]
        + list(COLUMNS_COUNTS_METRICS.values())
        + list(COLUMNS_RATES_METRICS.values())
    ]
    return tb

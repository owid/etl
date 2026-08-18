"""Load a meadow dataset and create a garden dataset."""

import numpy as np
import structlog
from owid.catalog import Table

from etl.helpers import PathFinder

log = structlog.get_logger()

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

REGIONS = [
    "Africa (WHO)",
    "Americas (WHO)",
    "Eastern Mediterranean (WHO)",
    "Europe (WHO)",
    "South-East Asia (WHO)",
    "Western Pacific (WHO)",
    "World",
]


def run() -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("cholera_cases_and_deaths")
    ds_gho = paths.load_dataset("gho")

    # Read table from meadow dataset.
    tb = ds_meadow.read("cholera_cases_and_deaths")
    tb_gho = process_gho_cholera(ds_gho)

    # drop unnecessary columns
    tb = tb.drop(columns=["autochthonous_cases", "imported_cases", "who_region"])
    tb["cholera_case_fatality_rate"] = (tb["deaths"] / tb["total_cases"]) * 100

    tb = tb.rename(
        columns={
            "total_cases": "cholera_reported_cases",
            "deaths": "cholera_deaths",
        }
    )

    #
    # Process data.
    #
    # Harmonize country names.
    tb = paths.regions.harmonize_names(tb=tb, warn_on_unused_countries=False)
    tb_gho = paths.regions.harmonize_names(tb=tb_gho, warn_on_unused_countries=False)

    # Check whether overlapping years (2000-2016) have any missing or disagreeing values between the two series. Run when updating.
    # sanity_check_overlap_with_gho(tb, tb_gho)

    # Merge the two series, preferring GHO wherever it has data.
    tb = merge_with_gho(tb, tb_gho)

    # calculate region totals:
    tb = paths.regions.add_aggregates(tb=tb, regions=REGIONS, min_num_values_per_year=2)

    # Improve table format.
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Initialize a new garden dataset.
    ds_garden = paths.create_dataset(tables=[tb], default_metadata=ds_meadow.metadata)

    # Save garden dataset.
    ds_garden.save()


def sanity_check_overlap_with_gho(tb: Table, tb_gho: Table) -> None:
    """The disease-outbreak and GHO series both cover 2000-2016. Warn (don't fail) about
    any (country, year) rows that are missing from one series, or whose values disagree,
    since GHO is only used to extend the series into earlier years and small source
    revisions between the two are expected.
    """
    value_cols = ["cholera_reported_cases", "cholera_deaths", "cholera_case_fatality_rate"]
    overlapping_years = set(tb["year"]) & set(tb_gho["year"])
    if not overlapping_years:
        return

    cols = ["country", "year"] + value_cols
    merged = tb.loc[tb["year"].isin(overlapping_years), cols].merge(
        tb_gho.loc[tb_gho["year"].isin(overlapping_years), cols],
        on=["country", "year"],
        how="outer",
        suffixes=("", "_gho"),
        indicator=True,
    )

    unmatched = merged[merged["_merge"] != "both"]
    if not unmatched.empty:
        log.warning(
            f"cholera_cases_and_deaths: {len(unmatched)} (country, year) rows in 2000-2016 appear in only "
            f"one of the two cholera series:\n{unmatched[['country', 'year', '_merge']].to_string(index=False)}"
        )

    matched = merged[merged["_merge"] == "both"]
    for col in value_cols:
        mismatch = ~np.isclose(matched[col], matched[f"{col}_gho"], equal_nan=True)
        if mismatch.any():
            log.warning(
                f"cholera_cases_and_deaths: {mismatch.sum()} disagreements between the two cholera series for "
                f"'{col}' in overlapping years:\n"
                f"{matched.loc[mismatch, ['country', 'year', col, f'{col}_gho']].to_string(index=False)}"
            )


def merge_with_gho(tb: Table, tb_gho: Table) -> Table:
    """Merge the outbreak-based cholera series with the GHO series into a single (country, year) series.

    Post 2016: All data comes from cholera dashboard
    Pre 2000: All data comes from GHO
    2000-2016: Both sources have data, where they disagree GHO takes priority. The outbreak series is only used to fill in (country, year) pairs GHO doesn't cover.

    cholera_case_fatality_rate is recomputed from the merged case/death counts, so we don't end up mixing the two sources for that column.
    """
    count_cols = ["cholera_reported_cases", "cholera_deaths"]

    tb_indexed = tb.set_index(["country", "year"])[count_cols]
    tb_gho_indexed = tb_gho.set_index(["country", "year"])[count_cols]

    tb_merged = tb_indexed.join(tb_gho_indexed, how="outer", lsuffix="", rsuffix="_gho")
    for col in count_cols:
        tb_merged[col] = tb_merged[col].astype(float)
        mask = tb_merged[f"{col}_gho"].notna()
        tb_merged.loc[mask, col] = tb_merged.loc[mask, f"{col}_gho"]
    tb_merged = tb_merged.drop(columns=[f"{col}_gho" for col in count_cols])

    tb_merged["cholera_case_fatality_rate"] = (tb_merged["cholera_deaths"] / tb_merged["cholera_reported_cases"]) * 100

    return tb_merged.reset_index()


def process_gho_cholera(who_gh_dataset):
    tb_names = [
        "cholera_case_fatality_rate",
        "number_of_reported_cases_of_cholera",
        "number_of_reported_deaths_from_cholera",
    ]
    cholera_bp = who_gh_dataset[tb_names[0]]
    for tb_name in tb_names[1:]:
        cholera_bp = cholera_bp.join(who_gh_dataset[tb_name].drop(columns=["comments"]), how="outer")

    tb = (
        cholera_bp.loc[
            :,
            [
                "cholera_case_fatality_rate",
                "number_of_reported_cases_of_cholera",
                "number_of_reported_deaths_from_cholera",
            ],
        ]
        .rename(
            columns={
                "cholera_case_fatality_rate": "cholera_case_fatality_rate",
                "number_of_reported_cases_of_cholera": "cholera_reported_cases",
                "number_of_reported_deaths_from_cholera": "cholera_deaths",
            }
        )
        .dropna(how="all", axis=0)
        .astype(float)
    )
    tb = tb.reset_index()
    return tb

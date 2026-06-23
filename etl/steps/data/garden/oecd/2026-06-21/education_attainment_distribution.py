"""Combine multiple education sources into long-run series.

Produces two indicators:
1. share_tertiary_education: OECD observed data spliced with Lee & Lee historical estimates.
2. average_years_of_schooling: UNDP HDR data (1990+) spliced with Lee & Lee historical estimates.

For both, the more recent/reliable source is used as far back as it goes per country,
and Lee & Lee fills in the earlier period.
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Lee & Lee columns used for the splice.
LEE_LEE_TERTIARY_COL = "mf_adults__25_64_years__percentage_of_tertiary_education"
LEE_LEE_AVG_YEARS_COL = "mf_youth_and_adults__15_64_years__average_years_of_education"


def splice_with_lee_lee(tb_recent: Table, tb_lee_lee: Table, col: str) -> Table:
    """Splice a recent source with Lee & Lee historical data.

    For each country, uses the recent source from its earliest available year.
    Lee & Lee fills in the years before that. Countries not in the recent source
    keep the full Lee & Lee series.
    """
    first_year = tb_recent.groupby("country")["year"].min().to_dict()

    mask = tb_lee_lee.apply(
        lambda row: row["country"] not in first_year or row["year"] < first_year[row["country"]],
        axis=1,
    )
    tb_ll_filtered = tb_lee_lee[mask]

    return pr.concat([tb_ll_filtered, tb_recent], short_name=col)


def sanity_check_outputs(tb: Table) -> None:
    tb_check = tb.reset_index()
    assert not tb_check.empty, "Output table is empty."
    assert not tb_check.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows in output."


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("education_attainment_distribution")
    tb_oecd = ds_meadow["education_attainment_distribution"].reset_index()

    ds_lee_lee = paths.load_dataset("education_lee_lee")
    tb_lee_lee = ds_lee_lee["education_lee_lee"].reset_index()

    ds_undp = paths.load_dataset("undp_hdr")
    tb_undp = ds_undp["undp_hdr_sex"].reset_index()

    #
    # Process data.
    #

    # Harmonize OECD country names.
    tb_oecd = paths.regions.harmonize_names(tb_oecd, country_col="country", countries_file=paths.country_mapping_path)

    # --- 1. Share with tertiary education (OECD + Lee & Lee) ---
    tb_ll_tertiary = tb_lee_lee[["country", "year", LEE_LEE_TERTIARY_COL]].copy()
    tb_ll_tertiary = tb_ll_tertiary.rename(columns={LEE_LEE_TERTIARY_COL: "share_tertiary_education"})
    tb_ll_tertiary = tb_ll_tertiary.dropna(subset=["share_tertiary_education"])

    tb_tertiary = splice_with_lee_lee(tb_oecd, tb_ll_tertiary, "share_tertiary_education")

    # --- 2. Average years of schooling (UNDP + Lee & Lee) ---
    # UNDP: filter for sex=total, keep mys column.
    tb_undp_mys = tb_undp.loc[tb_undp["sex"] == "total", ["country", "year", "mys"]].copy()
    tb_undp_mys = tb_undp_mys.rename(columns={"mys": "average_years_of_schooling"})
    tb_undp_mys = tb_undp_mys.dropna(subset=["average_years_of_schooling"])

    # Lee & Lee: extract average years of education.
    tb_ll_avg = tb_lee_lee[["country", "year", LEE_LEE_AVG_YEARS_COL]].copy()
    tb_ll_avg = tb_ll_avg.rename(columns={LEE_LEE_AVG_YEARS_COL: "average_years_of_schooling"})
    tb_ll_avg = tb_ll_avg.dropna(subset=["average_years_of_schooling"])

    tb_avg_years = splice_with_lee_lee(tb_undp_mys, tb_ll_avg, "average_years_of_schooling")

    # --- Combine both indicators ---
    tb = pr.merge(tb_tertiary, tb_avg_years, on=["country", "year"], how="outer")

    tb = tb.format(["country", "year"], short_name=paths.short_name)

    sanity_check_outputs(tb)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=[tb])
    ds_garden.save()

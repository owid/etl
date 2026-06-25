"""Load OECD educational attainment data and splice with Lee & Lee historical estimates.

For the combined indicator:
- Lee & Lee historical estimates up to 2005 (we drop post-2005 Lee & Lee values
  because they become increasingly unreliable).
- OECD observed data from its earliest available year per country.
- Where both overlap, OECD takes priority.
- For countries without OECD data, the series ends at 2005.

Also produces:
- An OECD-only indicator (no splice).
- A Wittgenstein Centre post_secondary indicator (25-64, SSP2) for comparison.
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# The Lee & Lee column that corresponds to the OECD tertiary education share (25-64, both sexes).
LEE_LEE_TERTIARY_COL = "mf_adults__25_64_years__percentage_of_tertiary_education"

# Lee & Lee data goes up to 2010 (the last year of historical estimates).
LEE_LEE_MAX_YEAR = 2010

# Wittgenstein Centre age bins.
WC_AGE_BINS_25_64 = ["25-29", "30-34", "35-39", "40-44", "45-49", "50-54", "55-59", "60-64"]
WC_AGE_BINS_15_64 = ["15-19", "20-24"] + WC_AGE_BINS_25_64


def sanity_check_inputs(tb_oecd: Table, tb_lee_lee: Table) -> None:
    assert not tb_oecd.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows in OECD data."
    assert tb_oecd["share_tertiary_education"].min() >= 0, "Negative share found in OECD data."
    assert tb_oecd["share_tertiary_education"].max() <= 100, "OECD share exceeds 100%."
    assert LEE_LEE_TERTIARY_COL in tb_lee_lee.columns, f"Missing column {LEE_LEE_TERTIARY_COL} in Lee & Lee data."


def sanity_check_outputs(tb: Table) -> None:
    tb_check = tb.reset_index()
    assert not tb_check.empty, "Output table is empty."
    assert not tb_check.duplicated(subset=["country", "year"]).any(), "Duplicate (country, year) rows in output."
    assert tb_check["share_tertiary_education"].min() >= 0, "Negative share in output."
    assert tb_check["share_tertiary_education"].max() <= 100, "Output share exceeds 100%."
    assert len(tb_check) > 1500, f"Unexpectedly few rows in output: {len(tb_check)}."


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("education_attainment_distribution")
    tb_oecd = ds_meadow["education_attainment_distribution"].reset_index()

    ds_lee_lee = paths.load_dataset("education_lee_lee")
    tb_lee_lee = ds_lee_lee["education_lee_lee"].reset_index()

    sanity_check_inputs(tb_oecd, tb_lee_lee)

    #
    # Process data.
    #

    # Harmonize OECD country names.
    tb_oecd = paths.regions.harmonize_names(tb_oecd, country_col="country", countries_file=paths.country_mapping_path)

    # Extract the tertiary education share from Lee & Lee.
    tb_ll = tb_lee_lee[["country", "year", LEE_LEE_TERTIARY_COL]].copy()
    tb_ll = tb_ll.rename(columns={LEE_LEE_TERTIARY_COL: "share_tertiary_education"})
    tb_ll = tb_ll.dropna(subset=["share_tertiary_education"])

    # Drop Lee & Lee data after LEE_LEE_MAX_YEAR.
    tb_ll = tb_ll[tb_ll["year"] <= LEE_LEE_MAX_YEAR]

    # For each country, find the earliest year with OECD data.
    oecd_first_year = tb_oecd.groupby("country")["year"].min().to_dict()

    # Keep Lee & Lee rows only for years before the earliest OECD data point for that country.
    # For countries not in OECD at all, keep the full Lee & Lee series (up to LEE_LEE_MAX_YEAR).
    mask = tb_ll.apply(
        lambda row: row["country"] not in oecd_first_year or row["year"] < oecd_first_year[row["country"]],
        axis=1,
    )
    tb_ll = tb_ll[mask]

    # Combine: Lee & Lee historical + OECD observed.
    tb = pr.concat([tb_ll, tb_oecd], short_name=paths.short_name)
    tb = tb.format(["country", "year"], short_name=paths.short_name)

    sanity_check_outputs(tb)

    # OECD-only table (no Lee & Lee, no splice).
    tb_oecd_only = tb_oecd.copy()
    tb_oecd_only = tb_oecd_only.format(["country", "year"], short_name="education_attainment_distribution_oecd")

    # Wittgenstein Centre tables (SSP2).
    tb_wc_tertiary = make_wc_share(
        "post_secondary", "share_tertiary_education", "education_attainment_distribution_wc", WC_AGE_BINS_25_64
    )
    tb_wc_no_edu = make_wc_share(
        "no_education", "share_no_formal_education", "education_no_formal_wc", WC_AGE_BINS_25_64
    )
    tb_wc_some_edu = make_wc_share(
        "some_education", "share_some_formal_education", "education_some_formal_wc", WC_AGE_BINS_25_64
    )
    tb_wc_no_edu_sex = make_wc_share_by_sex(
        "no_education", "share_no_formal_education", "education_no_formal_by_sex_wc", WC_AGE_BINS_25_64
    )

    # Combined Lee & Lee + Wittgenstein Centre splice for no formal / some formal education (15-64).
    tb_combined_formal = make_ll_wc_formal_education_splice(tb_lee_lee)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[tb, tb_oecd_only, tb_wc_tertiary, tb_wc_no_edu, tb_wc_some_edu, tb_wc_no_edu_sex, tb_combined_formal]
    )
    ds_garden.save()


def make_wc_share(education_cat: str, col_name: str, short_name: str, age_bins: list) -> Table:
    """Build an education share from Wittgenstein Centre age bins.

    For `post_secondary`: this is the aggregate tertiary category for all years.
    (From 2015, bachelor/master/short_post_secondary appear as subcategories
    but post_secondary remains the total.)

    For `no_education`: share of adults with no formal education.
    For `some_education`: share of adults with some formal education.
    """
    ds_wc = paths.load_dataset("wittgenstein_human_capital")
    tb_wc = ds_wc["by_sex_age_edu"].reset_index()

    # Filter: SSP2, both sexes, specified age bins.
    tb_wc = tb_wc[(tb_wc["scenario"] == 2) & (tb_wc["sex"] == "total") & (tb_wc["age"].isin(age_bins))]

    cat_pop = (
        tb_wc[tb_wc["education"] == education_cat]
        .groupby(["country", "year"], observed=True)["pop"]
        .sum()
        .reset_index()
        .rename(columns={"pop": "cat_pop"})
    )

    total_pop = (
        tb_wc[tb_wc["education"] == "total"]
        .groupby(["country", "year"], observed=True)["pop"]
        .sum()
        .reset_index()
        .rename(columns={"pop": "total_pop"})
    )

    tb = pr.merge(cat_pop, total_pop, on=["country", "year"])
    tb[col_name] = (tb["cat_pop"] / tb["total_pop"]) * 100
    tb = tb.drop(columns=["cat_pop", "total_pop"])

    tb = tb.format(["country", "year"], short_name=short_name)

    return tb


def make_wc_share_by_sex(education_cat: str, col_name: str, short_name: str, age_bins: list) -> Table:
    """Build an education share by sex from Wittgenstein Centre age bins."""
    ds_wc = paths.load_dataset("wittgenstein_human_capital")
    tb_wc = ds_wc["by_sex_age_edu"].reset_index()

    # Filter: SSP2, specified age bins, female + male.
    tb_wc = tb_wc[(tb_wc["scenario"] == 2) & (tb_wc["sex"].isin(["female", "male"])) & (tb_wc["age"].isin(age_bins))]

    cat_pop = (
        tb_wc[tb_wc["education"] == education_cat]
        .groupby(["country", "year", "sex"], observed=True)["pop"]
        .sum()
        .reset_index()
        .rename(columns={"pop": "cat_pop"})
    )

    total_pop = (
        tb_wc[tb_wc["education"] == "total"]
        .groupby(["country", "year", "sex"], observed=True)["pop"]
        .sum()
        .reset_index()
        .rename(columns={"pop": "total_pop"})
    )

    tb = pr.merge(cat_pop, total_pop, on=["country", "year", "sex"])
    tb[col_name] = (tb["cat_pop"] / tb["total_pop"]) * 100
    tb = tb.drop(columns=["cat_pop", "total_pop"])

    # Rename sex values for display.
    tb["sex"] = tb["sex"].map({"female": "Women", "male": "Men"}).astype("category")

    tb = tb.format(["country", "year", "sex"], short_name=short_name)

    return tb


LEE_LEE_NO_EDU_COL = "mf_youth_and_adults__15_64_years__percentage_of_no_education"


def make_ll_wc_formal_education_splice(tb_lee_lee) -> Table:
    """Splice Lee & Lee (1870-2010) with Wittgenstein Centre (post-2010) for no/some formal education, 15-64.

    Lee & Lee is used up to its last year per country. Wittgenstein Centre fills in from the next available year.
    """
    # Lee & Lee: no education, both sexes, 15-64.
    tb_ll = tb_lee_lee[["country", "year", LEE_LEE_NO_EDU_COL]].copy()
    tb_ll = tb_ll.rename(columns={LEE_LEE_NO_EDU_COL: "share_no_formal_education"})
    tb_ll = tb_ll.dropna(subset=["share_no_formal_education"])
    tb_ll = tb_ll[tb_ll["year"] <= LEE_LEE_MAX_YEAR]

    # Wittgenstein Centre: no education, 15-64 from age bins.
    ds_wc = paths.load_dataset("wittgenstein_human_capital")
    tb_wc = ds_wc["by_sex_age_edu"].reset_index()
    tb_wc = tb_wc[(tb_wc["scenario"] == 2) & (tb_wc["sex"] == "total") & (tb_wc["age"].isin(WC_AGE_BINS_15_64))]

    no_pop = (
        tb_wc[tb_wc["education"] == "no_education"]
        .groupby(["country", "year"], observed=True)["pop"]
        .sum()
        .reset_index()
        .rename(columns={"pop": "no_pop"})
    )
    tot_pop = (
        tb_wc[tb_wc["education"] == "total"]
        .groupby(["country", "year"], observed=True)["pop"]
        .sum()
        .reset_index()
        .rename(columns={"pop": "tot_pop"})
    )
    tb_wc_no = pr.merge(no_pop, tot_pop, on=["country", "year"])
    tb_wc_no["share_no_formal_education"] = (tb_wc_no["no_pop"] / tb_wc_no["tot_pop"]) * 100
    tb_wc_no = tb_wc_no.drop(columns=["no_pop", "tot_pop"])

    # For each country, find the last year with Lee & Lee data.
    ll_last_year = tb_ll.groupby("country")["year"].max().to_dict()

    # Keep WC rows only for years after the last Lee & Lee data point.
    mask = tb_wc_no.apply(
        lambda row: row["country"] not in ll_last_year or row["year"] > ll_last_year[row["country"]],
        axis=1,
    )
    tb_wc_no = tb_wc_no[mask]

    # Combine.
    tb = pr.concat([tb_ll, tb_wc_no], short_name="education_formal_combined")

    # Add some formal education = 100 - no formal.
    tb["share_some_formal_education"] = 100 - tb["share_no_formal_education"]

    tb = tb.format(["country", "year"], short_name="education_formal_combined")

    return tb

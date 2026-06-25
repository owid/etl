"""Load OECD educational attainment data and splice with Lee & Lee historical estimates.

The OECD data now includes multiple education levels (less than primary, below upper secondary,
upper secondary + post-secondary non-tertiary, tertiary) and sex breakdowns (total, female, male).

Combined indicators splice Lee & Lee historical estimates with OECD observed data:
- Tertiary education (25-64): Lee & Lee + OECD tertiary
- No formal education (25-64): Lee & Lee + OECD less-than-primary

Also produces:
- OECD-only indicators (all education levels, by sex).
- Wittgenstein Centre indicators for comparison.
- Lee & Lee + Wittgenstein Centre splice for no/some formal education (15-64).
"""

import owid.catalog.processing as pr
from owid.catalog import Table

from etl.helpers import PathFinder

paths = PathFinder(__file__)

# Lee & Lee columns.
LEE_LEE_TERTIARY_COL = "mf_adults__25_64_years__percentage_of_tertiary_education"
LEE_LEE_NO_EDU_COL = "mf_youth_and_adults__15_64_years__percentage_of_no_education"

# Lee & Lee data goes up to 2010 (the last year of historical estimates).
LEE_LEE_MAX_YEAR = 2010

# Wittgenstein Centre age bins.
WC_AGE_BINS_25_64 = ["25-29", "30-34", "35-39", "40-44", "45-49", "50-54", "55-59", "60-64"]
WC_AGE_BINS_15_64 = ["15-19", "20-24"] + WC_AGE_BINS_25_64


def run() -> None:
    #
    # Load inputs.
    #
    ds_meadow = paths.load_dataset("education_attainment_distribution")
    tb_oecd_all = ds_meadow["education_attainment_distribution"].reset_index()

    ds_lee_lee = paths.load_dataset("education_lee_lee")
    tb_lee_lee = ds_lee_lee["education_lee_lee"].reset_index()

    #
    # Process data.
    #

    # Harmonize OECD country names.
    tb_oecd_all = paths.regions.harmonize_names(
        tb_oecd_all, country_col="country", countries_file=paths.country_mapping_path
    )

    # Split OECD into total and by-sex.
    tb_oecd_total = tb_oecd_all[tb_oecd_all["sex"] == "total"].drop(columns=["sex"]).reset_index(drop=True)
    tb_oecd_by_sex = tb_oecd_all[tb_oecd_all["sex"] != "total"].reset_index(drop=True)

    # --- Combined tertiary: Lee & Lee + OECD (25-64, total) ---
    tb_tertiary_combined = make_ll_oecd_splice(
        tb_oecd_total,
        tb_lee_lee,
        oecd_col="share_tertiary",
        ll_col=LEE_LEE_TERTIARY_COL,
        output_col="share_tertiary_education",
        short_name=paths.short_name,
    )

    # --- Combined no formal education: Lee & Lee + OECD less-than-primary (total) ---
    # OECD "less than primary" = no formal education equivalent.
    tb_no_formal_combined = make_ll_oecd_splice(
        tb_oecd_total,
        tb_lee_lee,
        oecd_col="share_less_than_primary",
        ll_col=LEE_LEE_NO_EDU_COL,
        output_col="share_no_formal_education",
        short_name="education_no_formal_combined",
    )
    # Add some formal = 100 - no formal.
    tb_no_formal_combined_check = tb_no_formal_combined.reset_index()
    tb_no_formal_combined_check["share_some_formal_education"] = (
        100 - tb_no_formal_combined_check["share_no_formal_education"]
    )
    tb_no_formal_combined = tb_no_formal_combined_check.format(
        ["country", "year"], short_name="education_no_formal_combined"
    )

    # --- OECD-only: all education levels, total (no splice) ---
    tb_oecd_only = tb_oecd_total.copy()
    tb_oecd_only = tb_oecd_only.format(["country", "year"], short_name="education_attainment_distribution_oecd")

    # --- OECD by sex ---
    tb_oecd_by_sex["sex"] = tb_oecd_by_sex["sex"].map({"female": "Women", "male": "Men"}).astype("category")
    tb_oecd_sex = tb_oecd_by_sex.format(
        ["country", "year", "sex"], short_name="education_attainment_distribution_oecd_sex"
    )

    # --- Wittgenstein Centre tables (SSP2) ---
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

    # --- Lee & Lee + Wittgenstein Centre splice for no/some formal education (15-64) ---
    tb_ll_wc_formal = make_ll_wc_formal_education_splice(tb_lee_lee)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(
        tables=[
            tb_tertiary_combined,
            tb_no_formal_combined,
            tb_oecd_only,
            tb_oecd_sex,
            tb_wc_tertiary,
            tb_wc_no_edu,
            tb_wc_some_edu,
            tb_wc_no_edu_sex,
            tb_ll_wc_formal,
        ]
    )
    ds_garden.save()


def make_ll_oecd_splice(
    tb_oecd: Table,
    tb_lee_lee: Table,
    oecd_col: str,
    ll_col: str,
    output_col: str,
    short_name: str,
) -> Table:
    """Splice Lee & Lee historical estimates with OECD observed data for a single indicator."""
    # OECD side: keep only the relevant column.
    tb_o = tb_oecd[["country", "year", oecd_col]].copy()
    tb_o = tb_o.rename(columns={oecd_col: output_col})
    tb_o = tb_o.dropna(subset=[output_col])

    # Lee & Lee side.
    tb_ll = tb_lee_lee[["country", "year", ll_col]].copy()
    tb_ll = tb_ll.rename(columns={ll_col: output_col})
    tb_ll = tb_ll.dropna(subset=[output_col])
    tb_ll = tb_ll[tb_ll["year"] <= LEE_LEE_MAX_YEAR]

    # For each country, find the earliest year with OECD data.
    oecd_first_year = tb_o.groupby("country")["year"].min().to_dict()

    # Keep Lee & Lee rows only for years before the earliest OECD data point.
    mask = tb_ll.apply(
        lambda row: row["country"] not in oecd_first_year or row["year"] < oecd_first_year[row["country"]],
        axis=1,
    )
    tb_ll = tb_ll[mask]

    # Combine.
    tb = pr.concat([tb_ll, tb_o], short_name=short_name)
    tb = tb.format(["country", "year"], short_name=short_name)

    return tb


def make_wc_share(education_cat: str, col_name: str, short_name: str, age_bins: list) -> Table:
    """Build an education share from Wittgenstein Centre age bins."""
    ds_wc = paths.load_dataset("wittgenstein_human_capital")
    tb_wc = ds_wc["by_sex_age_edu"].reset_index()

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

    tb["sex"] = tb["sex"].map({"female": "Women", "male": "Men"}).astype("category")

    tb = tb.format(["country", "year", "sex"], short_name=short_name)

    return tb


def make_ll_wc_formal_education_splice(tb_lee_lee) -> Table:
    """Splice Lee & Lee (1870-2010) with Wittgenstein Centre (post-2010) for no/some formal education, 15-64."""
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

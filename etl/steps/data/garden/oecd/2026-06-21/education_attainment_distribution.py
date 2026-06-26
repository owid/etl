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
from owid.catalog import Dataset, Table
from structlog import get_logger

from etl.helpers import PathFinder

log = get_logger()

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
    tb_oecd_all = ds_meadow.read("education_attainment_distribution")

    ds_lee_lee = paths.load_dataset("education_lee_lee")
    tb_lee_lee = ds_lee_lee.read("education_lee_lee")

    ds_wc = paths.load_dataset("wittgenstein_human_capital")

    #
    # Process data.
    #

    # Harmonize OECD country names.
    tb_oecd_all = paths.regions.harmonize_names(tb_oecd_all)

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
    tb_no_formal_combined = tb_no_formal_combined.reset_index()
    tb_no_formal_combined["share_some_formal_education"] = 100 - tb_no_formal_combined["share_no_formal_education"]
    tb_no_formal_combined = tb_no_formal_combined.format(["country", "year"], short_name="education_no_formal_combined")

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
        ds_wc, "post_secondary", "share_tertiary_education", "education_attainment_distribution_wc", WC_AGE_BINS_25_64
    )
    tb_wc_no_edu = make_wc_share(
        ds_wc, "no_education", "share_no_formal_education", "education_no_formal_wc", WC_AGE_BINS_25_64
    )
    tb_wc_some_edu = make_wc_share(
        ds_wc, "some_education", "share_some_formal_education", "education_some_formal_wc", WC_AGE_BINS_25_64
    )
    tb_wc_no_edu_sex = make_wc_share_by_sex(
        ds_wc, "no_education", "share_no_formal_education", "education_no_formal_by_sex_wc", WC_AGE_BINS_25_64
    )

    # --- Lee & Lee + Wittgenstein Centre splice for no/some formal education (15-64) ---
    tb_ll_wc_formal = make_ll_wc_formal_education_splice(ds_wc, tb_lee_lee)

    # --- Three-source combined: Lee & Lee + OECD + WC for no formal education ---
    tb_no_formal_three = make_three_source_no_formal_splice(ds_wc, tb_oecd_total, tb_lee_lee)

    #
    # Sanity checks.
    #
    all_tables = [
        tb_tertiary_combined,
        tb_no_formal_combined,
        tb_oecd_only,
        tb_oecd_sex,
        tb_wc_tertiary,
        tb_wc_no_edu,
        tb_wc_some_edu,
        tb_wc_no_edu_sex,
        tb_ll_wc_formal,
        tb_no_formal_three,
    ]
    sanity_check_outputs(all_tables)

    #
    # Save outputs.
    #
    ds_garden = paths.create_dataset(tables=all_tables)
    ds_garden.save()


def _filter_before_first_year(tb: Table, tb_ref: Table, country_col: str = "country") -> Table:
    """Keep rows from `tb` only for years before the earliest year in `tb_ref`, per country."""
    first_year = tb_ref.groupby(country_col)["year"].min().rename("_first_year")
    tb = tb.join(first_year, on=country_col)
    tb = tb[(tb["_first_year"].isna()) | (tb["year"] < tb["_first_year"])].drop(columns=["_first_year"])
    return tb


def _filter_after_last_year(tb: Table, tb_ref: Table, country_col: str = "country") -> Table:
    """Keep rows from `tb` only for years after the last year in `tb_ref`, per country."""
    last_year = tb_ref.groupby(country_col)["year"].max().rename("_last_year")
    tb = tb.join(last_year, on=country_col)
    tb = tb[(tb["_last_year"].isna()) | (tb["year"] > tb["_last_year"])].drop(columns=["_last_year"])
    return tb


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

    # Keep Lee & Lee rows only for years before the earliest OECD data point.
    tb_ll = _filter_before_first_year(tb_ll, tb_o)

    # Combine.
    tb = pr.concat([tb_ll, tb_o], short_name=short_name)
    tb = tb.format(["country", "year"], short_name=short_name)

    return tb


def _compute_wc_share(
    ds_wc: Dataset, education_cat: str, age_bins: list, sex_filter: str = "total"
) -> Table:
    """Compute education share from Wittgenstein Centre age bins for a given education category and sex filter."""
    tb_wc = ds_wc.read("by_sex_age_edu")

    if sex_filter == "total":
        tb_wc = tb_wc[(tb_wc["scenario"] == 2) & (tb_wc["sex"] == "total") & (tb_wc["age"].isin(age_bins))]
        group_cols = ["country", "year"]
    else:
        tb_wc = tb_wc[
            (tb_wc["scenario"] == 2) & (tb_wc["sex"].isin(["female", "male"])) & (tb_wc["age"].isin(age_bins))
        ]
        group_cols = ["country", "year", "sex"]

    cat_pop = (
        tb_wc[tb_wc["education"] == education_cat]
        .groupby(group_cols, observed=True)["pop"]
        .sum()
        .reset_index()
        .rename(columns={"pop": "cat_pop"})
    )

    total_pop = (
        tb_wc[tb_wc["education"] == "total"]
        .groupby(group_cols, observed=True)["pop"]
        .sum()
        .reset_index()
        .rename(columns={"pop": "total_pop"})
    )

    tb = pr.merge(cat_pop, total_pop, on=group_cols)
    tb["share"] = (tb["cat_pop"] / tb["total_pop"]) * 100
    tb = tb.drop(columns=["cat_pop", "total_pop"])

    return tb


def make_wc_share(
    ds_wc: Dataset, education_cat: str, col_name: str, short_name: str, age_bins: list
) -> Table:
    """Build an education share from Wittgenstein Centre age bins."""
    tb = _compute_wc_share(ds_wc, education_cat, age_bins, sex_filter="total")
    tb = tb.rename(columns={"share": col_name})
    tb = tb.format(["country", "year"], short_name=short_name)
    return tb


def make_wc_share_by_sex(
    ds_wc: Dataset, education_cat: str, col_name: str, short_name: str, age_bins: list
) -> Table:
    """Build an education share by sex from Wittgenstein Centre age bins."""
    tb = _compute_wc_share(ds_wc, education_cat, age_bins, sex_filter="by_sex")
    tb = tb.rename(columns={"share": col_name})
    tb["sex"] = tb["sex"].map({"female": "Women", "male": "Men"}).astype("category")
    tb = tb.format(["country", "year", "sex"], short_name=short_name)
    return tb


def make_ll_wc_formal_education_splice(ds_wc: Dataset, tb_lee_lee: Table) -> Table:
    """Splice Lee & Lee (1870-2010) with Wittgenstein Centre (post-2010) for no/some formal education, 15-64."""
    # Lee & Lee: no education, both sexes, 15-64.
    tb_ll = tb_lee_lee[["country", "year", LEE_LEE_NO_EDU_COL]].copy()
    tb_ll = tb_ll.rename(columns={LEE_LEE_NO_EDU_COL: "share_no_formal_education"})
    tb_ll = tb_ll.dropna(subset=["share_no_formal_education"])
    tb_ll = tb_ll[tb_ll["year"] <= LEE_LEE_MAX_YEAR]

    # Wittgenstein Centre: no education, 15-64 from age bins.
    tb_wc_no = _compute_wc_share(ds_wc, "no_education", WC_AGE_BINS_15_64, sex_filter="total")
    tb_wc_no = tb_wc_no.rename(columns={"share": "share_no_formal_education"})

    # Keep WC rows only for years after the last Lee & Lee data point.
    tb_wc_no = _filter_after_last_year(tb_wc_no, tb_ll)

    # Combine.
    tb = pr.concat([tb_ll, tb_wc_no], short_name="education_formal_combined")

    # Add some formal education = 100 - no formal.
    tb["share_some_formal_education"] = 100 - tb["share_no_formal_education"]

    tb = tb.format(["country", "year"], short_name="education_formal_combined")

    return tb


def make_three_source_no_formal_splice(ds_wc: Dataset, tb_oecd_total: Table, tb_lee_lee: Table) -> Table:
    """Three-source splice for no formal education: OECD > Lee & Lee > Wittgenstein Centre.

    Priority:
    1. OECD countries: Lee & Lee (pre-OECD) -> OECD less-than-primary (from earliest available year)
    2. Non-OECD countries with Lee & Lee: Lee & Lee up to 2010 -> WC from next available year
    3. WC-only countries: WC only
    """
    # --- OECD: less than primary ---
    tb_o = tb_oecd_total[["country", "year", "share_less_than_primary"]].copy()
    tb_o = tb_o.rename(columns={"share_less_than_primary": "share_no_formal_education"})
    tb_o = tb_o.dropna(subset=["share_no_formal_education"])
    oecd_countries = set(tb_o["country"].unique())

    # --- Lee & Lee: no education, 15-64 ---
    tb_ll = tb_lee_lee[["country", "year", LEE_LEE_NO_EDU_COL]].copy()
    tb_ll = tb_ll.rename(columns={LEE_LEE_NO_EDU_COL: "share_no_formal_education"})
    tb_ll = tb_ll.dropna(subset=["share_no_formal_education"])
    tb_ll = tb_ll[tb_ll["year"] <= LEE_LEE_MAX_YEAR]

    # For OECD countries: keep Lee & Lee only before OECD starts.
    # For non-OECD countries: keep all Lee & Lee.
    tb_ll_keep = _filter_before_first_year(tb_ll, tb_o)

    # --- Wittgenstein Centre: no education, 15-64 ---
    tb_wc_no = _compute_wc_share(ds_wc, "no_education", WC_AGE_BINS_15_64, sex_filter="total")
    tb_wc_no = tb_wc_no.rename(columns={"share": "share_no_formal_education"})

    # For OECD countries: WC not used (OECD takes over).
    # For non-OECD + Lee & Lee countries: WC only after Lee & Lee ends.
    # For WC-only countries (no OECD, no Lee & Lee): use all WC.
    tb_wc_no = tb_wc_no[~tb_wc_no["country"].isin(oecd_countries)]
    tb_wc_no = _filter_after_last_year(tb_wc_no, tb_ll_keep)

    # Combine all three.
    tb = pr.concat([tb_ll_keep, tb_o, tb_wc_no], short_name="education_no_formal_three_sources")

    # Add some formal education = 100 - no formal.
    tb["share_some_formal_education"] = 100 - tb["share_no_formal_education"]

    tb = tb.format(["country", "year"], short_name="education_no_formal_three_sources")

    return tb


def sanity_check_outputs(tables: list[Table]) -> None:
    """Check all output tables for common data integrity issues."""
    for tb in tables:
        tb_flat = tb.reset_index()
        name = tb.metadata.short_name

        # No fully-NaN columns.
        nan_cols = tb_flat.columns[tb_flat.isna().all()].tolist()
        assert not nan_cols, f"[{name}] Fully-NaN columns: {nan_cols}"

        # No duplicate key rows.
        index_cols = [c for c in ["country", "year", "sex"] if c in tb_flat.columns]
        assert not tb_flat.duplicated(subset=index_cols).any(), f"[{name}] Duplicate rows on {index_cols}"

        # Share columns should be in [0, 100].
        share_cols = [c for c in tb_flat.columns if c.startswith("share_")]
        for col in share_cols:
            vals = tb_flat[col].dropna()
            if len(vals) == 0:
                continue
            assert vals.min() >= 0, f"[{name}] {col} has negative values (min={vals.min()})"
            assert vals.max() <= 100, f"[{name}] {col} exceeds 100 (max={vals.max()})"

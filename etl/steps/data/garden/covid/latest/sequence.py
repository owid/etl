"""Load a meadow dataset and create a garden dataset."""

from datetime import timedelta

import owid.catalog.processing as pr
import pandas as pd
from owid.catalog import Dataset, Table
from shared import add_population_2022

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Variables
## CoVariants -> OWID name mapping. If relevant=False, variant is placed in bucket "non_who", along with "others"
CATEGORY_OTHERS = "Others"
CATEGORY_NON_RELEVANT = "Non-relevant"
CATEGORY_O_NON_RELEVANT = "Omicron (others)"
# 'rename' field can be found in https://github.com/hodcroftlab/covariants/blob/master/scripts/clusters.py (field 'alt_display_name')
VARIANTS_DETAILS = {
    "20A.EU2": {"rename": "B.1.160", "relevant": False},
    "20A/S:439K": {"rename": "B.1.258", "relevant": False},
    "20A/S:98F": {"rename": "B.1.221", "relevant": False},
    "20B/S:1122L": {"rename": "B.1.1.302", "relevant": False},
    "20A/S:126A": {"rename": "B.1.620", "relevant": False},
    "20B/S:626S": {"rename": "B.1.1.277", "relevant": False},
    "20B/S:732A": {"rename": "B.1.1.519", "relevant": False},
    "20C/S:80Y": {"rename": "B.1.367", "relevant": False},
    "20E (EU1)": {"rename": "B.1.177", "relevant": False},
    "20H (Beta, V2)": {"rename": "Beta", "relevant": True},
    "20I (Alpha, V1)": {"rename": "Alpha", "relevant": True},
    "20J (Gamma, V3)": {"rename": "Gamma", "relevant": True},
    "21A (Delta)": {"rename": "Delta", "relevant": True},
    "21B (Kappa)": {"rename": "Kappa", "relevant": False},
    "21C (Epsilon)": {"rename": "Epsilon", "relevant": False},
    "21D (Eta)": {"rename": "Eta", "relevant": False},
    "21F (Iota)": {"rename": "Iota", "relevant": False},
    "21G (Lambda)": {"rename": "Lambda", "relevant": True},
    "21H (Mu)": {"rename": "Mu", "relevant": True},
    "21I (Delta)": {"rename": "Delta", "relevant": True},
    "21J (Delta)": {"rename": "Delta", "relevant": True},
    "21K (Omicron)": {"rename": "Omicron (BA.1)", "relevant": True},
    "21L (Omicron)": {"rename": "Omicron (BA.2)", "relevant": True},
    "22A (Omicron)": {"rename": "Omicron (BA.4)", "relevant": True},
    "22B (Omicron)": {"rename": "Omicron (BA.5)", "relevant": True},
    "22C (Omicron)": {"rename": "Omicron (BA.2.12.1)", "relevant": True},
    "22D (Omicron)": {"rename": "Omicron (BA.2.75)", "relevant": True},
    "22E (Omicron)": {"rename": "Omicron (BQ.1)", "relevant": True},
    "22F (Omicron)": {"rename": "Omicron (XBB.1)", "relevant": True},
    "23A (Omicron)": {"rename": "Omicron (XBB.1.5)", "relevant": True},
    "23B (Omicron)": {"rename": "Omicron (XBB.1.16)", "relevant": True},
    "23C (Omicron)": {"rename": "Omicron (CH.1.1)", "relevant": True},
    "23D (Omicron)": {"rename": "Omicron (XBB.1.9)", "relevant": True},
    "23E (Omicron)": {"rename": "Omicron (XBB.2.3)", "relevant": True},
    "23F (Omicron)": {"rename": "Omicron (EG.5.1)", "relevant": True},
    "23G (Omicron)": {"rename": "Omicron (XBB.1.5.70)", "relevant": True},
    "23H (Omicron)": {"rename": "Omicron (HK.3)", "relevant": True},
    "23I (Omicron)": {"rename": "Omicron (BA.2.86)", "relevant": True},
    "24A (Omicron)": {"rename": "Omicron (JN.1)", "relevant": True},
    "24B (Omicron)": {"rename": "Omicron (JN.1.11.1)", "relevant": True},
    "24C (Omicron)": {"rename": "Omicron (KP.3)", "relevant": True},
    "24D (Omicron)": {"rename": "Omicron (XDV.1)", "relevant": True},
    "24E (Omicron)": {"rename": "Omicron (KP.3.1.1)", "relevant": True},
    "24F (Omicron)": {"rename": "Omicron (XEC)", "relevant": True},
    "24G (Omicron)": {"rename": "Omicron (KP.2.3)", "relevant": True},
    "24H (Omicron)": {"rename": "Omicron (LF.7)", "relevant": True},
    "24I (Omicron)": {"rename": "Omicron (MV.1)", "relevant": True},
    "25A (Omicron)": {"rename": "Omicron (LP.8.1)", "relevant": True},
    "25B (Omicron)": {"rename": "Omicron (NB.1.8.1)", "relevant": True},
    "25C (Omicron)": {"rename": "Omicron (XFG)", "relevant": True},
    "S:677H.Robin1": {"rename": "S:677H.Robin1", "relevant": False},
    "S:677P.Pelican": {"rename": "S:677P.Pelican", "relevant": False},
    "recombinant": {"rename": "Recombinant", "relevant": False},
}
VARIANTS_MAPPING = {k: v["rename"] for k, v in VARIANTS_DETAILS.items()}
VARIANTS_RELEVANT = list(set(v["rename"] for v in VARIANTS_DETAILS.values() if v["relevant"]))
VARIANTS_NON_RELEVANT = list(set(v["rename"] for v in VARIANTS_DETAILS.values() if not v["relevant"]))
VARIANTS_OMICRON_BA = list(set(v["rename"] for v in VARIANTS_DETAILS.values() if v["rename"].startswith("Omicron (BA")))
VARIANTS_OMICRON_XBB = list(
    set(v["rename"] for v in VARIANTS_DETAILS.values() if v["rename"].startswith("Omicron (XBB"))
)
VARIANTS_OMICRON_JN = list(set(v["rename"] for v in VARIANTS_DETAILS.values() if v["rename"].startswith("Omicron (JN")))
VARIANTS_OMICRON_KP = list(set(v["rename"] for v in VARIANTS_DETAILS.values() if v["rename"].startswith("Omicron (KP")))

COUNTRY_MAPPING = {
    "USA": "United States",
    "Czech Republic": "Czechia",
    "Sint Maarten": "Sint Maarten (Dutch part)",
}
COLUMN_RENAME = {
    "total_sequences": "num_sequences_total",
}
COLUMNS_OUT = [
    "location",
    "date",
    "variant",
    "num_sequences",
    "perc_sequences",
    "num_sequences_total",
]
NUM_SEQUENCES_TOTAL_THRESHOLD = 50


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("sequence")
    ds_population = paths.load_dataset("population")

    # Read table from meadow dataset.
    tb = ds_meadow["sequence"].reset_index()

    #
    # Process data.
    #
    # First table: Variants
    # tb = filter_by_num_sequences(tb, "total_sequences")
    tb = tb.rename(columns={"total_sequences": "num_sequences_total", "cluster": "variant"})
    tb = rename_variant_names(tb)
    ## Fix variants
    tb = filter_variants(tb)
    tb = group_by_sum_variants(tb)
    tb = check_variants(tb)
    ## Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    tb = clean_date(tb)
    tb = add_category_others(tb)
    tb = add_category_non_relevant(tb)
    tb = add_percent(tb)
    tb = add_groupings(tb)
    tb = filter_by_num_sequences(tb, col_total="num_sequences_total")

    # Second table: Sequencing
    tb_seq = tb.astype({"country": "string", "variant": "string"}).copy()
    tb_seq = add_variant_dominant(tb_seq)
    tb_seq = add_variant_totals(tb_seq)
    tb_seq = add_per_capita(tb_seq, ds_population)
    tb_seq = add_cumsum(tb_seq)

    tb = tb.drop(columns=["num_sequences_total"])

    # Edit description key
    tb = add_metadata_variant_groups(tb)

    # Format
    tb = tb.format(["country", "date", "variant"], short_name="variants")
    tb_seq = tb_seq.format(["country", "date"], short_name="sequencing")

    #
    # Save outputs.
    #
    tables = [
        tb,
        tb_seq,
    ]
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=tables, check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # print(ds_garden["variants"]["num_sequences"].metadata.description_processing)
    # Save changes in the new garden dataset.
    ds_garden.save()


def filter_by_num_sequences(tb: Table, col_total: str) -> Table:
    msk = tb[col_total] < NUM_SEQUENCES_TOTAL_THRESHOLD
    # Info
    _sk_perc_rows = round(100 * (msk.sum() / len(tb)), 2)
    _sk_num_countries = tb.loc[msk, "country"].nunique()
    _sk_countries_top = tb[msk]["country"].value_counts().head(10).to_dict()
    print(
        f"Skipping {msk.sum()} datapoints ({_sk_perc_rows}%), affecting {_sk_num_countries} countries. Some are:"
        f" {_sk_countries_top}"
    )
    return tb.loc[~msk]


def rename_variant_names(tb: Table) -> Table:
    """Rename variant names."""
    tb["variant"] = tb["variant"].str.replace("cluster_counts.", "", regex=True).replace(VARIANTS_MAPPING)
    return tb


def filter_variants(tb: Table) -> Table:
    """Filter variants."""
    variants_ignore = [v["rename"] for _, v in VARIANTS_DETAILS.items() if v.get("ignore")]
    rows_init = tb.shape[0]
    tb = tb[-tb.variant.isin(variants_ignore)]
    rows_post = tb.shape[0]
    ratio = round((rows_post - rows_init) / rows_init * 100, 2)
    print(f"Removed: variants {variants_ignore}. Went from {rows_init} to {rows_post} rows ({ratio}%).")
    return tb


def group_by_sum_variants(tb: Table) -> Table:
    """Group by sum of variant values.

    There might be multple rows for a variant.
    """
    cols_index = [c for c in tb.columns if c not in {"num_sequences"}]
    tb = tb.groupby(cols_index, as_index=False, observed=True).sum()
    return tb


def check_variants(tb: Table) -> Table:
    """Check variants are as expected.

    For instance, this can raise an error if new variants are detected. If so, you need to add their details to `VARIANTS_DETAILS`.
    """
    variants_missing = set(tb["variant"].unique()).difference(VARIANTS_MAPPING.values())
    if variants_missing:
        raise ValueError(f"Unknown variants {variants_missing}. Edit class attribute VARIANTS_DETAILS.")
    return tb


def clean_date(tb: Table) -> Table:
    """Clean date column."""
    # Get date of report
    tb["date"] = pd.to_datetime(tb["week"].astype("string"), format="%Y-%m-%d") + timedelta(days=14)
    tb["date"] = tb["date"].astype("string")
    # Get date of publication of latest available file
    last_update = tb.date.m.origins[0].date_published
    tb["date"] = tb["date"].apply(lambda x: min(x, last_update))
    # Group values with duplicate dates caused by min(x, last_update), HOTFIX: https://github.com/owid/etl/pull/3180/files
    # tb = tb.drop(columns=["week"]).groupby(["country", "date", "variant"], as_index=False, observed=True).sum()
    tb = tb.drop(columns=["week"])
    # Set dtype to `date`
    tb["date"] = pd.to_datetime(tb["date"])

    return tb


def add_category_others(tb: Table) -> Table:
    """Get figures for category 'others'."""
    # Get total sequences per country and date
    tb_a = tb.loc[:, ["date", "country", "num_sequences_total"]].drop_duplicates()
    # Get total sequences per country and date
    tb_b = (
        tb.groupby(["date", "country"], as_index=False, observed=True)
        .agg({"num_sequences": sum})
        .rename(columns={"num_sequences": "num_sequences_categorised"})
    )
    tb_c = tb_a.merge(tb_b, on=["date", "country"])
    tb_c[CATEGORY_OTHERS] = tb_c["num_sequences_total"] - tb_c["num_sequences_categorised"]
    tb_c = tb_c.melt(
        id_vars=["country", "date", "num_sequences_total"],
        value_vars=CATEGORY_OTHERS,
        var_name="variant",
        value_name="num_sequences",
    )
    tb = pr.concat([tb, tb_c])
    return tb


def add_category_non_relevant(tb: Table) -> Table:
    """Create 'non-relevant' category.

    On top of category 'others', there is also the 'non-relevant' category. This category groups 'others' together with other variants that are not relevant.
    """
    # Select variants that are not relevant
    tb_secondary = tb.loc[~tb["variant"].isin(VARIANTS_RELEVANT)]
    if tb_secondary.groupby(["country", "date"])["num_sequences_total"].nunique().max() != 1:
        raise ValueError("Different value of `num_sequences_total` found for the same location and date")
    # Get number of sequences (groupby sum)
    tb_secondary = tb_secondary.groupby(["country", "date", "num_sequences_total"], as_index=False, observed=True)[
        "num_sequences"
    ].sum()
    # Assign non-relevant label
    tb_secondary["variant"] = CATEGORY_NON_RELEVANT
    # Add to main table
    tb = pr.concat([tb, tb_secondary], ignore_index=True)
    return tb


def add_percent(tb: Table) -> Table:
    """Get percentage of sequences."""
    tb["perc_sequences"] = 100 * tb["num_sequences"] / tb["num_sequences_total"]
    tb = _correct_excess_percentage(tb)
    return tb


def _correct_excess_percentage(tb: Table) -> Table:
    # 1) `non_who`
    # Get excess
    x = tb.loc[tb["variant"].isin(VARIANTS_RELEVANT + [CATEGORY_NON_RELEVANT])]
    x = x.groupby(["country", "date"], as_index=False, observed=True)["perc_sequences"].sum()
    x = x[abs(x["perc_sequences"] - 100) != 0]
    x["excess"] = x["perc_sequences"] - 100
    # Merge excess quantity with input tb
    tb = tb.merge(x[["country", "date", "excess"]], on=["country", "date"], how="outer")
    # Fill NaN, nearby zero
    tb["excess"] = tb["excess"].fillna(0).round(0)
    # Correct
    mask = tb["variant"].isin([CATEGORY_NON_RELEVANT])
    tb.loc[mask, "perc_sequences"] = (tb.loc[mask, "perc_sequences"] - tb.loc[mask, "excess"]).round(4)
    tb = tb.drop(columns="excess")
    # 2) `others`
    # Get excess
    x = tb.loc[~tb["variant"].isin([CATEGORY_NON_RELEVANT])]
    x = x.groupby(["country", "date"], as_index=False, observed=True)["perc_sequences"].sum()
    x = x[abs(x["perc_sequences"] - 100) != 0]
    x["excess"] = x["perc_sequences"] - 100
    # Merge excess quantity with input tb
    tb = tb.merge(x[["country", "date", "excess"]], on=["country", "date"], how="outer")
    # Fill NaN, nearby zero
    tb["excess"] = tb["excess"].fillna(0).round(0)
    # Correct
    mask = tb["variant"].isin([CATEGORY_OTHERS])
    tb.loc[mask, "perc_sequences"] = (tb.loc[mask, "perc_sequences"] - tb.loc[mask, "excess"]).round(4)
    tb = tb.drop(columns="excess")

    return tb


def add_group_by_prefix(tb: Table, starts_with: str, variant_group_name: str) -> Table:
    """Get totals for 'Omicron'.

    That is, aggregate all Omicron sub-variants.
    """
    # Get only Omicron rows
    msk = tb["variant"].str.startswith(starts_with)
    # Group
    tbg = tb.loc[msk].groupby(["country", "date"], observed=True, as_index=False)
    # Sum values
    values = tbg[["num_sequences", "perc_sequences"]].sum()
    # Get num total
    num_seq_ttl = tbg["num_sequences_total"].unique()
    assert (num_seq_ttl["num_sequences_total"].apply(len) == 1).all()
    num_seq_ttl["num_sequences_total"] = num_seq_ttl["num_sequences_total"].apply(lambda x: x[0])
    # Build df
    values = values.merge(num_seq_ttl, on=["country", "date"])
    values["variant"] = variant_group_name
    tb = pr.concat([tb, values], ignore_index=True)
    return tb


def add_groupings(tb: Table) -> Table:
    # Main Omicron
    tb = add_group_by_prefix(tb, "Omicron", "Omicron")
    # Omicron sublineages
    prefixes = [
        "Omicron (BA",
        "Omicron (XBB",
        "Omicron (JN",
        "Omicron (KP",
    ]
    for prefix in prefixes:
        tb = add_group_by_prefix(tb, prefix, f"{prefix})")
    return tb


def add_variant_dominant(tb: Table) -> Table:
    """Get dominant variant."""
    # Remove Omicron (check dominant within sub-variants)
    tb = tb.loc[tb["variant"] != "Omicron"]
    # Rest of the code
    tb["variant"] = tb["variant"].replace({CATEGORY_NON_RELEVANT: "!non_who"})
    # Sort rows
    tb = tb.sort_values(["num_sequences", "variant"], ascending=[False, True]).drop_duplicates(
        subset=["country", "date"], keep="first"
    )
    # Keep relevant columns
    tb = tb.loc[:, ["country", "date", "num_sequences_total", "variant"]]
    # Renamings
    tb["variant"] = tb.variant.replace({"!non_who": "Others"})
    tb = tb.rename(columns={"variant": "variant_dominant"})
    # Set to NaN if not enough sequencing is available
    msk = tb.num_sequences_total < 30
    tb.loc[msk, "variant_dominant"] = pd.NA
    return tb


def add_variant_totals(tb: Table) -> Table:
    """Get totals for variants."""
    # total = df.groupby(["location", "date", "num_sequences_total"])
    total = tb.loc[:, ["country", "date", "num_sequences_total", "variant_dominant"]].drop_duplicates()
    total = total.rename(columns={"num_sequences_total": "num_sequences"})
    # Sort
    total = total.sort_values(["country", "date"])
    return total


def add_per_capita(tb: Table, ds_population: Dataset) -> Table:
    """Get per-capita values."""
    tb = add_population_2022(tb, ds_population)
    tb["num_sequences_per_1M"] = 1000000 * tb["num_sequences"] / tb["population_2022"]
    tb = tb.drop(columns=["population_2022"])
    return tb


def add_cumsum(tb: Table) -> Table:
    """Add cumulative figures."""
    tb_cum = tb.groupby(["country"])[["num_sequences", "num_sequences_per_1M"]].cumsum()
    tb["num_sequences_cumulative"] = tb_cum.num_sequences
    tb["num_sequences_cumulative_per_1M"] = tb_cum.num_sequences_per_1M
    return tb


def add_metadata_variant_groups(tb: Table) -> Table:
    text_common = "This is a group of lineages which includes:"

    def to_list_str(values):
        return "\n- " + "\n- ".join([f"`{v}`" for v in values])

    list_variants_non_relevant = to_list_str(VARIANTS_NON_RELEVANT)
    list_variants_omicron_ba = to_list_str(VARIANTS_OMICRON_BA)
    list_variants_omicron_xbb = to_list_str(VARIANTS_OMICRON_XBB)
    list_variants_omicron_jn = to_list_str(VARIANTS_OMICRON_JN)
    list_variants_omicron_kp = to_list_str(VARIANTS_OMICRON_KP)

    tb["num_sequences"].metadata.description_key = [
        f"""<% if variant == '{CATEGORY_NON_RELEVANT}' %>{text_common}{list_variants_non_relevant}<% elif variant == 'Omicron (BA)' %>{text_common}{list_variants_omicron_ba}<% elif variant == 'Omicron (XBB)' %>{text_common}{list_variants_omicron_xbb}<% elif variant == 'Omicron (JN)' %>{text_common}{list_variants_omicron_jn}<% elif variant == 'Omicron (KP)' %>{text_common}{list_variants_omicron_kp}<%- endif -%>"""
    ]
    return tb

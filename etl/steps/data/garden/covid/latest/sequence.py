"""Load a meadow dataset and create a garden dataset."""

from datetime import timedelta

import pandas as pd
from owid.catalog import Table

from etl.data_helpers import geo
from etl.helpers import PathFinder, create_dataset

# Get paths and naming conventions for current step.
paths = PathFinder(__file__)

# Variables
## CoVariants -> OWID name mapping. If who=False, variant is placed in bucket "non_who", along with "others"
VARIANTS_DETAILS = {
    "20A.EU2": {"rename": "B.1.160", "who": False},
    "20A/S:439K": {"rename": "B.1.258", "who": False},
    "20A/S:98F": {"rename": "B.1.221", "who": False},
    "20B/S:1122L": {"rename": "B.1.1.302", "who": False},
    "20A/S:126A": {"rename": "B.1.620", "who": False},
    "20B/S:626S": {"rename": "B.1.1.277", "who": False},
    "20B/S:732A": {"rename": "B.1.1.519", "who": False},
    "20C/S:80Y": {"rename": "B.1.367", "who": False},
    "20E (EU1)": {"rename": "B.1.177", "who": False},
    "20H (Beta, V2)": {"rename": "Beta", "who": True},
    "20I (Alpha, V1)": {"rename": "Alpha", "who": True},
    "20J (Gamma, V3)": {"rename": "Gamma", "who": True},
    "21A (Delta)": {"rename": "Delta", "who": True},
    "21B (Kappa)": {"rename": "Kappa", "who": False},
    "21C (Epsilon)": {"rename": "Epsilon", "who": False},
    "21D (Eta)": {"rename": "Eta", "who": False},
    "21F (Iota)": {"rename": "Iota", "who": False},
    "21G (Lambda)": {"rename": "Lambda", "who": True},
    "21H (Mu)": {"rename": "Mu", "who": True},
    "21I (Delta)": {"rename": "Delta", "who": True},
    "21J (Delta)": {"rename": "Delta", "who": True},
    "21K (Omicron)": {"rename": "Omicron (BA.1)", "who": True},
    "21L (Omicron)": {"rename": "Omicron (BA.2)", "who": True},
    "22A (Omicron)": {"rename": "Omicron (BA.4)", "who": True},
    "22B (Omicron)": {"rename": "Omicron (BA.5)", "who": True},
    "22C (Omicron)": {"rename": "Omicron (BA.2.12.1)", "who": True},
    "22D (Omicron)": {"rename": "Omicron (BA.2.75)", "who": False},
    "22E (Omicron)": {"rename": "Omicron (BQ.1)", "who": True},
    "22F (Omicron)": {"rename": "Omicron (XBB)", "who": True},
    "23A (Omicron)": {"rename": "Omicron (XBB.1.5)", "who": True},
    "23B (Omicron)": {"rename": "Omicron (XBB.1.16)", "who": True},
    "23C (Omicron)": {"rename": "Omicron (CH.1.1)", "who": False},
    "23D (Omicron)": {"rename": "Omicron (XBB.1.9)", "who": True},
    "23E (Omicron)": {"rename": "Omicron (XBB.2.3)", "who": True},
    "23F (Omicron)": {"rename": "Omicron (EG.5.1)", "who": True},
    "23G (Omicron)": {"rename": "Omicron (XBB.1.5.70)", "who": True},
    "23H (Omicron)": {"rename": "Omicron (HK.3)", "who": True},
    "23I (Omicron)": {"rename": "Omicron (BA.2.86)", "who": True},
    "24A (Omicron)": {"rename": "Omicron (JN.1)", "who": True},
    "24B (Omicron)": {"rename": "Omicron (JN.1.11.1)", "who": True},
    "24C (Omicron)": {"rename": "Omicron (KP.3)", "who": True},
    "S:677H.Robin1": {"rename": "S:677H.Robin1", "who": False},
    "S:677P.Pelican": {"rename": "S:677P.Pelican", "who": False},
    "recombinant": {"rename": "Recombinant", "who": False},
}
VARIANTS_MAPPING = {k: v["rename"] for k, v in VARIANTS_DETAILS.items()}
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
NUM_SEQUENCES_TOTAL_THRESHOLD = 0


def run(dest_dir: str) -> None:
    #
    # Load inputs.
    #
    # Load meadow dataset.
    ds_meadow = paths.load_dataset("sequence")

    # Read table from meadow dataset.
    tb = ds_meadow["sequence"].reset_index()

    #
    # Process data.
    #
    tb = filter_by_num_sequences(tb)
    tb = tb.rename(columns={"total_sequences": "num_sequences_total", "cluster": "variant"})
    tb = rename_variant_names(tb)
    # Fix variants
    tb = filter_variants(tb)
    tb = group_by_sum_variants(tb)
    tb = check_variants(tb)
    # Harmonize country names
    tb = geo.harmonize_countries(
        df=tb,
        countries_file=paths.country_mapping_path,
    )
    tb = tb.format(["country", "year"])

    #
    # Save outputs.
    #
    # Create a new garden dataset with the same metadata as the meadow dataset.
    ds_garden = create_dataset(
        dest_dir, tables=[tb], check_variables_metadata=True, default_metadata=ds_meadow.metadata
    )

    # Save changes in the new garden dataset.
    ds_garden.save()


def filter_by_num_sequences(tb: Table) -> Table:
    msk = tb["total_sequences"] < NUM_SEQUENCES_TOTAL_THRESHOLD
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
    variants_missing = set(tb["variant"].unique()).difference(VARIANTS_MAPPING.values())
    if variants_missing:
        raise ValueError(f"Unknown variants {variants_missing}. Edit class attribute VARIANTS_DETAILS.")
    return tb


def clean_date(self, tb: Table) -> Table:
    dt = pd.to_datetime(tb["week"], format="%Y-%m-%d") + timedelta(days=14)
    dt = dt + timedelta(days=14)
    last_update = self._parse_last_update_date
    dt = dt.apply(lambda x: clean_date(min(x.date(), last_update), DATE_FORMAT))
    df = tb.assign(
        date=dt,
    )
    return tb.drop(columns=["week"])

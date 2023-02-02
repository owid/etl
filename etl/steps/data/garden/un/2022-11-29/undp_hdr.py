import pandas as pd
from owid.catalog import Dataset, Table
from owid.catalog.utils import underscore_table
from structlog import get_logger

from etl.data_helpers import geo
from etl.helpers import PathFinder
from etl.paths import DATA_DIR

log = get_logger()

# naming conventions
N = PathFinder(__file__)


SHORT_NAME = "undp_hdr"
MEADOW_VERSION = "2022-11-29"
MEADOW_DATASET = DATA_DIR / f"meadow/un/{MEADOW_VERSION}/{SHORT_NAME}"
COLD_START = False


def run(dest_dir: str) -> None:
    log.info("undp_hdr.start")
    # read dataset from meadow
    ds_meadow = Dataset(MEADOW_DATASET)
    # init garden dataset
    ds_garden = init_dataset(dest_dir, ds_meadow)
    # make table
    tb_garden = make_table(ds_meadow)
    # add table to dataset
    ds_garden.add(tb_garden)
    ds_garden.save()
    log.info("undp_hdr.end")


def init_dataset(dest_dir: str, ds_meadow: Dataset) -> Dataset:
    ds_garden = Dataset.create_empty(dest_dir)
    ds_garden.metadata = ds_meadow.metadata
    ds_garden.metadata.update_from_yaml(N.metadata_path)
    return ds_garden


def make_table(ds_meadow: Dataset):
    # get tables
    tb_meadow = ds_meadow["undp_hdr"]
    tb_metadata = ds_meadow["undp_hdr__metadata"]
    # prepare dataframe
    df = pd.DataFrame(tb_meadow)
    df_metadata = pd.DataFrame(tb_metadata)
    df = process(df, df_metadata)

    # build table
    log.info("undp_hdr.creating_table")
    tb_garden = underscore_table(Table(df))
    if COLD_START:
        tb_garden.metadata = tb_meadow.metadata
    else:
        tb_garden.update_metadata_from_yaml(N.metadata_path, "undp_hdr")
    return tb_garden


def process(df: pd.DataFrame, df_metadata: pd.DataFrame) -> pd.DataFrame:
    log.info("undp_hdr.harmonize_countries")
    df = harmonize_countries(df)
    # format table
    log.info("undp_hdr.format_df")
    df = format_df(df)
    # dtypes
    log.info("undp_hdr.dtypes")
    df = df.astype("Float64")
    # sanity check
    log.info("undp_hdr.sanity_check")
    sanity_check(df, df_metadata)
    return df


def format_df(df: pd.DataFrame) -> pd.DataFrame:
    """Outputs dataframe with indexes [country, year] and columns [variable].

    Original dataframe contains column such as 'hdi_2010' or 'mf_2015'; i.e. variable
    names and years are mixed together. This function preserves variables as columns but moves
    the year logic to the index.
    """
    df = df.drop(columns=["iso3", "hdicode", "region"])
    df = df.melt(id_vars=["country"])
    df[["variable", "year"]] = df["variable"].str.extract(r"(.*)_(\d{4})")
    df = df.pivot(index=["country", "year"], columns="variable", values="value")
    return df


def sanity_check(df: pd.DataFrame, df_metadata: pd.DataFrame) -> None:
    """Check that all variables in df are present in df_metadata."""
    cols_df = sorted(set(df.columns))
    cols_df_metadata = sorted(set(df_metadata.loc[df_metadata.short_name.isin(df.columns), "short_name"]))
    assert cols_df_metadata == cols_df


def harmonize_countries(df: pd.DataFrame) -> pd.DataFrame:
    unharmonized_countries = df["country"]
    df = geo.harmonize_countries(df=df, countries_file=str(N.country_mapping_path))

    missing_countries = set(unharmonized_countries[df.country.isnull()])
    if any(missing_countries):
        raise RuntimeError(
            "The following raw country names have not been harmonized. "
            f"Please: (a) edit {N.country_mapping_path} to include these country "
            f"names; or (b) add them to {N.excluded_countries_path}."
            f"Raw country names: {missing_countries}"
        )

    return df

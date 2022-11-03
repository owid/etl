import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog
from structlog import get_logger
import tempfile
from typing import List

from etl.helpers import Names
from etl.steps.data.converters import convert_walden_metadata
from owid.datautils.io import decompress_file
from io import StringIO
import re
import os
from glob import glob
from owid import catalog

log = get_logger()

# naming conventions
N = Names(__file__)


FILE_REGEX = (
    r"([a-zA-Z\-\s,]+), Life tables \(period {table}\), Total\tLast modified: (\d+ [a-zA-Z]{{3}} \d+);  Methods"
    r" Protocol: v\d+ \(\d+\)\n\n((?s:.)*)"
)


def make_table(walden_ds: WaldenCatalog, input_folder: str, table_name: str) -> catalog.Table:
    """Create table."""
    # Load files
    f = f"bltper_{table_name}"
    files = glob(os.path.join(input_folder, f"{f}/*.txt"))
    log.info(f"Looking for available files in {f}...")
    # Create df
    df = make_df(files, table_name)
    # Clean df
    df = clean_df(df)
    # df to table
    table = _df_to_table(walden_ds, table_name, df)
    # underscore all table columns
    table = underscore_table(table)
    return table


def make_df(files: List[str], table_name: str):
    """Create dataframe from files."""
    log.info("Creating dataframe from files...")
    regex = FILE_REGEX.format(table=table_name)
    dfs = []
    # Load each file
    for f in files:
        with open(f, "r") as f:
            text = f.read()
        # Get relevant fields
        match = re.search(regex, text)
        country, _, table_str = match.group(1, 2, 3)
        # Build country df
        df_ = _make_df_country(country, table_str)
        dfs.append(df_)
    # Concatenate all country dfs
    df = pd.concat(dfs)
    return df


def _make_df_country(country, table):
    """Create dataframe for individual country."""
    # Remove starting/ending spaces
    table = table.strip()
    # Remove spacing after newline
    table = re.sub(r"\n\s+", "\n", table)
    # Replace spacing with tabs
    table = re.sub(r"[^\S\r\n]+", "\t", table)
    # Build df
    df = pd.read_csv(StringIO(table), sep="\t")
    # Assign country
    df = df.assign(Country=country)
    # # Filter columns
    # df = df[["Country", "Year", "ex"]].rename(columns={"ex": "life_expectancy"})
    return df


def clean_df(df):
    """Clean dataframe."""
    log.info("Cleaning dataframe...")
    # Order columns
    cols_first = ["Country", "Year"]
    cols = cols_first + [col for col in df.columns if col not in cols_first]
    df = df[cols]
    # Rename columns
    df = _rename_columns_df(df)
    # Set dtypes
    df = _set_dtypes_df(df)
    return df


def _rename_columns_df(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "Country": "country",
            "Year": "year",
            "mx": "central_death_rate",
            "qx": "probability_of_death",
            "ax": "avg_survival_length",
            "lx": "num_survivors",
            "dx": "num_deaths",
            "Lx": "num_person_years_lived",
            "Tx": "num_person_years_remaining",
            "ex": "life_expectancy",
        }
    )


def _set_dtypes_df(df: pd.DataFrame) -> pd.DataFrame:
    return df.astype(
        {
            "central_death_rate": "float",
            "probability_of_death": "float",
            "avg_survival_length": "float",
            "num_survivors": "int",
            "num_deaths": "int",
            "num_person_years_lived": "int",
            "num_person_years_remaining": "int",
            "life_expectancy": "float",
        }
    )


def _df_to_table(walden_ds: WaldenCatalog, table_name: str, df: pd.DataFrame) -> catalog.Table:
    """Create table from dataframe."""
    # create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=table_name,
        title=f"{walden_ds.name} [{table_name}]",
        description=walden_ds.description,
    )
    tb = Table(df, metadata=table_metadata)
    return tb


def run(dest_dir: str) -> None:
    log.info("hmd_lt.start")

    # retrieve raw data from walden
    walden_ds = WaldenCatalog().find_one(namespace="hmd", short_name="hmd_lt", version="2022-11-01")
    local_file = walden_ds.ensure_downloaded()

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = "2022-11-03"

    with tempfile.TemporaryDirectory() as tmp_dir:
        # Decompress files
        decompress_file(local_file, tmp_dir)

        # Load data
        for i in [1, 5]:
            for j in [1, 5, 10]:
                # Create table and folder name
                table_name = f"{i}x{j}"
                # Create table
                log.info(f"Creating table '{table_name}'...")
                table = make_table(walden_ds, tmp_dir, table_name)
                # add table to a dataset
                log.info("Adding table to dataset...")
                ds.add(table)

    # finally save the dataset
    ds.save()

    log.info("hmd_lt.end")

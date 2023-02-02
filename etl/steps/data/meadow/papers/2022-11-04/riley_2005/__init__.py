"""For this pipeline, the Walden data comes as a PDF.

Considering that:

- Extracting the data from the PDF was complex. Several libraries failed to correctly
recognize and extract the data from the table (in page 2)
- The data contained is very little (<60 rows)

I decided to manually extract the data from the PDF and save it as a CSV and get feedback
from authors.
"""
import hashlib

import pandas as pd
import PyPDF2
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog
from structlog import get_logger

from etl.helpers import PathFinder
from etl.steps.data.converters import convert_walden_metadata

log = get_logger()

# naming conventions
N = PathFinder(__file__)


# Dataset details from Walden
NAMESPACE = "papers"
SHORT_NAME = "riley_2005"
VERSION_WALDEN = "2022-11-01"
# Meadow version
VERSION_MEADOW = "2022-11-04"
# Data file
DATA_FILE = N.directory / f"{SHORT_NAME}.data.csv"
HASH_EXPECTED = "b80b1796b1a2ce683db5ea9c5dc5ac2d"


def run(dest_dir: str) -> None:
    log.info(f"{SHORT_NAME}.start")

    # retrieve raw data from walden
    walden_ds = WaldenCatalog().find_one(namespace=NAMESPACE, short_name=SHORT_NAME, version=VERSION_WALDEN)
    local_file = walden_ds.ensure_downloaded()

    # Load data
    check_expected_data(local_file)

    # Load data
    df = load_data()

    # Create table
    tb = make_table(df, walden_ds)

    # initialize meadow dataset
    ds = init_meadow_dataset(dest_dir, walden_ds)
    # add table to a dataset
    ds.add(tb)
    # finally save the dataset
    ds.save()

    log.info(f"{SHORT_NAME}.end")


def check_expected_data(local_file: str) -> None:
    """Check that table in PDF is as expected.

    We do this by comparing the raw text extracted from the PDF with a copy, previously generated,
    of what is to be expected.
    """
    # Extract text from PDF (Walden)
    with open(local_file, "rb") as f:
        pdfReader = PyPDF2.PdfReader(f)
        text_pdf = pdfReader.pages[2].extract_text()
    # Load text from PDF as expected
    hash = hashlib.md5(text_pdf.encode()).hexdigest()
    assert hash == HASH_EXPECTED, "Text from PDF does not match expected text."


def load_data() -> pd.DataFrame:
    """Data loaded from a manually generated CSV.

    This CSV was generated manually by looking at the PDF and transcribing its values.
    """
    df = pd.read_csv(DATA_FILE)
    return df


def init_meadow_dataset(dest_dir: str, walden_ds: WaldenCatalog) -> Dataset:
    """Initialize meadow dataset."""
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = VERSION_MEADOW
    return ds


def make_table(df: pd.DataFrame, walden_ds: WaldenCatalog) -> Table:
    """Create table from dataframe and Walden metadata."""
    table_metadata = TableMeta(
        short_name=walden_ds.short_name,
        title="Life expectancy at birth",
        description="Life expectancy at birth estimates.",
    )
    tb = Table(df, metadata=table_metadata)
    tb = underscore_table(tb)
    return tb

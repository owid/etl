import pandas as pd
from owid.catalog import Dataset, Table, TableMeta
from owid.catalog.utils import underscore_table
from owid.walden import Catalog as WaldenCatalog
from structlog import get_logger

from etl.helpers import Names
from etl.paths import REFERENCE_DATASET
from etl.steps.data.converters import convert_walden_metadata

log = get_logger()

# naming conventions
N = Names(__file__)


def run(dest_dir: str) -> None:
    log.info("penn_world_table_national_accounts.start")

    # retrieve raw data from walden
    walden_ds = WaldenCatalog().find_one(
        namespace="ggdc", short_name="penn_world_table_national_accounts", version="2021-06-18"
    )
    local_file = walden_ds.ensure_downloaded()

    df = pd.read_excel(local_file, sheet_name="Data")

    # # clean and transform data
    # df = clean_data(df)

    # Read reference dataset for countries and regions

    ds_reference = Dataset(REFERENCE_DATASET)
    df_countries_regions = pd.DataFrame(ds_reference["countries_regions"]).reset_index()

    # Merge dataset and country dictionary to get the name of the country (and rename it as "country")
    df = pd.merge(
        df, df_countries_regions[["name", "iso_alpha3"]], left_on="countrycode", right_on="iso_alpha3", how="left"
    )
    df = df.rename(columns={"name": "country"})
    df = df.drop(columns=["iso_alpha3"])

    # Add country names for some specific 3-letter codes
    df.loc[df["countrycode"] == "CH2", ["country"]] = "China (alternative inflation series)"
    df.loc[df["countrycode"] == "CSK", ["country"]] = "Czechoslovakia"
    df.loc[df["countrycode"] == "RKS", ["country"]] = "Kosovo"
    df.loc[df["countrycode"] == "SUN", ["country"]] = "USSR"
    df.loc[df["countrycode"] == "YUG", ["country"]] = "Yugoslavia"

    # create new dataset and reuse walden metadata
    ds = Dataset.create_empty(dest_dir)
    ds.metadata = convert_walden_metadata(walden_ds)
    ds.metadata.version = "2022-11-28"

    # create table with metadata from dataframe
    table_metadata = TableMeta(
        short_name=walden_ds.short_name,
        title=walden_ds.name,
        description=walden_ds.description,
    )
    tb = Table(df, metadata=table_metadata)

    # underscore all table columns
    tb = underscore_table(tb)

    ds.metadata.update_from_yaml(N.metadata_path, if_source_exists="replace")
    tb.update_metadata_from_yaml(N.metadata_path, "penn_world_table_national_accounts")

    # add table to a dataset
    ds.add(tb)

    # finally save the dataset
    ds.save()

    log.info("penn_world_table_national_accounts.end")


# def clean_data(df: pd.DataFrame) -> pd.DataFrame:
#     return df.rename(
#         columns={
#             "country": "country",
#             "year": "year",
#             "pop": "population",
#             "gdppc": "gdp",
#         }
#     ).drop(columns=["countrycode"])

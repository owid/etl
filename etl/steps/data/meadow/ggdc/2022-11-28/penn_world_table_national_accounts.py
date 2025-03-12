import owid.catalog.processing as pr
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("penn_world_table_national_accounts.start")

    # retrieve raw data
    snap = paths.load_snapshot("penn_world_table.xlsx")
    tb = snap.read_excel(sheet_name="Data")

    # # clean and transform data
    # tb = clean_data(tb)

    # Read reference dataset for countries and regions
    tb_countries_regions = paths.load_dataset("regions")["regions"]

    # Merge dataset and country dictionary to get the name of the country (and rename it as "country")
    tb = pr.merge(
        tb, tb_countries_regions[["name", "iso_alpha3"]], left_on="countrycode", right_on="iso_alpha3", how="left"
    )
    tb = tb.rename(columns={"name": "country"})
    tb = tb.drop(columns=["iso_alpha3"])
    tb = tb.astype({"countrycode": str, "country": str})

    # Add country names for some specific 3-letter codes
    tb.loc[tb["countrycode"] == "CH2", ["country"]] = "China (alternative inflation series)"
    tb.loc[tb["countrycode"] == "CSK", ["country"]] = "Czechoslovakia"
    tb.loc[tb["countrycode"] == "RKS", ["country"]] = "Kosovo"
    tb.loc[tb["countrycode"] == "SUN", ["country"]] = "USSR"
    tb.loc[tb["countrycode"] == "YUG", ["country"]] = "Yugoslavia"

    # underscore all table columns
    tb = tb.underscore()

    ds = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)
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

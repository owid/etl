from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("penn_world_table.start")

    # retrieve raw data
    snap = paths.load_snapshot("penn_world_table.xlsx")
    tb = snap.read_excel(sheet_name="Data")

    # # clean and transform data
    # tb = clean_data(tb)

    # underscore all table columns
    tb = tb.underscore()

    ds = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)
    ds.save()

    log.info("penn_world_table.end")


# def clean_data(df: pd.DataFrame) -> pd.DataFrame:
#     return df.rename(
#         columns={
#             "country": "country",
#             "year": "year",
#             "pop": "population",
#             "gdppc": "gdp",
#         }
#     ).drop(columns=["countrycode"])

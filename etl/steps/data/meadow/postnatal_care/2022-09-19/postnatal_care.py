import numpy as np
import pandas as pd
from structlog import get_logger

from etl.helpers import PathFinder, create_dataset

log = get_logger()

# naming conventions
paths = PathFinder(__file__)


def run(dest_dir: str) -> None:
    log.info("postnatal_care.start")

    # retrieve raw data
    snap = paths.load_snapshot("postnatal_care.csv")
    tb = snap.read()

    tb = tb[tb["Series Code"].notna()]

    # clean and transform data
    tb = clean_data(tb)

    # underscore all table columns
    tb = tb.underscore()

    #
    # Save outputs.
    #
    # Create a new grapher dataset with the same metadata as the garden dataset.
    ds = create_dataset(dest_dir, tables=[tb], default_metadata=snap.metadata)

    # finally save the dataset
    ds.save()

    log.info("postnatal_care.end")


def clean_data(tb: pd.DataFrame) -> pd.DataFrame:
    tb = tb.drop(columns=["Country Code", "Series Name", "Series Code"])

    cols = tb.columns[1:].str[:4].tolist()
    tb.columns = ["country"] + cols
    tb = tb.replace("..", np.nan)
    tb = tb.melt(id_vars="country", value_vars=cols)
    tb = tb.rename(columns={"variable": "year", "value": "postnatal_care_coverage"})
    return tb

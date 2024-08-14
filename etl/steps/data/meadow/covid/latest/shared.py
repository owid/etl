from typing import Optional

import pandas as pd
from owid.catalog import Table

ZERO_DAY = "2020-01-21"


def year_to_date(tb: Table, col_date: str = "date", col_year: str = "Year", zero_day: Optional[str] = None) -> Table:
    if zero_day is None:
        zero_day = ZERO_DAY
    tb[col_date] = pd.Timestamp(zero_day) + pd.to_timedelta(tb[col_year], unit="days")  # type: ignore
    tb = tb.drop(columns=[col_year])
    return tb

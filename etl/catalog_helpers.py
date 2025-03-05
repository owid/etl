"""Methods concerning owid.catalog, but that have not been integrated in the library yet. Instead, they are grouped here.

Difference with etl.helpers: In there, we want to keep the high-level utils for ETL. That includes tooling to interact with our catalog but also for reading/writing to/from other places.
"""

import datetime as dt

from owid.catalog import Table


def last_date_accessed(tb: Table) -> str:
    """Get maximum date_accessed from all origins in the table and display it in a specific format.

    Usage:
        create_dataset(..., yaml_params={"date_accessed": last_date_accessed(tb)})
    """
    date_accessed = max([origin.date_accessed for col in tb.columns for origin in tb[col].m.origins])
    return dt.datetime.strptime(date_accessed, "%Y-%m-%d").strftime("%d %B %Y")


def last_date_published(tb: Table) -> str:
    """Get maximum date_published from all origins in the table and display it in a specific format.

    Usage:
        create_dataset(..., yaml_params={"date_published": last_date_published(tb)})
    """
    date_published = max([origin.date_published for col in tb.columns for origin in tb[col].m.origins])
    return dt.datetime.strptime(date_published, "%Y-%m-%d").strftime("%d %B %Y")

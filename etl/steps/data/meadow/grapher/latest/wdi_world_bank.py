import pandas as pd
import structlog
from pathlib import Path
from typing import Optional

from owid.catalog import Dataset, Table, Variable, VariableMeta

from etl.db_backport import DBBackport

log = structlog.get_logger(__name__)


WDI_CETS_FILE = Path(__file__).parent / "WDI_CETS.xls"


def _load_topics() -> pd.DataFrame:
    """Load official WDI topics from the World Bank. Return dataframe
    with columns `Topic` and `Topic description`."""
    df: pd.DataFrame = pd.read_excel(WDI_CETS_FILE, sheet_name="Coding")
    df = df[["Topic", "Topic description"]]
    df = df.loc[: df[df.Topic == "Non-CETS indicators in WDI"].index[0]].dropna()
    return df


def set_variable_metadata(variable: Variable, meta: VariableMeta) -> None:
    # TODO: setter on metadata would be nicer
    variable._fields[variable.checked_name] = meta


# TODO: use empty default before merging
def run(dest_dir: str, topic_codes: Optional[list[str]] = ["AG"]) -> None:
    DATASET_NAME = "World Development Indicators - World Bank (2021.07.30)"

    db = DBBackport()
    ds_id, ds_meta = db.find_dataset(name=DATASET_NAME)
    ds = Dataset.create_empty(dest_dir, ds_meta)

    topics = _load_topics()
    for topic, topic_description in topics[["Topic", "Topic description"]].values:
        if topic_codes and topic not in topic_codes:
            log.info("skip", code=topic)
            continue

        log.info("process", code=topic)

        vs_ids, vs_metas = db.find_variables(
            dataset_id=ds_id, where=f"code like '{topic}.%%'"
        )
        df = db.find_values(variable_ids=list(vs_ids), format="wide")
        log.info(
            "wdi_world_bank.find_values", memory_usage=df.memory_usage().sum() / 1e6
        )

        t = Table(df)
        t.metadata.short_name = topic_description

        # add variable metadata
        for col, vs_meta in zip(t.columns, vs_metas):
            set_variable_metadata(t[col], vs_meta)

        ds.add(t)

    ds.save()


if __name__ == "__main__":
    # python etl/steps/data/meadow/grapher/latest/wdi_world_bank.py
    run("/tmp/", ["AG"])

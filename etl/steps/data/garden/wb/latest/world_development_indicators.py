import pandas as pd
import structlog
from pathlib import Path

from owid.catalog import Dataset

from etl.paths import DATA_DIR

REFERENCE = DATA_DIR / "reference"

log = structlog.get_logger(__name__)

WBI_BACKPORT = (
    DATA_DIR
    / "backport/owid/latest/dataset_5357_world_development_indicators__world_bank__2021_07_30"
)

WDI_CETS_FILE = Path(__file__).parent / "WDI_CETS.xls"


def _load_topics() -> pd.DataFrame:
    """Load official WDI topics from the World Bank. Return dataframe
    with columns `Topic` and `Topic description`."""
    df: pd.DataFrame = pd.read_excel(WDI_CETS_FILE, sheet_name="Coding")
    df = df[["Topic", "Topic description"]]
    df = df.loc[: df[df.Topic == "Non-CETS indicators in WDI"].index[0]].dropna()
    return df


def run(dest_dir: str) -> None:
    """Group indicators into tables by their topic codes."""
    ds = Dataset(WBI_BACKPORT)
    t = ds["dataset_5357_world_development_indicators__world_bank__2021_07_30"]

    new_ds = Dataset.create_empty(dest_dir, ds.metadata)
    new_ds.metadata.short_name = "world_development_indicators_world_bank"

    topics = _load_topics()
    for topic, topic_description in topics[["Topic", "Topic description"]].values:
        log.info("process", code=topic)

        code_columns = []
        for col in t.columns:
            if t[col].additional_info["grapher_meta"]["code"].startswith(f"{topic}."):
                code_columns.append(col)

        t_topic = t[code_columns].prune_metadata().copy()

        # drop rows with all NA values
        t_topic = t_topic.dropna(how="all")

        # name tables after their topic codes
        t_topic.metadata.short_name = topic.lower()
        t_topic.metadata.description = topic_description

        new_ds.add(t_topic)

    new_ds.save()

import random

import pandas as pd
import streamlit as st

from apps.wizard.app_pages.anomalist.utils import AnomalyTypeEnum
from apps.wizard.utils import cached

# This should be removed and replaced with dynamic fields
ENTITIES_DEFAULT = [
    "Spain",
    "France",
    "Germany",
    "Italy",
    "United Kingdom",
    "United States",
    "China",
    "India",
    "Japan",
    "Brazil",
    "Russia",
    "Canada",
    "South Africa",
    "Australia",
    "Venezuela",
    "Croatia",
    "Azerbaijan",
]


def mock_anomalies_df_time_change(indicators_id, n=5):
    records = [
        {
            "entity": random.sample(ENTITIES_DEFAULT, 1)[0],
            "year": random.randint(1950, 2020),
            "score": round(random.random(), 2),
            "indicator_id": random.sample(indicators_id, 1)[0],
        }
        for i in range(n)
    ]

    df = pd.DataFrame(records)
    return df


def mock_anomalies_df_upgrade_change(indicators_id_upgrade, n=5):
    records = [
        {
            "entity": random.sample(ENTITIES_DEFAULT, 1)[0],
            "year": random.randint(1950, 2020),
            "score": round(random.random(), 2),
            "indicator_id": random.sample(indicators_id_upgrade, 1)[0],
        }
        for i in range(n)
    ]

    df = pd.DataFrame(records)
    return df


def mock_anomalies_df_upgrade_missing(indicators_id_upgrade, n=5):
    records = [
        {
            "entity": random.sample(ENTITIES_DEFAULT, 1)[0],
            "year": random.randint(1950, 2020),
            "score": random.randint(0, 1),
            "indicator_id": random.sample(indicators_id_upgrade, 1)[0],
        }
        for i in range(n)
    ]

    df = pd.DataFrame(records)
    return df


@st.cache_data(ttl=60 * 60)
def mock_anomalies_df(indicators_id, indicators_id_upgrade, n=5):
    # 1/ Get anomalies df
    ## Time change
    df_change = mock_anomalies_df_time_change(indicators_id, n)
    df_change["type"] = AnomalyTypeEnum.TIME_CHANGE.value
    ## Upgrade: value change
    df_upgrade_change = mock_anomalies_df_upgrade_change(indicators_id_upgrade, n)
    df_upgrade_change["type"] = AnomalyTypeEnum.UPGRADE_CHANGE.value
    ## Upgrade: Missing data point
    df_upgrade_miss = mock_anomalies_df_upgrade_missing(indicators_id_upgrade, n)
    df_upgrade_miss["type"] = AnomalyTypeEnum.UPGRADE_MISSING.value

    # 2/ Combine
    df = pd.concat([df_change, df_upgrade_change, df_upgrade_miss])

    # Ensure there is only one row per entity, anomaly type and indicator
    df = df.sort_values("score", ascending=False).drop_duplicates(["entity", "type", "indicator_id"])

    # Replace entity name with entity ID
    entity_mapping = cached.load_entity_ids()
    entity_mapping_inv = {v: k for k, v in entity_mapping.items()}
    df["entity_id"] = df["entity"].map(entity_mapping_inv)
    # st.write(entity_mapping)

    # 3/ Add meta scores
    num_scores = len(df)
    df["score_population"] = [random.random() for i in range(num_scores)]
    df["score_analytics"] = [random.random() for i in range(num_scores)]

    # 4/ Weighed combined score
    # Weighed combined score
    w_score = 1
    w_pop = 1
    w_views = 1
    df["score_weighed"] = (w_score * df["score"] + w_pop * df["score_population"] + w_views * df["score_analytics"]) / (
        w_score + w_pop + w_views
    )
    return df

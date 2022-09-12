import pandas as pd


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(
        columns={
            "location_name": "country",
            "val": "value",
            "measure_name": "measure",
            "sex_name": "sex",
            "age_name": "age",
            "cause_name": "cause",
            "metric_name": "metric",
        },
        errors="ignore",
    ).drop(
        columns=["measure_id", "location_id", "sex_id", "age_id", "cause_id", "metric_id"],
        errors="ignore",
    )

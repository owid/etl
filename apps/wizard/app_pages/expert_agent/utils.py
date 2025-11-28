from pathlib import Path

from pydantic import BaseModel, Field
from structlog import get_logger

from apps.utils.llms import LLM_MODELS

log = get_logger()

CURRENT_DIR = Path(__file__).parent


# Load available models
## See all of them in https://github.com/pydantic/pydantic-ai/blob/master/pydantic_ai_slim/pydantic_ai/models/__init__.py
MODELS_DISPLAY = {m["name"]: m["display_name"] for m in LLM_MODELS["models"]}
MODELS_AVAILABLE_LIST = list(MODELS_DISPLAY.keys())
MODEL_DEFAULT = "openai:gpt-5-mini"


class DataFrameModel(BaseModel):
    columns: list[str] = Field(description="List of column names in the DataFrame.")
    dtypes: dict[str, str] = Field(description="Dictionary mapping column names to their data types.")
    data: list[list] = Field(description="Sample rows from the DataFrame (limited for performance).")
    total_rows: int = Field(description="Total number of rows in the original DataFrame.")


class QueryResult(BaseModel):
    message: str = Field(
        description="Status message about the query execution. 'SUCCESS' if valid, otherwise error details."
    )
    valid: bool = Field(description="Whether the query executed successfully and returned data.")
    result: DataFrameModel | None = Field(
        default=None, description="The query results as a serialized DataFrame with sample data."
    )
    url_metabase: str | None = Field(
        default=None, description="URL to the created Metabase question for interactive exploration."
    )
    url_datasette: str | None = Field(default=None, description="URL to view the query results in Datasette.")
    card_id_metabase: int | None = Field(
        default=None, description="Metabase card ID that can be used with plotting tools."
    )


def serialize_df(df, num_rows: int | None = None) -> DataFrameModel:
    if num_rows is None:
        df_head = df
    else:
        df_head = df.head(num_rows)

    data = DataFrameModel(
        columns=df.columns.tolist(),
        dtypes={c: str(t) for c, t in df.dtypes.items()},
        data=df_head.to_numpy().tolist(),  # small slice
        total_rows=len(df),
    )
    return data

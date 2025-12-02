from contextlib import asynccontextmanager

import logfire
import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware

from api_search.semantic_search import initialize_semantic_search_async
from api_search.v1 import v1
from etl import config

log = structlog.get_logger()

config.enable_sentry()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize semantic search on API startup."""
    initialize_semantic_search_async()
    yield


def get_application():
    _app = FastAPI(
        title="OWID Search API",
        description="""
Semantic search API for Our World in Data indicators using embeddings and vector similarity.

This API enables you to find relevant indicators using natural language queries. Results are ranked by semantic similarity to your query, making it easy to discover indicators even if you don't know the exact terminology.

For searching charts and articles by title or content, see our [Search API](https://docs.owid.io/projects/etl/api/search-api/).
""".strip(),
        lifespan=lifespan,
        servers=[
            {"url": "https://search.owid.io", "description": "Production server"},
        ],
    )

    _app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    _app.add_middleware(
        SentryAsgiMiddleware,  # type: ignore
    )

    return _app


app = get_application()

app.include_router(v1)

if config.LOGFIRE_TOKEN_ETL_API:
    logfire.configure(token=config.LOGFIRE_TOKEN_ETL_API)
    logfire.instrument_fastapi(app)
else:
    logfire.configure(send_to_logfire=False)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}

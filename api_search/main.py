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
    _app = FastAPI(title="OWID Search API", lifespan=lifespan)

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

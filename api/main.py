import logfire
import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware

from api.v1 import v1
from etl import config
from etl.db import get_engine

log = structlog.get_logger()

config.enable_sentry()

engine = get_engine()


def get_application():
    _app = FastAPI(title="ETL API")

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

# NOTE: I tried using subapplications, but they don't propagate errors to middleware
# see https://github.com/tiangolo/fastapi/discussions/8577 (even the latest versions didn't help)
app.include_router(v1)

if config.LOGFIRE_TOKEN_ETL_API:
    logfire.configure(token=config.LOGFIRE_TOKEN_ETL_API)
    logfire.instrument_fastapi(app)
else:
    logfire.configure(send_to_logfire=False)


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}

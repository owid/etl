from contextlib import asynccontextmanager

import logfire
import structlog
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from sentry_sdk.integrations.asgi import SentryAsgiMiddleware

from api.semantic_search import initialize_semantic_search_async
from api.v1 import v1
from etl import config
from etl.db import get_engine
from etl.slack_helpers import format_slack_message, send_slack_message

log = structlog.get_logger()

config.enable_sentry()

engine = get_engine()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize semantic search on API startup."""
    initialize_semantic_search_async()
    yield


def get_application():
    _app = FastAPI(title="ETL API", lifespan=lifespan)

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


@app.middleware("http")
async def slack_middleware(request: Request, call_next):
    # don't log OPTIONS requests
    if request.method == "OPTIONS":
        return await call_next(request)

    # don't log favicon requests
    if "favicon.ico" in request.url.path:
        return await call_next(request)

    req_body = await request.body()

    log.info("request", method=request.method, url=str(request.url), body=req_body)

    response = await call_next(request)

    res_body = b""
    async for chunk in response.body_iterator:
        res_body += chunk

    log.info("response", method=request.method, url=str(request.url), status_code=response.status_code, body=res_body)

    # Send requests to Slack
    if "search/indicators" not in str(request.url):
        send_slack_message(
            "#metadata-updates",
            format_slack_message(
                request.method, request.url, response.status_code, req_body.decode(), res_body.decode()
            ),
        )

    return Response(
        content=res_body,
        status_code=response.status_code,
        headers=dict(response.headers),
    )


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}

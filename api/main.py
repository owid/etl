import bugsnag
import structlog
from bugsnag.asgi import BugsnagMiddleware
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from api.v1 import v1
from etl import config
from etl.db import get_engine

from . import utils

log = structlog.get_logger()

bugsnag.configure(
    api_key=config.BUGSNAG_API_KEY,
)

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
        BugsnagMiddleware,
    )

    return _app


app = get_application()

# NOTE: I tried using subapplications, but they don't propagate errors to middleware
# see https://github.com/tiangolo/fastapi/discussions/8577 (even the latest versions didn't help)
app.include_router(v1)


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

    utils.send_slack_message(
        "#metadata-updates",
        utils.format_slack_message(
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

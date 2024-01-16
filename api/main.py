import json

import bugsnag
import structlog
from bugsnag.asgi import BugsnagMiddleware
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from slack_sdk import WebClient

from api.v1 import v1
from etl import config
from etl.db import get_engine
from etl.helpers import read_json_schema
from etl.paths import SCHEMAS_DIR

log = structlog.get_logger()

bugsnag.configure(
    api_key=config.BUGSNAG_API_KEY,
)

engine = get_engine()

slack_client = WebClient(token=config.SLACK_API_TOKEN)

DATASET_SCHEMA = read_json_schema(path=SCHEMAS_DIR / "dataset-schema.json")


def get_application():
    _app = FastAPI(title="ETL API")

    _app.add_middleware(
        CORSMiddleware,
        # allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
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

# mount subapplications as versions
app.mount("/v1", v1)


def send_slack_message(message: str) -> None:
    if config.SLACK_API_TOKEN:
        slack_client.chat_postMessage(channel="@Mojmir", text=message)


def format_slack_message(method, url, req_body, res_body):
    try:
        res_body = json.dumps(json.loads(res_body), indent=2)
    except json.decoder.JSONDecodeError:
        pass

    try:
        req_body = json.dumps(json.loads(req_body), indent=2)
    except json.decoder.JSONDecodeError:
        pass

    return f"""
:information_source: *{method}* {url}
Request
```
{req_body}
```
Response
```
{res_body}
```
    """


@app.middleware("http")
async def slack_middleware(request: Request, call_next):
    req_body = await request.body()

    log.info("request", method=request.method, url=str(request.url), body=req_body)

    response = await call_next(request)

    res_body = b""
    async for chunk in response.body_iterator:
        res_body += chunk

    log.info("response", method=request.method, url=str(request.url), body=res_body)

    send_slack_message(format_slack_message(request.method, request.url, req_body.decode(), res_body.decode()))

    return Response(
        content=res_body,
        status_code=response.status_code,
        headers=dict(response.headers),
    )


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}

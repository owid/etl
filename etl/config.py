#
#  config.py
#

"""
The environment variables and settings here are for publishing options, they're
only important for OWID staff.
"""

import os
import pwd
from os import environ as env

import bugsnag
from dotenv import load_dotenv

from etl.paths import BASE_DIR

ENV_FILE = env.get("ENV", BASE_DIR / ".env")

load_dotenv(ENV_FILE)

DEBUG = env.get("DEBUG") == "True"

# publishing to OWID's public data catalog
S3_BUCKET = "owid-catalog"
S3_REGION_NAME = "nyc3"
S3_ENDPOINT_URL = "https://nyc3.digitaloceanspaces.com"
S3_HOST = "nyc3.digitaloceanspaces.com"
S3_ACCESS_KEY = env.get("OWID_ACCESS_KEY")
S3_SECRET_KEY = env.get("OWID_SECRET_KEY")

# publishing to R2 public data catalog
R2_REGION_NAME = "auto"
R2_ENDPOINT_URL = env.get("R2_ENDPOINT_URL")
R2_ACCESS_KEY = env.get("R2_ACCESS_KEY")
R2_SECRET_KEY = env.get("R2_SECRET_KEY")

# publishing to grapher's MySQL db
GRAPHER_USER_ID = env.get("GRAPHER_USER_ID")
DB_NAME = env.get("DB_NAME", "grapher")
DB_HOST = env.get("DB_HOST", "localhost")
DB_PORT = int(env.get("DB_PORT", "3306"))
DB_USER = env.get("DB_USER", "root")
DB_PASS = env.get("DB_PASS", "")

# metaplay config
METAPLAY_PORT = int(env.get("METAPLAY_PORT", "8051"))


def get_username():
    return pwd.getpwuid(os.getuid())[0]


if "DATA_API_ENV" in env:
    DATA_API_ENV = env["DATA_API_ENV"]
else:
    DATA_API_ENV = env.get("DATA_API_ENV", get_username())

# Production checks
if DATA_API_ENV == "production":
    assert DB_NAME == "live_grapher", "DB_NAME must be set to live_grapher when publishing to production"

if DB_NAME == "live_grapher":
    assert DATA_API_ENV == "production", "DATA_API_ENV must be set to production when publishing to live_grapher"

# if running against live, use s3://owid-api, otherwise use s3://owid-api-staging
# Cloudflare workers running on https://api.ourworldindata.org/ and https://api-staging.owid.io/ will use them
if DATA_API_ENV == "production":
    BAKED_VARIABLES_PATH = "s3://owid-api/v1/indicators"
    DATA_API_URL = "https://api.ourworldindata.org/v1/indicators"
else:
    BAKED_VARIABLES_PATH = f"s3://owid-api-staging/{DATA_API_ENV}/v1/indicators"
    DATA_API_URL = f"https://api-staging.owid.io/{DATA_API_ENV}/v1/indicators"


def variable_data_url(variable_id):
    return f"{DATA_API_URL}/{variable_id}.data.json"


def variable_metadata_url(variable_id):
    return f"{DATA_API_URL}/{variable_id}.metadata.json"


# run ETL steps with debugger on exception
IPDB_ENABLED = False

# number of workers for grapher inserts
GRAPHER_INSERT_WORKERS = int(env.get("GRAPHER_WORKERS", 40))

# forbid any individual step from consuming more than this much memory
# (only enforced on Linux)
MAX_VIRTUAL_MEMORY_LINUX = 32 * 2**30  # 32 GB

# increment this to force a full rebuild of all datasets
ETL_EPOCH = 3

# any garden or grapher dataset after this date will have strict mode enabled
STRICT_AFTER = "2023-06-25"


def enable_bugsnag() -> None:
    BUGSNAG_API_KEY = env.get("BUGSNAG_API_KEY")
    if BUGSNAG_API_KEY and not DEBUG:
        bugsnag.configure(
            api_key=BUGSNAG_API_KEY,
        )  # type: ignore

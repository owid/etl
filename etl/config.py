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

# publishing to grapher's MySQL db
GRAPHER_USER_ID = env.get("GRAPHER_USER_ID")
DB_NAME = env.get("DB_NAME", "grapher")
DB_HOST = env.get("DB_HOST", "localhost")
DB_PORT = int(env.get("DB_PORT", "3306"))
DB_USER = env.get("DB_USER", "root")
DB_PASS = env.get("DB_PASS", "")


def get_username():
    return pwd.getpwuid(os.getuid())[0]


# if running against live or staging, use s3://owid-catalog that has CDN
# otherwise use s3://owid-test/baked-variables/<username> for local development
# it might be better to save things locally instead of S3, but that would require
# a lot of changes to the codebase (and even grapher one)
if DB_NAME in ("live_grapher", "staging_grapher"):
    DEFAULT_BAKED_VARIABLES_PATH = f"s3://owid-catalog/baked-variables/{DB_NAME}"
else:
    DEFAULT_BAKED_VARIABLES_PATH = f"s3://owid-test/baked-variables/{get_username()}"
BAKED_VARIABLES_PATH = env.get("BAKED_VARIABLES_PATH", DEFAULT_BAKED_VARIABLES_PATH)

# run ETL steps with debugger on exception
IPDB_ENABLED = False

# number of workers for grapher inserts
GRAPHER_INSERT_WORKERS = int(env.get("GRAPHER_WORKERS", 40))

# forbid any individual step from consuming more than this much memory
# (only enforced on Linux)
MAX_VIRTUAL_MEMORY_LINUX = 32 * 2**30  # 20 GB


def enable_bugsnag() -> None:
    BUGSNAG_API_KEY = env.get("BUGSNAG_API_KEY")
    if BUGSNAG_API_KEY and not DEBUG:
        bugsnag.configure(
            api_key=BUGSNAG_API_KEY,
        )  # type: ignore

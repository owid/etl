#
#  config.py
#

"""
The environment variables and settings here are for publishing options, they're
only important for OWID staff.
"""

from os import environ as env

import bugsnag
from dotenv import load_dotenv

ENV_FILE = env.get("ENV", ".env")

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

# run ETL steps with debugger on exception
IPDB_ENABLED = False

# number of workers for grapher inserts
# NOTE: this will soon be deprecated after we get rid of data_values
GRAPHER_INSERT_WORKERS = int(env.get("GRAPHER_WORKERS", 10))

# forbid any individual step from consuming more than this much memory
# (only enforced on Linux)
MAX_VIRTUAL_MEMORY_LINUX = 16 * 2**30  # 16 GB


def enable_bugsnag() -> None:
    BUGSNAG_API_KEY = env.get("BUGSNAG_API_KEY")
    if BUGSNAG_API_KEY and not DEBUG:
        bugsnag.configure(
            api_key=BUGSNAG_API_KEY,
        )  # type: ignore

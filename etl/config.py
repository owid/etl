#
#  config.py
#

"""
The environment variables and settings here are for publishing options, they're
only important for OWID staff.
"""
import os
import pwd
import re
from dataclasses import dataclass, fields
from os import environ as env
from pathlib import Path
from typing import Literal, Optional, cast

import bugsnag
import git
import pandas as pd
import structlog
from dotenv import dotenv_values, load_dotenv
from sqlalchemy.engine import Engine

from etl.paths import BASE_DIR

log = structlog.get_logger()

ENV_FILE = Path(env.get("ENV_FILE", BASE_DIR / ".env"))


def get_username():
    return pwd.getpwuid(os.getuid())[0]


def load_env():
    if env.get("ENV", "").startswith("."):
        raise ValueError(f"ENV was replaced by ENV_FILE, please use ENV_FILE={env['ENV']} ... instead.")

    load_dotenv(ENV_FILE)


def _normalise_branch(branch_name):
    return re.sub(r"[\/\._]", "-", branch_name)


def get_container_name(branch_name):
    normalized_branch = _normalise_branch(branch_name)

    # Strip staging-site- prefix to add it back later
    normalized_branch = normalized_branch.replace("staging-site-", "")

    # Ensure the container name is less than 63 characters
    container_name = f"staging-site-{normalized_branch[:50]}"
    # Remove trailing hyphens
    return container_name.rstrip("-")


load_env()


pd.set_option("future.no_silent_downcasting", True)

# When DEBUG is on
# - run steps in the same process (speeding up ETL)
DEBUG = env.get("DEBUG") in ("True", "true", "1")

# Environment, e.g. production, staging, dev
ENV = env.get("ENV", "dev")
ENV_IS_REMOTE = ENV in ("production", "staging")

# publishing to OWID's public data catalog in R2
R2_BUCKET = "owid-catalog"
R2_SNAPSHOTS_PUBLIC = "owid-snapshots"
R2_SNAPSHOTS_PRIVATE = "owid-snapshots-private"
R2_SNAPSHOTS_PUBLIC_READ = "https://snapshots.owid.io"

# publishing to grapher's MySQL db
GRAPHER_USER_ID = env.get("GRAPHER_USER_ID")
DB_NAME = env.get("DB_NAME", "grapher")
DB_HOST = env.get("DB_HOST", "localhost")
DB_PORT = int(env.get("DB_PORT", "3306"))
DB_USER = env.get("DB_USER", "root")
DB_PASS = env.get("DB_PASS", "")

DB_IS_PRODUCTION = DB_NAME == "live_grapher"

# Special ENV file with access to production DB (read-only), used by chart-diff
ENV_FILE_PROD = os.environ.get("ENV_FILE_PROD")

if "DATA_API_ENV" in env:
    DATA_API_ENV = env["DATA_API_ENV"]
else:
    DATA_API_ENV = env.get("DATA_API_ENV", get_username())

# Production checks
if DATA_API_ENV == "production":
    assert DB_IS_PRODUCTION, "DB_NAME must be set to live_grapher when publishing to production"

if DB_IS_PRODUCTION:
    assert DATA_API_ENV == "production", "DATA_API_ENV must be set to production when publishing to live_grapher"


def load_STAGING() -> Optional[str]:
    # if STAGING is used, override ENV values
    STAGING = env.get("STAGING")

    # ENV_FILE takes precedence over STAGING
    if STAGING and ENV_FILE != BASE_DIR / ".env":
        log.warning("Both ENV_FILE and STAGING is set, STAGING will be ignored.")
        return None
    # if STAGING=1, use branch name
    elif STAGING == "1":
        branch_name = git.Repo(BASE_DIR).active_branch.name
        if branch_name == "master":
            log.warning("You're on master branch, using local env instead of STAGING=master")
            return None
        else:
            return branch_name
    else:
        return STAGING


STAGING = load_STAGING()

# if STAGING is used, override ENV values
if STAGING is not None:
    GRAPHER_USER_ID = 1  # use Admin user when working with staging
    DB_USER = "owid"
    DB_NAME = "owid"
    DB_PASS = ""
    DB_PORT = 3306
    DB_HOST = get_container_name(STAGING)
    DATA_API_ENV = get_container_name(STAGING)


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

# number of workers for checking dirty steps, we need to parallelize this
# because we're making a lot of HTTP requests
DIRTY_STEPS_WORKERS = int(env.get("DIRTY_STEPS_WORKERS", 5))

# default number of processes for running steps if not using --workers
# it is 1 by default because we usually can't run multiple steps in parallel in dev
RUN_STEPS_WORKERS = int(env.get("RUN_STEPS_WORKERS", 1))

# number of workers for grapher inserts to DB
# NOTE: make sure the product of run processes and grapher workers is constant
GRAPHER_INSERT_WORKERS = int(env.get("GRAPHER_WORKERS", max(10, int(40 / RUN_STEPS_WORKERS))))

# only upsert indicators matching this filter, this is useful for fast development
# of data pages for a single indicator
GRAPHER_FILTER = env.get("GRAPHER_FILTER", None)

# if set, don't delete indicators from MySQL, only append / update new ones
# you can use this to only process subset of indicators in your step to
# speed up development. It's up to you how you define filtering logic in your step
SUBSET = env.get("SUBSET", None)

# forbid any individual step from consuming more than this much memory
# (only enforced on Linux)
MAX_VIRTUAL_MEMORY_LINUX = 32 * 2**30  # 32 GB

# increment this to force a full rebuild of all datasets
ETL_EPOCH = 5

# any garden or grapher dataset after this date will have strict mode enabled
STRICT_AFTER = "2023-06-25"

SLACK_API_TOKEN = env.get("SLACK_API_TOKEN")

# if True, commit and push updates to YAML files coming from admin
ETL_API_COMMIT = env.get("ETL_API_COMMIT") in ("True", "true", "1")

# if True, commit and push updates from fasttrack
FASTTRACK_COMMIT = env.get("FASTTRACK_COMMIT") in ("True", "true", "1")

ADMIN_HOST = env.get("ADMIN_HOST", f"http://staging-site-{STAGING}" if STAGING else "http://localhost:3030")

# Tailscale address of Admin, this cannot be just `http://owid-admin-prod`
# because that would resolve to LXC container instead of the actual server
TAILSCALE_ADMIN_HOST = "http://owid-admin-prod.tail6e23.ts.net"

BUGSNAG_API_KEY = env.get("BUGSNAG_API_KEY")

OPENAI_API_KEY = env.get("OPENAI_API_KEY", None)

OWIDBOT_ACCESS_TOKEN = env.get("OWIDBOT_ACCESS_TOKEN", None)

# OWIDBOT app
OWIDBOT_APP_PRIVATE_KEY_PATH = env.get("OWIDBOT_APP_PRIVATE_KEY_PATH", None)
# get it from https://github.com/settings/apps/owidbot-app
OWIDBOT_APP_CLIENT_ID = env.get("OWIDBOT_APP_CLIENT_ID", None)
# get it from https://github.com/settings/installations
OWIDBOT_APP_INSTALLATION_ID = env.get("OWIDBOT_APP_INSTALLATION_ID", None)

# Load github token (only used for creating PRs from the command line).
GITHUB_TOKEN = env.get("GITHUB_TOKEN", None)


def enable_bugsnag() -> None:
    if BUGSNAG_API_KEY:
        bugsnag.configure(
            api_key=BUGSNAG_API_KEY,
        )  # type: ignore


# Wizard config
WIZARD_PORT = 8053


### WIP: OWIDENV

OWIDEnvType = Literal["production", "dev", "staging", "unknown"]


@dataclass
class Config:
    """Configuration for OWID environment which is a subset of etl.config."""

    GRAPHER_USER_ID: int | str | None
    DB_USER: str
    DB_NAME: str
    DB_PASS: str
    DB_PORT: str
    DB_HOST: str

    @classmethod
    def from_env_file(cls, env_file: str):
        env_dict = dotenv_values(env_file)
        config_dict = {}
        for field in fields(cls):
            if field.name in {"GRAPHER_USER_ID"}:
                config_dict[field.name] = env_dict.get(field.name)
            else:
                if field.name not in env_dict:
                    raise KeyError(f"Field {field.name} not found in env file {env_file}!")
                config_dict[field.name] = env_dict[field.name]
        return cls(**config_dict)  # type: ignore


class UnknownOWIDEnv(Exception):
    pass


class OWIDEnv:
    """OWID environment.

    TODO: maybe worth moving to etl.config
    """

    _env_remote: OWIDEnvType | None
    _env_local: OWIDEnvType | None
    conf: Config
    _engine: Engine | None

    def __init__(
        self,
        conf: Config,
    ) -> None:
        self.conf = conf
        # Remote environment: environment where the database is located
        self._env_remote = None
        # Local environment: environment where the code is running
        self._env_local = None  # "production", "staging", "dev"
        # Engine (cached)
        self._engine = None

    @property
    def env(self):
        """Environment one's in. Only works if remote and local environment are the same."""
        if self.env_remote == self.env_local:
            return self.env_remote
        raise ValueError(f"env_remote ({self.env_remote}) and env_local ({self.env_local}) are different.")

    @property
    def env_local(self):
        """Detect local environment."""
        if self._env_local is None:
            self._env_local = cast(OWIDEnvType, ENV)
        return self._env_local

    @property
    def env_remote(self):
        """Detect remote environment."""
        if self._env_remote is None:
            # production
            if self.conf.DB_NAME == "live_grapher":
                self._env_remote = "production"
            # local
            elif self.conf.DB_NAME == "grapher" and self.conf.DB_USER == "grapher":
                self._env_remote = "dev"
            # other
            elif self.conf.DB_NAME == "owid" and self.conf.DB_USER == "owid":
                self._env_remote = "staging"
            # unknown
            else:
                self._env_remote = "unknown"

        return self._env_remote

    @classmethod
    def from_local(cls):
        conf = Config(
            GRAPHER_USER_ID=1,
            DB_USER="owid",
            DB_NAME="owid",
            DB_PASS="",
            DB_PORT="3306",
            DB_HOST=DB_HOST,
        )
        return cls(conf)

    @classmethod
    def from_staging(cls, branch: str):
        """Create OWIDEnv for staging."""
        conf = Config(
            GRAPHER_USER_ID=1,
            DB_USER="owid",
            DB_NAME="owid",
            DB_PASS="",
            DB_PORT="3306",
            DB_HOST=get_container_name(branch),
        )
        return cls(conf)

    @classmethod
    def from_env_file(cls, env_file: str):
        """Create OWIDEnv from env file."""
        assert Path(env_file).exists(), f"ENV file {env_file} doesn't exist"
        return cls(conf=Config.from_env_file(env_file))

    @classmethod
    def from_staging_or_env_file(cls, staging_or_env_file: str):
        """Create OWIDEnv from staging or env file."""
        if Path(staging_or_env_file).exists():
            return cls.from_env_file(staging_or_env_file)
        return cls.from_staging(staging_or_env_file)

    def get_engine(self) -> Engine:
        """Get engine for env.

        DEPRECATED: Use property `engine` property instead.
        """
        from etl.db import get_engine

        return get_engine(self.conf.__dict__)

    @property
    def engine(self) -> Engine:
        """Get engine for env."""
        from etl.db import get_engine

        if self._engine is None:
            self._engine = get_engine(self.conf.__dict__)
        return self._engine

    @property
    def site(self) -> str | None:
        """Get site."""
        if self.env_remote == "production":
            return "https://ourworldindata.org"
        elif self.env_remote == "dev":
            return "http://localhost:3030"
        elif self.env_remote == "staging":
            return f"http://{self.conf.DB_HOST}"
        return None

    @property
    def name(self) -> str:
        """Get site."""
        if self.env_remote in {"production", "dev"}:
            return self.env_remote
        elif self.env_remote == "staging":
            return f"{self.conf.DB_HOST}"
        raise ValueError(f"Unknown env_remote (DB_NAME/DB_USER={self.conf.DB_NAME}/{self.conf.DB_USER})")

    @property
    def base_site(self) -> str | None:
        """Get site."""
        if self.env_remote == "production":
            return "https://admin.owid.io"
        elif self.env_remote in ["dev", "staging"]:
            return self.site
        return None

    @property
    def admin_site(
        self,
    ) -> str | None:
        """Get admin url."""
        if self.base_site:
            return f"{self.base_site}/admin"

    @property
    def data_api_url(self) -> str:
        """Get api url."""
        if self.env_remote == "production":
            return "https://api.ourworldindata.org"
        elif self.env_remote == "staging":
            return f"https://api-staging.owid.io/{self.conf.DB_HOST}"
        else:
            raise ValueError(f"Unknown DATA_API for env_remote={self.env_remote}")

    @property
    def chart_approval_tool_url(self) -> str:
        """Get chart approval tool url."""
        return f"{self.admin_site}/suggested-chart-revisions/review"

    @property
    def indicators_url(self) -> str:
        """Get indicators url."""
        return self.data_api_url + "/v1/indicators"

    @property
    def wizard_url(self) -> str:
        """Get wizard url."""
        if self.env_local == "dev":
            return f"http://localhost:{WIZARD_PORT}/"
        elif self.env_local == "production":
            return "https://etl.owid.io/wizard/"
        else:
            return f"{self.base_site}/etl/wizard"

    @property
    def wizard_url_remote(self) -> str:
        """Get wizard url (in remote server)."""
        if self.env_remote == "dev":
            return f"http://localhost:{WIZARD_PORT}/"
        elif self.env_remote == "production":
            return "https://etl.owid.io/wizard/"
        else:
            return f"{self.base_site}/etl/wizard"

    def dataset_admin_site(self, dataset_id: str | int) -> str:
        """Get dataset admin url."""
        return f"{self.admin_site}/datasets/{dataset_id}/"

    def variable_admin_site(self, variable_id: str | int) -> str:
        """Get variable admin url."""
        return f"{self.admin_site}/variables/{variable_id}/"

    def chart_admin_site(self, chart_id: str | int) -> str:
        """Get chart admin url."""
        return f"{self.admin_site}/charts/{chart_id}/edit"

    def chart_site(self, slug: str) -> str:
        """Get chart url."""
        return f"{self.site}/grapher/{slug}"

    def thumb_url(self, slug: str):
        """
        Turn https://ourworldindata.org/grapher/life-expectancy"
        Into https://ourworldindata.org/grapher/thumbnail/life-expectancy.png
        """
        return f"{self.site}/grapher/thumbnail/{slug}.png"

    def indicator_metadata_url(self, variable_id):
        return f"{self.indicators_url}/{variable_id}.metadata.json"

    def indicator_data_url(self, variable_id):
        return f"{self.indicators_url}/{variable_id}.data.json"


# Wrap envs in OWID_ENV
OWID_ENV = OWIDEnv(
    Config(
        GRAPHER_USER_ID=GRAPHER_USER_ID,
        DB_USER=DB_USER,
        DB_NAME=DB_NAME,
        DB_PASS=DB_PASS,
        DB_PORT=str(DB_PORT),
        DB_HOST=DB_HOST,
    )
)

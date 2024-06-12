"""Tools to handle OWID environment."""

from dataclasses import dataclass, fields
from pathlib import Path
from typing import Literal, cast

from dotenv import dotenv_values
from typing_extensions import Self

from apps.wizard.config import WIZARD_PORT
from etl import config
from etl.config import ENV, get_container_name
from etl.db import Engine, get_engine

OWIDEnvType = Literal["production", "dev", "staging", "unknown"]


@dataclass
class Config:
    """Configuration for OWID environment which is a subset of etl.config."""

    DB_USER: str
    DB_NAME: str
    DB_PASS: str
    DB_PORT: str
    DB_HOST: str

    @classmethod
    def from_env_file(cls, env_file: str) -> Self:
        env_dict = dotenv_values(env_file)
        config_dict = {field.name: env_dict[field.name] for field in fields(cls)}
        return cls(**config_dict)  # type: ignore


class UnknownOWIDEnv(Exception):
    pass


class OWIDEnv:
    """OWID environment."""

    _env_remote: OWIDEnvType | None
    _env_local: OWIDEnvType | None
    conf: Config
    _engine: Engine | None

    def __init__(
        self: Self,
        conf: Config | None = None,
    ) -> None:
        self.conf = conf or cast(Config, config)
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
    def from_staging(cls, branch: str) -> Self:
        """Create OWIDEnv for staging."""
        conf = Config(
            DB_USER="owid",
            DB_NAME="owid",
            DB_PASS="",
            DB_PORT="3306",
            DB_HOST=get_container_name(branch),
        )
        return cls(conf)

    @classmethod
    def from_env_file(cls, env_file: str) -> Self:
        """Create OWIDEnv from env file."""
        assert Path(env_file).exists(), f"ENV file {env_file} doesn't exist"
        return cls(conf=Config.from_env_file(env_file))

    @classmethod
    def from_staging_or_env_file(cls, staging_or_env_file: str) -> Self:
        """Create OWIDEnv from staging or env file."""
        if Path(staging_or_env_file).exists():
            return cls.from_env_file(staging_or_env_file)
        return cls.from_staging(staging_or_env_file)

    def get_engine(self) -> Engine:
        """Get engine for env.

        DEPRECATED: Use property `engine` property instead.
        """
        return get_engine(self.conf.__dict__)

    @property
    def engine(self) -> Engine:
        """Get engine for env."""
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
        self: Self,
    ) -> str | None:
        """Get admin url."""
        if self.base_site:
            return f"{self.base_site}/admin"

    @property
    def data_api_url(self: Self) -> str:
        """Get api url."""
        if self.env_remote == "production":
            return "https://api.ourworldindata.org"
        elif self.env_remote == "staging":
            return f"https://api-staging.owid.io/{self.conf.DB_HOST}"
        else:
            raise ValueError(f"Unknown DATA_API for env_remote={self.env_remote}")

    @property
    def chart_approval_tool_url(self: Self) -> str:
        """Get chart approval tool url."""
        return f"{self.admin_site}/suggested-chart-revisions/review"

    @property
    def indicators_url(self: Self) -> str:
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

    def dataset_admin_site(self: Self, dataset_id: str | int) -> str:
        """Get dataset admin url."""
        return f"{self.admin_site}/datasets/{dataset_id}/"

    def variable_admin_site(self: Self, variable_id: str | int) -> str:
        """Get variable admin url."""
        return f"{self.admin_site}/variables/{variable_id}/"

    def chart_admin_site(self: Self, chart_id: str | int) -> str:
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


OWID_ENV = OWIDEnv()

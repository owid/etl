"""Tools to handle OWID environment."""
import re
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Literal, Optional, cast

from dotenv import dotenv_values
from typing_extensions import Self

from etl import config
from etl.db import Engine, get_engine

OWIDEnvType = Literal["live", "local", "remote-staging", "unknown"]


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

    env_type_id: OWIDEnvType
    conf: Config

    def __init__(
        self: Self,
        conf: Config | None = None,
    ) -> None:
        self.conf = conf or cast(Config, config)
        self.env_type_id = self.detect_env_type()

    def detect_env_type(self: Self) -> OWIDEnvType:
        """Detect environment type."""
        # live
        if self.conf.DB_NAME == "live_grapher":
            return "live"
        # local
        elif self.conf.DB_NAME == "grapher" and self.conf.DB_USER == "grapher":
            return "local"
        # other
        elif self.conf.DB_NAME == "owid" and self.conf.DB_USER == "owid":
            return "remote-staging"
        return "unknown"

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
        """Get engine for env."""
        return get_engine(self.conf.__dict__)

    @property
    def site(self) -> str | None:
        """Get site."""
        if self.env_type_id == "live":
            return "https://ourworldindata.org"
        elif self.env_type_id == "local":
            return "http://localhost:3030"
        elif self.env_type_id == "remote-staging":
            return f"http://{self.conf.DB_HOST}"
        return None

    @property
    def name(self) -> str:
        """Get site."""
        if self.env_type_id == "live":
            return "production"
        elif self.env_type_id == "local":
            return "local"
        elif self.env_type_id == "remote-staging":
            return f"{self.conf.DB_HOST}"
        raise ValueError("Unknown env_type_id")

    @property
    def base_site(self) -> str | None:
        """Get site."""
        if self.env_type_id == "live":
            return "https://admin.owid.io"
        elif self.env_type_id in ["local", "remote-staging"]:
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
    def api_site(self: Self) -> str:
        """Get api url."""
        if self.env_type_id == "live":
            return "https://api.ourworldindata.org"
        elif self.env_type_id == "remote-staging":
            return f"https://api-staging.owid.io/{self.conf.DB_HOST}"
        elif self.env_type_id == "local":
            return "http://localhost:8000"
        else:
            raise UnknownOWIDEnv()

    @property
    def chart_approval_tool_url(self: Self) -> str:
        """Get chart approval tool url."""
        return f"{self.admin_site}/suggested-chart-revisions/review"

    @property
    def indicators_url(self: Self) -> str:
        """Get indicators url."""
        return self.api_site + "/v1/indicators/"

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


OWID_ENV = OWIDEnv()

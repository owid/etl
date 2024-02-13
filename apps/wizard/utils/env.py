"""Tools to handle OWID environment."""
from typing import Literal, Optional

from typing_extensions import Self

from etl import config

OWIDEnvType = Literal["live", "staging", "local", "remote-staging", "unknown"]


class OWIDEnv:
    """OWID environment."""

    env_type_id: OWIDEnvType

    def __init__(
        self: Self,
        env_type_id: Optional[OWIDEnvType] = None,
    ) -> None:
        if env_type_id is None:
            self.env_type_id = self.detect_env_type()
        else:
            self.env_type_id = env_type_id

    def detect_env_type(self: Self) -> OWIDEnvType:
        """Detect environment type."""
        # live
        if config.DB_NAME == "live_grapher":
            return "live"
        # staging
        elif config.DB_NAME == "staging_grapher" and config.DB_USER == "staging_grapher":
            return "staging"
        # local
        elif config.DB_NAME == "grapher" and config.DB_USER == "grapher":
            return "local"
        # other
        elif config.DB_NAME == "owid" and config.DB_USER == "owid":
            return "remote-staging"
        return "unknown"

    @property
    def site(self) -> str | None:
        """Get site."""
        if self.env_type_id == "live":
            return "ourworldindata.org"
        elif self.env_type_id == "staging":
            return "staging.ourworldindata.org"
        elif self.env_type_id == "local":
            return "localhost:3030"
        elif self.env_type_id == "remote-staging":
            return config.DB_HOST
        return None

    @property
    def base_site(self) -> str | None:
        """Get site."""
        if self.env_type_id == "live":
            return "owid.cloud"
        elif self.env_type_id == "staging":
            return "staging.owid.cloud"
        elif self.env_type_id == "local":
            return "localhost:3030"
        elif self.env_type_id == "remote-staging":
            return config.DB_HOST
        return None

    @property
    def admin_site(
        self: Self,
    ) -> str | None:
        """Get admin url."""
        if self.base_site:
            return f"{self.base_site}/admin"

    @property
    def chart_approval_tool_url(self: Self) -> str:
        """Get chart approval tool url."""
        return f"{self.admin_site}/suggested-chart-revisions/review"

    def dataset_admin_site(self: Self, dataset_id: str | int) -> str:
        """Get dataset admin url."""
        return f"{self.admin_site}/datasets/{dataset_id}/"

    def variable_admin_site(self: Self, variable_id: str | int) -> str:
        """Get variable admin url."""
        return f"{self.admin_site}/variables/{variable_id}/"

    def chart_admin_site(self: Self, chart_id: str | int) -> str:
        """Get chart admin url."""
        return f"{self.admin_site}/charts/{chart_id}/edit"

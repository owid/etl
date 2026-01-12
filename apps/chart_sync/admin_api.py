import json
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests
import structlog
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import HTTPError

from etl.config import ADMIN_API_KEY, DEFAULT_GRAPHER_SCHEMA, OWIDEnv

log = structlog.get_logger()


def is_502_error(exception):
    # Check if the exception is an HTTPError and if it's a 502 Bad Gateway error
    return isinstance(exception, HTTPError) and exception.response.status_code == 502


class AdminAPI(object):
    def __init__(self, owid_env: OWIDEnv, api_key: Optional[str] = ADMIN_API_KEY):
        self.owid_env = owid_env
        if not api_key:
            raise ValueError("ADMIN_API_KEY is required. Set it in .env or pass api_key parameter.")
        self.api_key = api_key

    def _headers(self, user_id: Optional[int] = None) -> Dict[str, str]:
        """Build headers for API requests."""
        headers = {"Authorization": f"Bearer {self.api_key}"}
        if user_id is not None:
            headers["X-Act-As-User-Id"] = str(user_id)
        return headers

    def _json_from_response(self, resp: requests.Response) -> dict:
        if resp.status_code != 200:
            log.error("Admin API error", status_code=resp.status_code, text=resp.text)
        resp.raise_for_status()
        try:
            js = resp.json()
        except (json.JSONDecodeError, requests.exceptions.JSONDecodeError) as e:
            raise AdminAPIError(resp.text) from e
        return js

    def get_chart_config(self, chart_id: int) -> dict:
        resp = requests.get(
            f"{self.owid_env.admin_api}/charts/{chart_id}.config.json",
            headers=self._headers(),
        )
        js = self._json_from_response(resp)
        return js

    def get_chart_references(self, chart_id: int) -> dict:
        resp = requests.get(
            f"{self.owid_env.admin_api}/charts/{chart_id}.references.json",
            headers=self._headers(),
        )
        js = self._json_from_response(resp)
        return js

    def create_chart(self, chart_config: dict, user_id: Optional[int] = None) -> dict:
        # Extract isInheritanceEnabled from config and prepare request params
        config = chart_config.copy()
        is_inheritance_enabled = config.pop("isInheritanceEnabled", None)

        # Build request parameters
        params = {}
        if is_inheritance_enabled is not None:
            inheritance_param = "enable" if is_inheritance_enabled else "disable"
            params["inheritance"] = inheritance_param

        resp = requests.post(
            self.owid_env.admin_api + "/charts",
            headers=self._headers(user_id),
            json=config,
            params=params,
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError({"error": js["error"], "chart_config": chart_config})
        return js

    def update_chart(self, chart_id: int, chart_config: dict, user_id: Optional[int] = None) -> dict:
        # Extract isInheritanceEnabled from config and prepare request params
        config = chart_config.copy()
        is_inheritance_enabled = config.pop("isInheritanceEnabled", None)

        # Build request parameters
        params = {}
        if is_inheritance_enabled is not None:
            inheritance_param = "enable" if is_inheritance_enabled else "disable"
            params["inheritance"] = inheritance_param

        resp = requests.put(
            f"{self.owid_env.admin_api}/charts/{chart_id}",
            headers=self._headers(user_id),
            json=config,
            params=params,
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError({"error": js["error"], "chart_config": chart_config})
        return js

    def set_tags(self, chart_id: int, tags: List[Dict[str, Any]], user_id: Optional[int] = None) -> dict:
        resp = requests.post(
            f"{self.owid_env.admin_api}/charts/{chart_id}/setTags",
            headers=self._headers(user_id),
            json={"tags": tags},
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError({"error": js["error"], "tags": tags})
        return js

    def put_grapher_config(self, variable_id: int, grapher_config: Dict[str, Any]) -> dict:
        # If schema is missing, use the default one
        grapher_config.setdefault("$schema", DEFAULT_GRAPHER_SCHEMA)

        # Retry in case we're restarting Admin on staging server
        resp = requests_with_retry().put(
            self.owid_env.admin_api + f"/variables/{variable_id}/grapherConfigETL",
            headers=self._headers(),
            json=grapher_config,
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError({"error": js["error"], "variable_id": variable_id, "grapher_config": grapher_config})
        return js

    def delete_grapher_config(self, variable_id: int) -> dict:
        resp = requests.delete(
            self.owid_env.admin_api + f"/variables/{variable_id}/grapherConfigETL",
            headers=self._headers(),
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError({"error": js["error"], "variable_id": variable_id})
        return js

    def put_mdim_config(self, mdim_catalog_path: str, mdim_config: dict, user_id: Optional[int] = None) -> dict:
        # Retry in case we're restarting Admin on staging server
        url = self.owid_env.admin_api + f"/multi-dims/{quote(mdim_catalog_path, safe='')}"
        resp = requests_with_retry().put(
            url,
            headers=self._headers(user_id),
            json={"config": mdim_config},
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError(
                {"error": js["error"], "mdim_catalog_path": mdim_catalog_path, "mdim_config": mdim_config}
            )
        return js

    def put_explorer_config(self, slug: str, tsv: str, user_id: Optional[int] = None) -> dict:
        # Retry in case we're restarting Admin on staging server
        url = self.owid_env.admin_api + f"/explorers/{slug}"
        resp = requests_with_retry().put(
            url,
            headers=self._headers(user_id),
            json={"tsv": tsv, "commitMessage": "Update explorer from ETL"},
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError({"error": js["error"], "slug": slug, "tsv": tsv[:1000]})
        return js

    def create_dod(self, name: str, content: str, user_id: int | None = None) -> Dict[str, Any]:
        """Create a new DoD (Details on Demand)."""
        data = {
            "name": name,
            "content": content,
        }
        resp = requests.post(
            f"{self.owid_env.admin_api}/dods",
            headers=self._headers(user_id),
            json=data,
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError({"error": js["error"], "dod_data": data})
        return js

    def update_dod(self, dod_id: int, content: str, user_id: int | None = None) -> Dict[str, Any]:
        """Update an existing DoD."""
        data = {
            "content": content,
        }
        resp = requests.patch(
            f"{self.owid_env.admin_api}/dods/{dod_id}",
            headers=self._headers(user_id),
            json=data,
        )
        js = self._json_from_response(resp)
        # NOTE: update DoD doesn't return `success`, but {dod: 1} (which is wrong, it should return DoD id)
        # if not js["success"]:
        #     raise AdminAPIError({"error": js["error"], "dod_data": data})
        return js

    def get_narrative_chart(self, narrative_chart_id: int) -> dict:
        """Get a narrative chart by ID."""
        resp = requests.get(
            f"{self.owid_env.admin_api}/narrative-charts/{narrative_chart_id}.config.json",
            headers=self._headers(),
        )
        js = self._json_from_response(resp)
        return js

    def update_narrative_chart(self, narrative_chart_id: int, config: dict, user_id: Optional[int] = None) -> dict:
        """Update a narrative chart's config.

        Args:
            narrative_chart_id: The ID of the narrative chart to update
            config: The updated patch config for the narrative chart
            user_id: Optional user ID for the session

        Returns:
            Response dict from the API
        """
        resp = requests.put(
            f"{self.owid_env.admin_api}/narrative-charts/{narrative_chart_id}",
            headers=self._headers(user_id),
            json={"config": config},
        )
        js = self._json_from_response(resp)
        if not js.get("success", True):  # Some endpoints don't return success
            raise AdminAPIError({"error": js.get("error"), "narrative_chart_id": narrative_chart_id, "config": config})
        return js


def requests_with_retry() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 504])
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


class AdminAPIError(Exception):
    pass

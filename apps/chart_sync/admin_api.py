import json
from functools import cache
from typing import Any
from urllib.parse import quote

import requests
import structlog
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import HTTPError

from etl.config import ADMIN_API_KEY, DEFAULT_GRAPHER_SCHEMA, GRAPHER_USER_ID, OWIDEnv
from etl.http import USER_AGENT
from etl.http import session as http_session

log = structlog.get_logger()

# (connect, read) timeout for every call in this module. `requests` waits forever without
# one, so an admin server that accepts the connection and then stops responding blocks the
# calling thread indefinitely. On a full staging build that surfaced as two grapher upsert
# steps that never returned: the run went silent after its last step and was only killed by
# Buildkite's timeout 2.5 hours later. A bounded wait turns that into a step that fails.
TIMEOUT = (10, 120)


def is_502_error(exception):
    # Check if the exception is an HTTPError and if it's a 502 Bad Gateway error
    return isinstance(exception, HTTPError) and exception.response.status_code == 502


class AdminAPI:
    def __init__(self, owid_env: OWIDEnv, api_key: str | None = ADMIN_API_KEY):
        self.owid_env = owid_env
        self.api_key = api_key

    def _headers(self, user_id: int | None = None) -> dict[str, str]:
        """Build headers for API requests."""
        headers: dict[str, str] = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        if user_id is not None:
            headers["x-act-as-user"] = str(user_id)
        return headers

    def _raise_for_response(self, resp: requests.Response) -> None:
        """Log and raise on a failed response. Split out for the routes that answer without a body."""
        if resp.status_code != 200:
            log.error("Admin API error", status_code=resp.status_code, text=resp.text)
        if resp.status_code == 401 and not self.api_key:
            user_id_hint = f" --userId {GRAPHER_USER_ID}" if GRAPHER_USER_ID else " --userId <YOUR_USER_ID>"
            raise AdminAPIError(
                "Unauthorized: ADMIN_API_KEY is required. Set it in .env.\n"
                f'Generate it with: ssh owid@owid-admin-prod "cd ~/owid-grapher && yarn createAdminApiKey{user_id_hint}"'
            )
        resp.raise_for_status()

    def _json_from_response(self, resp: requests.Response) -> dict:
        self._raise_for_response(resp)
        try:
            js = resp.json()
        except (json.JSONDecodeError, requests.exceptions.JSONDecodeError) as e:
            raise AdminAPIError(resp.text) from e
        return js

    def get_chart_config(self, chart_id: int) -> dict:
        resp = http_session.get(
            f"{self.owid_env.admin_api}/charts/{chart_id}.config.json",
            headers=self._headers(),
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        return js

    def get_chart_references(self, chart_id: int) -> dict:
        resp = http_session.get(
            f"{self.owid_env.admin_api}/charts/{chart_id}.references.json",
            headers=self._headers(),
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        return js

    def create_chart(self, chart_config: dict, user_id: int | None = None) -> dict:
        # Extract chart-table fields; keep them out of chart_configs.full payload.
        config = chart_config.copy()
        is_inheritance_enabled = config.pop("isInheritanceEnabled", None)
        config.pop("forceDatapage", None)

        # Build request parameters
        params = {}
        if is_inheritance_enabled is not None:
            inheritance_param = "enable" if is_inheritance_enabled else "disable"
            params["inheritance"] = inheritance_param

        resp = http_session.post(
            self.owid_env.admin_api + "/charts",
            headers=self._headers(user_id),
            json=config,
            params=params,
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError({"error": js["error"], "chart_config": chart_config})
        return js

    def update_chart(self, chart_id: int, chart_config: dict, user_id: int | None = None) -> dict:
        # Extract chart-table fields; keep them out of chart_configs.full payload.
        config = chart_config.copy()
        is_inheritance_enabled = config.pop("isInheritanceEnabled", None)
        config.pop("forceDatapage", None)

        # Build request parameters
        params = {}
        if is_inheritance_enabled is not None:
            inheritance_param = "enable" if is_inheritance_enabled else "disable"
            params["inheritance"] = inheritance_param

        resp = http_session.put(
            f"{self.owid_env.admin_api}/charts/{chart_id}",
            headers=self._headers(user_id),
            json=config,
            params=params,
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError({"error": js["error"], "chart_config": chart_config})
        return js

    def set_tags(self, chart_id: int, tags: list[dict[str, Any]], user_id: int | None = None) -> dict:
        resp = http_session.post(
            f"{self.owid_env.admin_api}/charts/{chart_id}/setTags",
            headers=self._headers(user_id),
            json={"tags": tags},
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError({"error": js["error"], "tags": tags})
        return js

    def create_site_redirect(self, source: str, target: str, user_id: int | None = None) -> dict:
        """Create a site-wide URL redirect (redirects table).

        For arbitrary paths, including wildcards — not just charts. It bakes into the static
        `_redirects` file as an unconditional 301 that matches before the grapher route runs,
        so it also shadows any chart redirect on the same source.

        For a chart -> chart redirect prefer `create_chart_redirect`: the alias then shows up in
        the target chart's editor, and since grapher #6674 it carries a query string too.
        """
        resp = http_session.post(
            f"{self.owid_env.admin_api}/site-redirects/new",
            headers=self._headers(user_id),
            json={"source": source, "target": target},
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        if not js.get("success"):
            raise AdminAPIError({"error": js.get("error"), "source": source, "target": target})
        return js

    def delete_site_redirect(self, redirect_id: int, user_id: int | None = None) -> dict:
        """Delete a site-wide URL redirect by id (there is no update endpoint, so
        callers change a target by deleting then re-creating)."""
        resp = http_session.delete(
            f"{self.owid_env.admin_api}/site-redirects/{redirect_id}",
            headers=self._headers(user_id),
            timeout=TIMEOUT,
        )
        return self._json_from_response(resp)

    def create_chart_redirect(
        self,
        chart_id: int,
        slug: str,
        target_query_param: str | None = None,
        user_id: int | None = None,
    ) -> dict:
        """Point an old slug at a chart (chart_slug_redirects).

        The API behind the chart editor's "Alternative URLs for this chart". `chart_id` is the
        TARGET chart; `slug` is the old, bare slug (no "/grapher/", no leading slash).
        `target_query_param` is a query string without the leading "?", e.g.
        "tab=scatter&time=latest" — the server trims it and stores an empty string as NULL.

        The redirect is consulted only when /grapher/<slug> returns a 404, so the chart that
        owns the slug has to be unpublished for it to fire. The stored params are only a base:
        the visitor's own query params override them key by key.

        Two asymmetries with `create_site_redirect`, both left to the caller: this endpoint
        validates nothing (a duplicate slug comes back as a raw MySQL unique-key error rather
        than a JsonError, and chains are not rejected), and it does not trigger a static build,
        so the row stays unbaked until some other mutation triggers one.
        """
        payload: dict[str, Any] = {"slug": slug}
        if target_query_param is not None:
            payload["targetQueryParam"] = target_query_param
        resp = http_session.post(
            f"{self.owid_env.admin_api}/charts/{chart_id}/redirects/new",
            headers=self._headers(user_id),
            json=payload,
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        if not js.get("success"):
            raise AdminAPIError(
                {
                    "error": js.get("error"),
                    "chart_id": chart_id,
                    "slug": slug,
                    "target_query_param": target_query_param,
                }
            )
        return js

    def get_chart_redirects(self, chart_id: int) -> list[dict]:
        """Old slugs pointing AT this chart: [{id, slug, chartId, targetQueryParam}].

        These are inbound aliases, and unpublishing a chart deletes every one of them
        ("Unpublishing chart, delete any existing redirects to it" in the grapher admin), so
        read them before an unpublish if they have to survive it.
        """
        resp = http_session.get(
            f"{self.owid_env.admin_api}/charts/{chart_id}.redirects.json",
            headers=self._headers(),
            timeout=TIMEOUT,
        )
        return self._json_from_response(resp).get("redirects", [])

    def delete_chart_redirect(self, redirect_id: int, user_id: int | None = None) -> dict:
        """Delete a chart redirect by id.

        Note the asymmetric paths: creating one is /charts/{chart_id}/redirects/new, deleting it
        is /redirects/{id}. There is no update endpoint, so callers change a target_query_param
        by deleting and re-creating. Unlike the create, this does trigger a static build.
        """
        resp = http_session.delete(
            f"{self.owid_env.admin_api}/redirects/{redirect_id}",
            headers=self._headers(user_id),
            timeout=TIMEOUT,
        )
        return self._json_from_response(resp)

    def trigger_static_build(self) -> None:
        """Enqueue a static build — the admin's "Manually triggered deploy".

        Most mutating routes trigger one themselves, but a few don't: `create_chart_redirect` is the
        notable one, so a redirect written that way does not reach the baked redirect map (and so
        does not serve) until some unrelated mutation happens to bake the site. Call this when a run
        might not have triggered a build any other way. The deploy queue coalesces changes, so
        calling it alongside a mutation that already triggered one costs nothing.

        The route answers with an empty body, hence no return value and no JSON parsing.
        """
        resp = http_session.put(
            f"{self.owid_env.admin_api}/deploy",
            headers=self._headers(),
            timeout=TIMEOUT,
        )
        self._raise_for_response(resp)

    def put_grapher_config(self, variable_id: int, grapher_config: dict[str, Any]) -> dict:
        # If schema is missing, use the default one
        grapher_config.setdefault("$schema", DEFAULT_GRAPHER_SCHEMA)

        # Retry in case we're restarting Admin on staging server
        resp = requests_with_retry().put(
            self.owid_env.admin_api + f"/variables/{variable_id}/grapherConfigETL",
            headers=self._headers(),
            json=grapher_config,
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError({"error": js["error"], "variable_id": variable_id, "grapher_config": grapher_config})
        return js

    def delete_grapher_config(self, variable_id: int) -> dict:
        resp = http_session.delete(
            self.owid_env.admin_api + f"/variables/{variable_id}/grapherConfigETL",
            headers=self._headers(),
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError({"error": js["error"], "variable_id": variable_id})
        return js

    def put_mdim_config(self, mdim_catalog_path: str, mdim_config: dict, user_id: int | None = None) -> dict:
        # Retry in case we're restarting Admin on staging server
        url = self.owid_env.admin_api + f"/multi-dims/{quote(mdim_catalog_path, safe='')}"
        resp = requests_with_retry().put(
            url,
            headers=self._headers(user_id),
            json={"config": mdim_config},
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError(
                {"error": js["error"], "mdim_catalog_path": mdim_catalog_path, "mdim_config": mdim_config}
            )
        return js

    def put_explorer_config(self, slug: str, tsv: str, user_id: int | None = None) -> dict:
        # Retry in case we're restarting Admin on staging server
        url = self.owid_env.admin_api + f"/explorers/{slug}"
        resp = requests_with_retry().put(
            url,
            headers=self._headers(user_id),
            json={"tsv": tsv, "commitMessage": "Update explorer from ETL"},
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError({"error": js["error"], "slug": slug, "tsv": tsv[:1000]})
        return js

    def create_dod(self, name: str, content: str, user_id: int | None = None) -> dict[str, Any]:
        """Create a new DoD (Details on Demand)."""
        data = {
            "name": name,
            "content": content,
        }
        resp = http_session.post(
            f"{self.owid_env.admin_api}/dods",
            headers=self._headers(user_id),
            json=data,
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError({"error": js["error"], "dod_data": data})
        return js

    def update_dod(self, dod_id: int, content: str, user_id: int | None = None) -> dict[str, Any]:
        """Update an existing DoD."""
        data = {
            "content": content,
        }
        resp = http_session.patch(
            f"{self.owid_env.admin_api}/dods/{dod_id}",
            headers=self._headers(user_id),
            json=data,
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        # NOTE: update DoD doesn't return `success`, but {dod: 1} (which is wrong, it should return DoD id)
        # if not js["success"]:
        #     raise AdminAPIError({"error": js["error"], "dod_data": data})
        return js

    def get_narrative_chart(self, narrative_chart_id: int) -> dict:
        """Get a narrative chart by ID."""
        resp = http_session.get(
            f"{self.owid_env.admin_api}/narrative-charts/{narrative_chart_id}.config.json",
            headers=self._headers(),
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        return js

    def update_narrative_chart(self, narrative_chart_id: int, config: dict, user_id: int | None = None) -> dict:
        """Update a narrative chart's config.

        Args:
            narrative_chart_id: The ID of the narrative chart to update
            config: The updated patch config for the narrative chart
            user_id: Optional user ID for the session

        Returns:
            Response dict from the API
        """
        resp = http_session.put(
            f"{self.owid_env.admin_api}/narrative-charts/{narrative_chart_id}",
            headers=self._headers(user_id),
            json={"config": config},
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        if not js.get("success", True):  # Some endpoints don't return success
            raise AdminAPIError({"error": js.get("error"), "narrative_chart_id": narrative_chart_id, "config": config})
        return js

    def delete_variables(self, variable_ids: list[int]) -> dict:
        """Delete variables, leaving alone any a chart still uses.

        Grapher owns the delete because it owns the schema around it: the tables holding a
        `variableId` foreign key, and the `chart_configs` rows (and their R2 objects) that a
        variable leaves behind. It deletes what it safely can and reports the rest back;
        deciding whether a variable still used by a chart should fail the run is left to the
        caller, which is why nothing here raises on `blocked`.

        Args:
            variable_ids: The variables to remove

        Returns:
            {"deleted": [variable_id],
             "blocked": [{"variableId", "variableName", "chartId", "chartSlug"}]}
        """
        # Retry in case we're restarting Admin on staging server. This is idempotent — a
        # variable already gone stays gone — so repeating it is safe.
        resp = requests_with_retry().post(
            f"{self.owid_env.admin_api}/variables/delete",
            headers=self._headers(),
            json={"variableIds": variable_ids},
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        if not js.get("success", True):
            raise AdminAPIError({"error": js.get("error"), "variable_ids": variable_ids})
        return js

    def set_dataset_archived(self, dataset_id: int, is_archived: bool, user_id: int | None = None) -> dict:
        """Set the archived status of a dataset.

        Args:
            dataset_id: The ID of the dataset to archive/unarchive
            is_archived: Whether to archive (True) or unarchive (False) the dataset
            user_id: Optional user ID for the session

        Returns:
            Response dict from the API
        """
        resp = http_session.post(
            f"{self.owid_env.admin_api}/datasets/{dataset_id}/setArchived",
            headers=self._headers(user_id),
            json={"isArchived": is_archived},
            timeout=TIMEOUT,
        )
        js = self._json_from_response(resp)
        if not js.get("success", True):
            raise AdminAPIError({"error": js.get("error"), "dataset_id": dataset_id, "is_archived": is_archived})
        return js


@cache
def requests_with_retry() -> requests.Session:
    """Session for admin calls that should survive a restarting staging server.

    Cached, so callers share one connection pool. Building a session per call opened a
    fresh connection for every request, which on a dataset like world_bank_pip means tens
    of thousands of them in a single step.
    """
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    # 401 is included because staging's admin API can transiently reject a valid key while
    # grapher-build's DB migrations run concurrently with this build (see owid/ops#540).
    # `read=1` keeps a hung server from multiplying TIMEOUT by the full retry budget; the
    # status retries are the ones worth spending.
    # POST is not retryable by default because urllib3 can't know whether a POST is safe to
    # repeat. Ours are: only route an idempotent POST through this session.
    retries = Retry(
        total=5,
        read=1,
        backoff_factor=1,
        status_forcelist=[401, 500, 502, 503, 504],
        allowed_methods=Retry.DEFAULT_ALLOWED_METHODS | {"POST"},
    )
    # One adapter for both schemes: pool_maxsize covers the upsert thread pool that calls
    # put_grapher_config concurrently, so threads don't discard each other's connections.
    adapter = HTTPAdapter(max_retries=retries, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


class AdminAPIError(Exception):
    pass

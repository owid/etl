import base64
import datetime as dt
import json
import random
import string
from functools import cache
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests
import structlog
from requests.adapters import HTTPAdapter, Retry
from requests.exceptions import HTTPError
from sqlalchemy import text
from sqlalchemy.orm import Session

from etl.config import DEFAULT_GRAPHER_SCHEMA, ENV_GRAPHER_USER_ID, OWIDEnv
from etl.grapher import model as gm

log = structlog.get_logger()


def is_502_error(exception):
    # Check if the exception is an HTTPError and if it's a 502 Bad Gateway error
    return isinstance(exception, HTTPError) and exception.response.status_code == 502


class AdminAPI(object):
    def __init__(self, owid_env: OWIDEnv, grapher_user_id: Optional[int] = ENV_GRAPHER_USER_ID):
        self.owid_env = owid_env
        self.session_id = create_session_id(owid_env, grapher_user_id)

    def _json_from_response(self, resp: requests.Response) -> dict:
        if resp.status_code != 200:
            log.error("Admin API error", status_code=resp.status_code, text=resp.text)
        resp.raise_for_status()
        try:
            js = resp.json()
        except (json.JSONDecodeError, requests.exceptions.JSONDecodeError) as e:
            raise AdminAPIError(resp.text) from e
        return js

    def _get_session_id(self, user_id: Optional[int] = None) -> str:
        """Return the session id for the given user, or the default session id."""
        if user_id:
            return create_session_id(self.owid_env, user_id)
        return self.session_id

    def get_chart_config(self, chart_id: int) -> dict:
        resp = requests.get(
            f"{self.owid_env.admin_api}/charts/{chart_id}.config.json",
            cookies={"sessionid": self.session_id},
        )
        js = self._json_from_response(resp)
        return js

    def get_chart_references(self, chart_id: int) -> dict:
        resp = requests.get(
            f"{self.owid_env.admin_api}/charts/{chart_id}.references.json",
            cookies={"sessionid": self.session_id},
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
            cookies={"sessionid": self._get_session_id(user_id)},
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
            cookies={"sessionid": self._get_session_id(user_id)},
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
            cookies={"sessionid": self._get_session_id(user_id)},
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
            cookies={"sessionid": self.session_id},
            json=grapher_config,
        )
        js = self._json_from_response(resp)
        if not js["success"]:
            raise AdminAPIError({"error": js["error"], "variable_id": variable_id, "grapher_config": grapher_config})
        return js

    def delete_grapher_config(self, variable_id: int) -> dict:
        resp = requests.delete(
            self.owid_env.admin_api + f"/variables/{variable_id}/grapherConfigETL",
            cookies={"sessionid": self.session_id},
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
            cookies={"sessionid": self._get_session_id(user_id)},
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
            cookies={"sessionid": self._get_session_id(user_id)},
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
            cookies={"sessionid": self._get_session_id(user_id)},
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
            cookies={"sessionid": self._get_session_id(user_id)},
            json=data,
        )
        js = self._json_from_response(resp)
        # NOTE: update DoD doesn't return `success`, but {dod: 1} (which is wrong, it should return DoD id)
        # if not js["success"]:
        #     raise AdminAPIError({"error": js["error"], "dod_data": data})
        return js


@cache
def create_session_id(owid_env: OWIDEnv, grapher_user_id: int) -> str:
    engine = owid_env.get_engine()
    with Session(engine) as session:
        user = session.get(gm.User, grapher_user_id)
        # User is not in a database, use GRAPHER_USER_ID from .env
        if not user:
            user = session.get(gm.User, ENV_GRAPHER_USER_ID)
            assert user, f"User with id {ENV_GRAPHER_USER_ID} not found in the database. Initially tried to use user with id {grapher_user_id}."
        session_id = _create_user_session(session, user.email)
        session.commit()

    return session_id


def requests_with_retry() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502])
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


def _generate_random_string(length=32) -> str:
    letters_and_digits = string.ascii_letters + string.digits
    result_str = "".join(random.choice(letters_and_digits) for i in range(length))
    return result_str


def _create_user_session(session: Session, user_email: str, expiration_seconds=3600) -> str:
    """Create a new short-lived session for given user and return its session id."""
    # Generate a random string
    session_key = _generate_random_string()

    json_str = json.dumps({"user_email": user_email})

    # Base64 encode
    session_data = base64.b64encode(("prefix:" + json_str).encode("utf-8")).decode("utf-8")

    query = text(
        """
        INSERT INTO sessions (session_key, session_data, expire_date)
        VALUES (:session_key, :session_data, :expire_date);
    """
    )
    session.execute(
        query,
        params={
            "session_key": session_key,
            "session_data": session_data,
            "expire_date": dt.datetime.utcnow() + dt.timedelta(seconds=expiration_seconds),
        },
    )

    return session_key


class AdminAPIError(Exception):
    pass

import base64
import datetime as dt
import json
import random
import string
from typing import Any, Dict, List, Optional

import requests
import structlog
from sqlalchemy import text
from sqlalchemy.orm import Session

from etl import grapher_model as gm
from etl.config import GRAPHER_USER_ID, TAILSCALE_ADMIN_HOST
from etl.db import Engine

log = structlog.get_logger()


class AdminAPI(object):
    def __init__(self, engine: Engine, grapher_user_id: Optional[int] = None):
        with Session(engine) as session:
            if grapher_user_id:
                user = session.get(gm.User, grapher_user_id)
            else:
                user = session.get(gm.User, GRAPHER_USER_ID)
            assert user
            self.session_id = _create_user_session(session, user.email)
            session.commit()

        if engine.url.database == "live_grapher" and "prod-db" in str(engine.url.host):
            self.base_url = TAILSCALE_ADMIN_HOST
        else:
            self.base_url = f"http://{engine.url.host}.tail6e23.ts.net"

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
            f"{self.base_url}/admin/api/charts/{chart_id}.config.json",
            cookies={"sessionid": self.session_id},
        )
        js = self._json_from_response(resp)
        return js

    def create_chart(self, chart_config: dict) -> dict:
        resp = requests.post(
            self.base_url + "/admin/api/charts",
            cookies={"sessionid": self.session_id},
            json=chart_config,
        )
        js = self._json_from_response(resp)
        assert js["success"]
        return js

    def update_chart(self, chart_id: int, chart_config: dict) -> dict:
        resp = requests.put(
            f"{self.base_url}/admin/api/charts/{chart_id}",
            cookies={"sessionid": self.session_id},
            json=chart_config,
        )
        js = self._json_from_response(resp)
        assert js["success"]
        return js

    def set_tags(self, chart_id: int, tags: List[Dict[str, Any]]) -> dict:
        resp = requests.post(
            f"{self.base_url}/admin/api/charts/{chart_id}/setTags",
            cookies={"sessionid": self.session_id},
            json={"tags": tags},
        )
        js = self._json_from_response(resp)
        assert js["success"]
        return js

    def put_grapher_config(self, variable_id: int, grapher_config: Dict[str, Any]) -> dict:
        resp = requests.put(
            self.base_url + f"/admin/api/variables/{variable_id}/grapherConfigETL",
            cookies={"sessionid": self.session_id},
            json=grapher_config,
        )
        js = self._json_from_response(resp)
        assert js["success"]
        return js

    def delete_grapher_config(self, variable_id: int) -> dict:
        resp = requests.delete(
            self.base_url + f"/admin/api/variables/{variable_id}/grapherConfigETL",
            cookies={"sessionid": self.session_id},
        )
        js = self._json_from_response(resp)
        assert js["success"]
        return js


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

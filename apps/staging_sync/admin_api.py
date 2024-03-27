import base64
import datetime as dt
import json
import random
import string
from typing import Any, Dict, List

import requests
from sqlmodel import Session

from etl import grapher_model as gm
from etl.config import GRAPHER_USER_ID
from etl.db import Engine


class AdminAPI(object):
    def __init__(self, engine: Engine):
        with Session(engine) as session:
            user = session.get(gm.User, GRAPHER_USER_ID)
            assert user
            self.session_id = _create_user_session(session, user.email)
            session.commit()

        if engine.url.database == "live_grapher" and "prod-db" in str(engine.url.host):
            self.base_url = "https://owid.cloud"
        else:
            self.base_url = f"http://{engine.url.host}"

    def get_chart_config(self, chart_id: int) -> dict:
        resp = requests.get(
            f"{self.base_url}/admin/api/charts/{chart_id}.config.json",
            cookies={"sessionid": self.session_id},
        )
        resp.raise_for_status()
        return resp.json()

    def create_chart(self, chart_config: dict) -> dict:
        resp = requests.post(
            self.base_url + "/admin/api/charts",
            cookies={"sessionid": self.session_id},
            json=chart_config,
        )
        resp.raise_for_status()
        js = resp.json()
        assert js["success"]
        return js

    def update_chart(self, chart_id: int, chart_config: dict) -> dict:
        resp = requests.put(
            f"{self.base_url}/admin/api/charts/{chart_id}",
            cookies={"sessionid": self.session_id},
            json=chart_config,
        )
        resp.raise_for_status()
        js = resp.json()
        assert js["success"]
        return js

    def set_tags(self, chart_id: int, tags: List[Dict[str, Any]]) -> dict:
        resp = requests.post(
            f"{self.base_url}/admin/api/charts/{chart_id}/setTags",
            cookies={"sessionid": self.session_id},
            json={"tags": tags},
        )
        resp.raise_for_status()
        js = resp.json()
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

    query = """
        INSERT INTO sessions (session_key, session_data, expire_date)
        VALUES (:session_key, :session_data, :expire_date);
    """
    session.execute(
        query,  # type: ignore
        params={
            "session_key": session_key,
            "session_data": session_data,
            "expire_date": dt.datetime.utcnow() + dt.timedelta(seconds=expiration_seconds),
        },
    )

    return session_key

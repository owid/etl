import os
from contextlib import contextmanager

import pytest
from streamlit.testing.v1 import AppTest
from streamlit.testing.v1.element_tree import Button

from apps.wizard.utils import WIZARD_DIR
from etl import config

DEFAULT_TIMEOUT = 20


@contextmanager
def temporary_config(env_config):
    original_config = config.OWID_ENV
    config.OWID_ENV = env_config
    try:
        yield
    finally:
        config.OWID_ENV = original_config


@pytest.fixture
def set_config():
    """Connect to staging MySQL if running in Buildkite. Otherwise, connect to local MySQL.

    In case we want to create a dedicated database, use the following:
    # download DB
    # curl -Lo playground/owid_metadata.sql.gz https://files.ourworldindata.org/owid_metadata.sql.gz
    # create new database
    # mysql -h 127.0.0.1 -u root --port 3306 -p"owid" -D "" -e "CREATE DATABASE test_owid_123;"
    # fill it with data
    # cat playground/owid_metadata.sql.gz | gunzip | mysql -h 127.0.0.1 -u root --port 3306 -p"owid" -D "test_owid_123"
    """
    if "BUILDKITE_BRANCH" in os.environ:
        branch_name = os.environ["BUILDKITE_BRANCH"]
        env_config = config.OWIDEnv(
            config.Config(
                GRAPHER_USER_ID=1,
                DB_USER="owid",
                DB_NAME="owid",
                DB_PASS="",
                DB_PORT="3306",
                DB_HOST=config.get_container_name(branch_name),
            )
        )
        with temporary_config(env_config):
            yield
    else:
        # running locally, connect to DB specified in .env
        yield


def _pick_button_by_label(at: AppTest, label: str) -> Button:
    buttons = [button for button in at.button if button.label == label]
    if len(buttons) > 1:
        raise ValueError(f"Multiple buttons with label '{label}' found.")
    elif len(buttons) == 0:
        raise ValueError(f"Button with label '{label}' not found.")
    return buttons[0]


@pytest.mark.integration
@pytest.mark.usefixtures("set_config")
def test_app_chart_diff():
    """Ensure that chart-diff doesn't raise any errors on start."""
    at = AppTest.from_file(str(WIZARD_DIR / "app_pages/chart_diff/app.py"), default_timeout=DEFAULT_TIMEOUT).run()
    # allowed exceptions from migration of chart configs
    if at.exception:
        if (
            "(pymysql.err.ProgrammingError) (1146, \"Table 'live_grapher.chart_configs' doesn't exist\")"
            in at.exception[0].message
        ):
            return
    assert not at.exception


@pytest.mark.integration
@pytest.mark.usefixtures("set_config")
def test_app_indicator_upgrade():
    at = AppTest.from_file(
        str(WIZARD_DIR / "app_pages/indicator_upgrade/app.py"), default_timeout=DEFAULT_TIMEOUT
    ).run()
    assert not at.exception

    # Click on Next (1/3)
    _pick_button_by_label(at, "Next (1/3)").click().run()

    # NOTE: default datasets might return `It looks as the dataset 6378 has no indicator in use in any chart! Therefore, no mapping is needed.`
    # Click on Next (2/3)
    # _pick_button_by_label(at, "Next (2/3)").click().run()

    assert not at.exception


@pytest.mark.integration
@pytest.mark.usefixtures("set_config")
def test_app_fasttrack():
    at = AppTest.from_file(str(WIZARD_DIR / "app_pages/fasttrack/app.py"), default_timeout=DEFAULT_TIMEOUT).run()
    assert not at.exception

    # Try to reimport the latest uploaded sheet
    at.radio[0].set_value("update_gsheet")
    _pick_button_by_label(at, "Submit").click().run()

    assert not at.exception

import os
from contextlib import contextmanager

import pytest
from sqlalchemy import and_, select
from sqlalchemy.orm import Session
from streamlit.testing.v1 import AppTest
from streamlit.testing.v1.element_tree import Button, Toggle

import etl.grapher.model as gm
from apps.wizard.utils import WIZARD_DIR
from etl import config

DEFAULT_TIMEOUT = 30


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


def _pick_toggle_by_label(at: AppTest, label: str) -> Toggle:
    toggles = [toggle for toggle in at.toggle if toggle.label == label]
    if len(toggles) > 1:
        raise ValueError(f"Multiple toggles with label '{label}' found.")
    elif len(toggles) == 0:
        raise ValueError(f"Button with label '{label}' not found.")
    return toggles[0]


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
    at.button_group[0].set_value(["update_gsheet"])
    # at.radio[0].set_value("update_gsheet")
    _pick_button_by_label(at, "Submit").click().run()

    assert not at.exception


@pytest.mark.integration
@pytest.mark.usefixtures("set_config")
def test_app_harmonizer():
    at = AppTest.from_file(str(WIZARD_DIR / "app_pages/harmonizer/app.py"), default_timeout=DEFAULT_TIMEOUT).run()

    # Show all datasets
    toggle = _pick_toggle_by_label(at, "Show all datasets")
    toggle.set_value(True).run()
    assert not at.exception

    # Select dataset
    sel = at.selectbox[0]
    dataset_uri = "data://meadow/gapminder/2023-03-31/population"
    sel.set_value(dataset_uri).run()
    assert not at.exception

    assert (
        len(at.selectbox) == 3
    ), f"By selecting dataset {dataset_uri}, there should be three selectboxes (automatically populated)."

    # Check selectbox options
    sel2 = at.selectbox[1]
    assert sel2.value == "population", f"Expected 'population' but got {sel2.value}."

    sel3 = at.selectbox[2]
    assert sel3.value == "country", f"Expected 'country' but got {sel3.value}."


@pytest.mark.integration
@pytest.mark.usefixtures("set_config")
def test_app_dashboard():
    at = AppTest.from_file(str(WIZARD_DIR / "app_pages/dashboard/app.py"), default_timeout=DEFAULT_TIMEOUT).run()

    # Click on toggle
    toggles = at.toggle
    assert len(toggles) == 1, "There are more than one toggles on the page. Please revisit the test."
    toggle = toggles[0]
    toggle.set_value(False).run()

    assert not at.exception


@pytest.mark.integration
@pytest.mark.usefixtures("set_config")
def test_app_dataset_preview():
    at = AppTest.from_file(str(WIZARD_DIR / "app_pages/dataset_preview/app.py"), default_timeout=DEFAULT_TIMEOUT).run()

    # Select random dataset
    dataset_id = _get_random_dataset()

    sel = at.selectbox[0]
    sel.set_value(dataset_id).run()

    assert not at.exception

    # Click dependency graph
    btn = _pick_button_by_label(at, "Dependency graph")
    btn.click().run()

    assert not at.exception


def _get_random_dataset():
    with Session(config.OWID_ENV.engine) as session:
        ds = (
            session.execute(
                select(gm.Dataset).where(and_(gm.Dataset.isArchived == False, gm.Dataset.isPrivate == False))  # noqa
            )
            .scalars()
            .first()
        )

    dataset_id = ds.id  # type: ignore

    return dataset_id


@pytest.mark.integration
@pytest.mark.usefixtures("set_config")
def test_app_producer_analytics():
    at = AppTest.from_file(
        str(WIZARD_DIR / "app_pages/producer_analytics/app.py"), default_timeout=DEFAULT_TIMEOUT
    ).run()

    assert not at.exception


@pytest.mark.integration
@pytest.mark.usefixtures("set_config")
def test_app_explorer():
    at = AppTest.from_file(str(WIZARD_DIR / "app_pages/explorer_edit.py"), default_timeout=DEFAULT_TIMEOUT).run()

    assert not at.exception


@pytest.mark.integration
@pytest.mark.usefixtures("set_config")
def test_app_insight_search():
    at = AppTest.from_file(str(WIZARD_DIR / "app_pages/insight_search/app.py"), default_timeout=DEFAULT_TIMEOUT).run()

    # Set Author
    assert len(at.multiselect) == 1
    at.multiselect
    at.multiselect[0].set_value(["Max Roser"]).run()

    assert not at.exception


@pytest.mark.integration
@pytest.mark.usefixtures("set_config")
def test_app_insighter():
    at = AppTest.from_file(str(WIZARD_DIR / "app_pages/datainsight_robot.py"), default_timeout=DEFAULT_TIMEOUT).run()

    # Set Grapher URL
    assert len(at.text_input) == 1
    at.text_input[0].set_value("https://ourworldindata.org/grapher/life-expectancy").run()

    assert not at.exception

    # Generate
    at.button[0].click().run()
    assert not at.exception


@pytest.mark.integration
@pytest.mark.usefixtures("set_config")
def test_app_chart_animation():
    at = AppTest.from_file(str(WIZARD_DIR / "app_pages/chart_animation.py"), default_timeout=DEFAULT_TIMEOUT).run()

    assert not at.exception

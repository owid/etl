import pytest

from etl.config import Config, OWIDEnv, get_container_name


def test_get_container_name():
    assert get_container_name("branch") == "staging-site-branch"
    assert get_container_name("feature/x") == "staging-site-feature-x"
    assert get_container_name("do_not-do/this") == "staging-site-do-not-do-this"


def test_OWIDEnv_staging():
    env = OWIDEnv.from_staging("branch")
    assert env.env_remote == "staging"
    assert env.site == "http://staging-site-branch"
    assert env.name == "staging-site-branch"
    assert env.base_site == "http://staging-site-branch"
    assert env.admin_site == "http://staging-site-branch/admin"
    assert env.data_api_url == "https://api-staging.owid.io/staging-site-branch"
    assert env.indicators_url == "https://api-staging.owid.io/staging-site-branch/v1/indicators"

    env._env_local = "staging"
    assert env.wizard_url == "http://staging-site-branch/etl/wizard"


def test_OWIDEnv_production():
    env = OWIDEnv(
        Config(
            GRAPHER_USER_ID=1,
            DB_USER="user",
            DB_NAME="live_grapher",
            DB_PASS="xxx",
            DB_PORT="3306",
            DB_HOST="prod-db.owid.io",
        )
    )
    assert env.env_remote == "production"
    assert env.site == "https://ourworldindata.org"
    assert env.name == "production"
    assert env.base_site == "https://admin.owid.io"
    assert env.admin_site == "https://admin.owid.io/admin"
    assert env.data_api_url == "https://api.ourworldindata.org"
    assert env.indicators_url == "https://api.ourworldindata.org/v1/indicators"

    env._env_local = "production"
    assert env.wizard_url == "https://etl.owid.io/wizard/"


def test_OWIDEnv_dev():
    env = OWIDEnv(
        Config(
            GRAPHER_USER_ID=1,
            DB_USER="grapher",
            DB_NAME="grapher",
            DB_PASS="xxx",
            DB_PORT="3306",
            DB_HOST="127.0.0.1",
        )
    )
    assert env.env_remote == "dev"
    assert env.site == "http://localhost:3030"
    assert env.name == "dev"
    assert env.base_site == "http://localhost:3030"
    assert env.admin_site == "http://localhost:3030/admin"
    with pytest.raises(ValueError):
        env.data_api_url

    env._env_local = "dev"
    assert env.wizard_url == "http://localhost:8053/"

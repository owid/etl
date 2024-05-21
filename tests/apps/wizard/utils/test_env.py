from apps.wizard.utils.env import Config, OWIDEnv, get_container_name


def test_get_container_name():
    assert get_container_name("branch") == "staging-site-branch"
    assert get_container_name("feature/x") == "staging-site-feature-x"
    assert get_container_name("do_not-do/this") == "staging-site-do-not-do-this"


def test_OWIDEnv_staging():
    env = OWIDEnv.from_staging("branch")
    assert env.env_type_id == "staging"
    assert env.site == "http://staging-site-branch"
    assert env.name == "staging-site-branch"
    assert env.base_site == "http://staging-site-branch"
    assert env.admin_site == "http://staging-site-branch/admin"
    assert env.api_site == "https://api-staging.owid.io/staging-site-branch"
    assert env.indicators_url == "https://api-staging.owid.io/staging-site-branch/v1/indicators/"


def test_OWIDEnv_production():
    env = OWIDEnv(
        Config(
            DB_USER="user",
            DB_NAME="live_grapher",
            DB_PASS="xxx",
            DB_PORT="3306",
            DB_HOST="prod-db.owid.io",
        )
    )
    assert env.env_type_id == "production"
    assert env.site == "https://ourworldindata.org"
    assert env.name == "production"
    assert env.base_site == "https://admin.owid.io"
    assert env.admin_site == "https://admin.owid.io/admin"
    assert env.api_site == "https://api.ourworldindata.org"
    assert env.indicators_url == "https://api.ourworldindata.org/v1/indicators/"


def test_OWIDEnv_local():
    env = OWIDEnv(
        Config(
            DB_USER="grapher",
            DB_NAME="grapher",
            DB_PASS="xxx",
            DB_PORT="3306",
            DB_HOST="127.0.0.1",
        )
    )
    assert env.env_type_id == "local"
    assert env.site == "http://localhost:3030"
    assert env.name == "local"
    assert env.base_site == "http://localhost:3030"
    assert env.admin_site == "http://localhost:3030/admin"
    assert env.api_site == "http://localhost:8000"
    assert env.indicators_url == "http://localhost:8000/v1/indicators/"

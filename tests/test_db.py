from etl.db import get_engine


def _conf(**overrides):
    conf = dict(DB_USER="user", DB_PASS="p@ss", DB_HOST="db.example.org", DB_PORT="3306", DB_NAME="grapher")
    return {**conf, **overrides}


def test_get_engine_reuses_engine_for_same_conf():
    # Callers like `production_or_master_engine` pass a fresh conf dict on every call. Each call must reuse the
    # same Engine, otherwise every call leaks a new connection pool.
    assert get_engine(_conf()) is get_engine(_conf())


def test_get_engine_separates_different_conf():
    assert get_engine(_conf()) is not get_engine(_conf(DB_HOST="other.example.org"))


def test_get_engine_port_type_does_not_matter():
    assert get_engine(_conf(DB_PORT="3306")) is get_engine(_conf(DB_PORT=3306))

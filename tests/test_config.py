from etl.config import get_container_name


def test_get_container_name():
    assert get_container_name("branch") == "staging-site-branch"
    assert get_container_name("feature/x") == "staging-site-feature-x"
    assert get_container_name("do_not-do/this") == "staging-site-do-not-do-this"

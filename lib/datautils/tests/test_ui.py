from owid.datautils.ui import bail, blue, log, red

mock_strs = ["Test", "1", "Something + weird-no particulár meaning"]


def test_red():
    for mock_str in mock_strs:
        s = red(mock_str)
        assert s == f"\x1b[31m{mock_str}\x1b[0m"


def test_blue():
    for mock_str in mock_strs:
        s = blue(mock_str)
        assert s == f"\x1b[34m{mock_str}\x1b[0m"


def test_bail():
    try:
        bail("Test")
    except SystemExit:
        pass
    else:
        assert False


def test_log():
    log("action", "message")

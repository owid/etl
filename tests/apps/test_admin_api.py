import ast
import inspect
from pathlib import Path

from apps.chart_sync import admin_api

HTTP_METHODS = {"get", "put", "post", "delete", "patch"}


def _http_calls(tree: ast.Module) -> list[ast.Call]:
    """Every request made through `http_session` or `requests_with_retry()` in the module."""
    calls = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr not in HTTP_METHODS:
            continue
        receiver = node.func.value
        via_session = isinstance(receiver, ast.Name) and receiver.id == "http_session"
        via_retry = (
            isinstance(receiver, ast.Call)
            and isinstance(receiver.func, ast.Name)
            and receiver.func.id == "requests_with_retry"
        )
        if via_session or via_retry:
            calls.append(node)
    return calls


def test_every_admin_api_call_has_a_timeout():
    """Without a timeout, `requests` waits forever on an admin server that stops responding.

    That is not hypothetical: it once left two grapher upsert steps running until the CI job
    hit its own timeout hours later. Assert the invariant over the whole module rather than
    per call site, so a new endpoint cannot quietly reintroduce an unbounded wait.
    """
    tree = ast.parse(Path(inspect.getfile(admin_api)).read_text())
    calls = _http_calls(tree)
    assert calls, "found no admin API calls to check — did the module stop using requests?"

    missing = [f"line {c.lineno}" for c in calls if not any(kw.arg == "timeout" for kw in c.keywords)]
    assert not missing, f"admin API calls without a timeout: {', '.join(missing)}"


def test_retrying_session_is_shared():
    """A session per call opens a connection per request — tens of thousands for a big dataset."""
    assert admin_api.requests_with_retry() is admin_api.requests_with_retry()

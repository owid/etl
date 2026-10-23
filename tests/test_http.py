import os
import threading
from collections.abc import Generator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from etl.http import session


@pytest.fixture
def server() -> Generator[tuple[str, list[int]], None, None]:
    """Local keep-alive server that records the client port, i.e. the connection, of every request."""
    ports: list[int] = []

    class KeepAliveHandler(BaseHTTPRequestHandler):
        # HTTP/1.1 keeps the connection open, so the session pools it.
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:
            ports.append(self.client_address[1])
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), KeepAliveHandler)
    threading.Thread(target=httpd.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True).start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}/", ports
    httpd.shutdown()
    httpd.server_close()


# Forking with the server thread running warns on Python 3.12+.
@pytest.mark.filterwarnings("ignore:.*fork.*:DeprecationWarning")
def test_forked_child_gets_its_own_connection(server: tuple[str, list[int]]) -> None:
    url, ports = server
    session.get(url, timeout=5).raise_for_status()

    pid = os.fork()
    if pid == 0:
        try:
            session.get(url, timeout=5).raise_for_status()
            os._exit(0)
        except BaseException:
            os._exit(1)
    _, status = os.waitpid(pid, 0)
    assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, "Request from the forked child failed"

    session.get(url, timeout=5).raise_for_status()

    parent_before, child, parent_after = ports
    assert child != parent_before, "Child reused the parent's pooled connection"
    assert parent_after == parent_before, "Parent lost its pooled connection after the fork"

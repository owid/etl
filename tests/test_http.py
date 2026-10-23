import os
import threading
from collections.abc import Generator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from etl.http import session


class _RecordingServer(ThreadingHTTPServer):
    """Records the client port of every request, i.e. which connection it came on."""

    client_ports: list[int]


class _KeepAliveHandler(BaseHTTPRequestHandler):
    # HTTP/1.1 keeps the connection open, so the session pools it.
    protocol_version = "HTTP/1.1"
    server: _RecordingServer

    def do_GET(self) -> None:
        self.server.client_ports.append(self.client_address[1])
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format, *args) -> None:
        pass


@pytest.fixture
def server() -> Generator[_RecordingServer, None, None]:
    httpd = _RecordingServer(("127.0.0.1", 0), _KeepAliveHandler)
    httpd.client_ports = []
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield httpd
    httpd.shutdown()
    httpd.server_close()


@pytest.mark.filterwarnings("ignore:.*fork.*:DeprecationWarning")
def test_forked_child_gets_its_own_connection(server: _RecordingServer) -> None:
    url = f"http://127.0.0.1:{server.server_address[1]}/"
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

    parent_before, child, parent_after = server.client_ports
    assert child != parent_before, "Child reused the parent's pooled connection"
    assert parent_after == parent_before, "Parent lost its pooled connection after the fork"

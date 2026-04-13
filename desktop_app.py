from __future__ import annotations

import os
import socket
import threading
import time
from contextlib import closing
from typing import Optional
from urllib.error import URLError
from urllib.request import urlopen

import webview
from werkzeug.serving import make_server

from app import app


def _find_free_port(host: str, preferred: int) -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, preferred))
            return preferred
        except OSError:
            sock.bind((host, 0))
            return int(sock.getsockname()[1])


def _wait_for_server(url: str, timeout_sec: float = 15.0) -> None:
    deadline = time.time() + timeout_sec
    last_error: Optional[Exception] = None
    while time.time() < deadline:
        try:
            with urlopen(url, timeout=2) as resp:
                if 200 <= getattr(resp, "status", 200) < 500:
                    return
        except Exception as exc:  # pragma: no cover - startup timing
            last_error = exc
            time.sleep(0.2)
    raise RuntimeError(f"Desktop app server did not start in time: {last_error}")


class ServerThread(threading.Thread):
    def __init__(self, host: str, port: int):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self._server = make_server(host, port, app, threaded=True)
        self._ctx = app.app_context()
        self._ctx.push()

    def run(self) -> None:
        self._server.serve_forever()

    def shutdown(self) -> None:
        try:
            self._server.shutdown()
        finally:
            self._ctx.pop()


def main() -> None:
    host = "127.0.0.1"
    preferred_port = int(os.getenv("PORT", "5000"))
    port = _find_free_port(host, preferred_port)
    url = f"http://{host}:{port}"

    server = ServerThread(host, port)
    server.start()

    try:
        _wait_for_server(url)
    except Exception:
        server.shutdown()
        raise

    window = webview.create_window(
        "Stock Chart App",
        url,
        width=1600,
        height=1000,
        min_size=(1100, 760),
        confirm_close=True,
    )

    try:
        webview.start()
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()

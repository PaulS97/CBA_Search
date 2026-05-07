#!/usr/bin/env python3
"""Start the local app in production mode inside a desktop webview window."""

from __future__ import annotations

import os
import socket
import threading
import time

import uvicorn
import webview

from backend.app import app
from runtime_paths import FRONTEND_DIST, SOURCE_ROOT, print_runtime_diagnostics


HOST = "127.0.0.1"
PREFERRED_PORT = 8000
PORT_SCAN_LIMIT = 100


def ensure_frontend_build_exists() -> None:
    """Fail fast with a clear message when the production frontend is missing."""
    if FRONTEND_DIST.exists():
        return

    raise SystemExit(
        f"Frontend build not found at {FRONTEND_DIST}.\n"
        f"Build the frontend first, for example:\n"
        f"  cd {SOURCE_ROOT / 'frontend'}\n"
        f"  npm run build"
    )


def find_available_port(host: str, start_port: int) -> int:
    """Return the first localhost port available at or above start_port."""
    for port in range(start_port, start_port + PORT_SCAN_LIMIT):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind((host, port))
            except OSError:
                continue
            return port
    raise RuntimeError(
        f"No available port found from {start_port} to {start_port + PORT_SCAN_LIMIT - 1}."
    )


def wait_until_ready(host: str, port: int, timeout_seconds: int = 15) -> None:
    """Wait until the local server port accepts connections."""
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"Backend did not start on {host}:{port} within {timeout_seconds} seconds.")


def run_server(server: uvicorn.Server) -> None:
    """Run Uvicorn in the background until its shutdown flag is set."""
    server.run()


def stop_server(server: uvicorn.Server, server_thread: threading.Thread) -> None:
    """Ask Uvicorn to shut down, then force-exit if the process is still alive."""
    server.should_exit = True
    server_thread.join(timeout=5)
    if server_thread.is_alive():
        os._exit(0)


def main() -> None:
    ensure_frontend_build_exists()
    print_runtime_diagnostics("run_app")

    port = find_available_port(HOST, PREFERRED_PORT)
    app_url = f"http://{HOST}:{port}"
    config = uvicorn.Config(app, host=HOST, port=port, log_level="info")
    server = uvicorn.Server(config)
    server_thread = threading.Thread(target=run_server, args=(server,), daemon=True)
    server_thread.start()

    try:
        wait_until_ready(HOST, port)
        webview.create_window("CBA Search", app_url, width=1400, height=900)
        webview.start()
    finally:
        stop_server(server, server_thread)


if __name__ == "__main__":
    main()

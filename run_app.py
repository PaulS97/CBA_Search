#!/usr/bin/env python3
"""Start the local app in production mode and open it in the default browser."""

from __future__ import annotations

import socket
import threading
import time
import webbrowser

import uvicorn

from backend.app import app
from runtime_paths import FRONTEND_DIST, SOURCE_ROOT, print_runtime_diagnostics


HOST = "127.0.0.1"
PORT = 8000
APP_URL = f"http://{HOST}:{PORT}"


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


def open_browser_when_ready() -> None:
    """Wait for the local server port to accept connections, then open the browser."""
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            with socket.create_connection((HOST, PORT), timeout=0.5):
                webbrowser.open(APP_URL)
                return
        except OSError:
            time.sleep(0.2)


def main() -> None:
    ensure_frontend_build_exists()
    print_runtime_diagnostics("run_app")
    threading.Thread(target=open_browser_when_ready, daemon=True).start()
    uvicorn.run(app, host=HOST, port=PORT)


if __name__ == "__main__":
    main()

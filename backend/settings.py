from __future__ import annotations

import os

from runtime_paths import APP_SUPPORT_DIR, APP_SUPPORT_ENV_PATH, load_local_dotenv

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - dependency availability is runtime-specific.
    load_dotenv = None


OPENAI_API_KEY_NAME = "OPENAI_API_KEY"


def load_settings() -> None:
    """Load local dotenv settings without overriding an existing process env."""
    load_local_dotenv(load_dotenv)
    if os.getenv(OPENAI_API_KEY_NAME):
        return

    key = read_openai_api_key_from_app_support()
    if key:
        os.environ[OPENAI_API_KEY_NAME] = key


def read_openai_api_key_from_app_support() -> str:
    """Fallback parser for the packaged-app dotenv key."""
    if not APP_SUPPORT_ENV_PATH.exists():
        return ""

    for line in APP_SUPPORT_ENV_PATH.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        name, value = stripped.split("=", 1)
        if name.strip() != OPENAI_API_KEY_NAME:
            continue
        return value.strip().strip("\"'")
    return ""


def is_openai_api_key_configured() -> bool:
    load_settings()
    return bool(os.getenv(OPENAI_API_KEY_NAME, "").strip())


def save_openai_api_key(api_key: str) -> None:
    key = api_key.strip()
    if not key:
        raise ValueError("OpenAI API key is required.")

    APP_SUPPORT_DIR.mkdir(parents=True, exist_ok=True)
    APP_SUPPORT_ENV_PATH.write_text(f"{OPENAI_API_KEY_NAME}={key}\n", encoding="utf-8")
    os.environ[OPENAI_API_KEY_NAME] = key

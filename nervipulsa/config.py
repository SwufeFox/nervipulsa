"""Configuration precedence: command line, environment, user file, defaults.

The API key is stored outside the workspace. Nothing in this module writes
the key into the event journal or the worker environment.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any


PROVIDER_DEFAULTS: dict[str, dict[str, str]] = {
    "magpie": {
        "adapter": "openai-chat-completions",
        "base_url": "http://127.0.0.1:3425/v1",
        "model": "",
        "api_key": "magpie",
    },
    "openai": {
        "adapter": "openai-chat-completions",
        "base_url": "https://api.openai.com/v1",
        "model": "",
        "api_key": "",
    },
    "deepseek": {
        "adapter": "openai-chat-completions",
        "base_url": "https://api.deepseek.com",
        "model": "",
        "api_key": "",
    },
    "openrouter": {
        "adapter": "openai-chat-completions",
        "base_url": "https://openrouter.ai/api/v1",
        "model": "",
        "api_key": "",
    },
    "ollama": {
        "adapter": "openai-chat-completions",
        "base_url": "http://localhost:11434/v1",
        "model": "",
        "api_key": "",
    },
    "lmstudio": {
        "adapter": "openai-chat-completions",
        "base_url": "http://localhost:1234/v1",
        "model": "",
        "api_key": "",
    },
}


def config_dir() -> Path:
    override = os.environ.get("NERVIPULSA_CONFIG_DIR")
    if override:
        return Path(override)
    if os.name == "nt":
        base = os.environ.get("APPDATA")
        root = Path(base) if base else Path.home() / "AppData" / "Roaming"
    else:
        xdg = os.environ.get("XDG_CONFIG_HOME")
        root = Path(xdg) if xdg else Path.home() / ".config"
    return root / "nervipulsa"


def config_path() -> Path:
    return config_dir() / "config.json"


@dataclass
class Settings:
    provider: str = "magpie"
    adapter: str = "openai-chat-completions"
    base_url: str = "http://127.0.0.1:3425/v1"
    model: str = ""
    api_key: str = "magpie"
    active_profile: str = "magpie"
    profiles: dict[str, dict[str, str]] = field(default_factory=lambda: {
        name: dict(profile) for name, profile in PROVIDER_DEFAULTS.items()
    })
    workspace: str = ""
    max_activations: int = 100
    max_timeout: float = 120
    default_timeout: float = 30
    context_limit: int = 200_000
    ordinary_limit: int = 64
    result_limit: int = 4
    batch_limit: int = 32

    def public_view(self) -> dict[str, Any]:
        data = asdict(self)
        data["api_key"] = "set" if self.api_key else "missing"
        data["profiles"] = {
            name: {**profile, "api_key": "set" if profile.get("api_key") else "missing"}
            for name, profile in self.profiles.items()
        }
        return data


_ENV = {
    "provider": "NERVIPULSA_PROVIDER",
    "base_url": "NERVIPULSA_BASE_URL",
    "model": "NERVIPULSA_MODEL",
    "api_key": "NERVIPULSA_API_KEY",
    "workspace": "NERVIPULSA_DIR",
    "max_activations": "NERVIPULSA_MAX_ACTIVATIONS",
    "max_timeout": "NERVIPULSA_MAX_TIMEOUT",
    "default_timeout": "NERVIPULSA_TIMEOUT",
    "context_limit": "NERVIPULSA_CONTEXT_LIMIT",
}


def load_file(path: Path | None = None) -> dict[str, Any]:
    target = path or config_path()
    if not target.exists():
        return {}
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def load_settings(
    cli: dict[str, Any] | None = None,
    *,
    environ: dict[str, str] | None = None,
    file_data: dict[str, Any] | None = None,
) -> Settings:
    """Overlay defaults, then the user file, then environment, then CLI."""
    settings = Settings()
    values = file_data if file_data is not None else load_file()
    _apply(settings, values)
    if "profiles" not in values:
        settings.active_profile = settings.provider
        defaults = PROVIDER_DEFAULTS.get(settings.provider, {})
        for key, value in defaults.items():
            if key not in values:
                setattr(settings, key, value)
        settings.profiles.setdefault(settings.provider, {
            "adapter": settings.adapter,
            "base_url": settings.base_url,
            "model": settings.model,
            "api_key": settings.api_key,
        })
    active = settings.profiles.get(settings.active_profile)
    if isinstance(active, dict):
        for key in ("adapter", "base_url", "model", "api_key"):
            if key in active and key not in values:
                setattr(settings, key, active[key])
    env = os.environ if environ is None else environ
    from_env: dict[str, Any] = {}
    for field_name, env_name in _ENV.items():
        if env.get(env_name):
            from_env[field_name] = env[env_name]
    _apply(settings, from_env)
    _apply(settings, cli or {})
    override = cli or {}
    provider_overridden = "provider" in from_env or "provider" in override
    if provider_overridden and "active_profile" not in override:
        selected = settings.provider
        settings.active_profile = selected
        profile = settings.profiles.get(selected) or PROVIDER_DEFAULTS.get(selected, {})
        explicit = set(from_env) | set(override)
        for key in ("adapter", "base_url", "model", "api_key"):
            if key not in explicit and key in profile:
                setattr(settings, key, profile[key])
    _coerce(settings)
    return settings


def save_settings(settings: Settings, path: Path | None = None) -> Path:
    target = path or config_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    settings.profiles[settings.active_profile] = {
        "adapter": settings.adapter,
        "base_url": settings.base_url,
        "model": settings.model,
        "api_key": settings.api_key,
    }
    payload = asdict(settings)
    temporary = target.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    if os.name != "nt":
        os.chmod(temporary, 0o600)
    temporary.replace(target)
    if os.name != "nt":
        os.chmod(target, 0o600)
    return target


def _apply(settings: Settings, values: dict[str, Any]) -> None:
    known = {item.name for item in fields(settings)}
    for key, value in values.items():
        if key not in known or value is None or value == "":
            continue
        setattr(settings, key, value)


def _coerce(settings: Settings) -> None:
    settings.provider = str(settings.provider or "openai")
    settings.adapter = str(settings.adapter or "openai-chat-completions")
    settings.active_profile = str(settings.active_profile or settings.provider)
    if not isinstance(settings.profiles, dict):
        settings.profiles = {}
    settings.base_url = str(settings.base_url or "https://api.openai.com/v1").rstrip("/")
    settings.model = str(settings.model or "")
    settings.api_key = str(settings.api_key or "")
    settings.workspace = str(settings.workspace or "")
    settings.max_activations = max(1, int(settings.max_activations))
    settings.max_timeout = float(settings.max_timeout)
    settings.default_timeout = float(settings.default_timeout)
    if settings.default_timeout <= 0 or settings.default_timeout > settings.max_timeout:
        settings.default_timeout = min(30.0, settings.max_timeout)
    settings.context_limit = max(1000, int(settings.context_limit))
    settings.ordinary_limit = max(1, int(settings.ordinary_limit))
    settings.result_limit = max(1, int(settings.result_limit))
    settings.batch_limit = max(1, int(settings.batch_limit))

"""Configuration precedence and secret storage."""

from __future__ import annotations

import json
from pathlib import Path

from nervipulsa.config import Settings, load_settings, save_settings


def test_cli_beats_environment_beats_file() -> None:
    settings = load_settings(
        {"model": "from-cli"},
        environ={
            "NERVIPULSA_MODEL": "from-env",
            "NERVIPULSA_API_KEY": "env-key",
            "NERVIPULSA_BASE_URL": "https://env.example/v1",
        },
        file_data={
            "model": "from-file",
            "api_key": "file-key",
            "base_url": "https://file.example/v1",
            "provider": "openai",
        },
    )
    assert settings.model == "from-cli"
    assert settings.api_key == "env-key"
    assert settings.base_url == "https://env.example/v1"
    assert settings.provider == "openai"
    assert settings.public_view()["api_key"] == "set"


def test_save_roundtrip_keeps_the_key_out_of_the_view(workspace: Path) -> None:
    settings = Settings(model="m", api_key="super-secret-key", base_url="https://example.test/v1")
    path = save_settings(settings, workspace / "config.json")
    stored = json.loads(path.read_text(encoding="utf-8"))
    assert stored["api_key"] == "super-secret-key"
    loaded = load_settings(file_data=stored, environ={})
    assert loaded.api_key == "super-secret-key"
    assert "super-secret-key" not in json.dumps(loaded.public_view())

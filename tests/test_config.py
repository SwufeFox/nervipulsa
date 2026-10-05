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


def test_legacy_config_loads_and_magpie_profile_is_available() -> None:
    settings = load_settings(
        file_data={"provider": "custom-gateway", "base_url": "https://gateway.test/v1", "model": "m", "api_key": "k"},
        environ={},
    )
    assert settings.provider == "custom-gateway"
    assert settings.adapter == "openai-chat-completions"
    assert settings.active_profile == "custom-gateway"
    assert settings.profiles["magpie"]["adapter"] == "openai-chat-completions"


def test_default_settings_use_official_magpie_local_gateway() -> None:
    settings = load_settings(environ={}, file_data={})
    assert settings.provider == "magpie"
    assert settings.active_profile == "magpie"
    assert settings.adapter == "openai-chat-completions"
    assert settings.base_url == "http://127.0.0.1:3425/v1"
    assert settings.api_key == "magpie"
    assert settings.model == ""




def test_environment_provider_override_selects_matching_profile() -> None:
    settings = load_settings(
        environ={"NERVIPULSA_PROVIDER": "openai"},
        file_data={},
    )
    assert settings.provider == settings.active_profile == "openai"
    assert settings.base_url == "https://api.openai.com/v1"


def test_cli_provider_override_selects_matching_profile_and_preserves_explicit_endpoint() -> None:
    settings = load_settings(
        {"provider": "openai"},
        environ={},
        file_data={},
    )
    assert settings.provider == settings.active_profile == "openai"
    assert settings.base_url == "https://api.openai.com/v1"

    custom = load_settings(
        {"provider": "openai", "base_url": "https://proxy.example/v1"},
        environ={},
        file_data={},
    )
    assert custom.provider == custom.active_profile == "openai"
    assert custom.base_url == "https://proxy.example/v1"



def test_profiles_persist_independent_endpoint_model_and_key(workspace: Path) -> None:
    settings = Settings()
    settings.profiles["openai"] = {
        "adapter": "openai-chat-completions",
        "base_url": "https://api.openai.com/v1",
        "model": "gpt-test",
        "api_key": "openai-key",
    }
    path = save_settings(settings, workspace / "config.json")
    stored = json.loads(path.read_text(encoding="utf-8"))
    loaded = load_settings(file_data=stored, environ={})
    assert loaded.profiles["magpie"]["base_url"] == "http://127.0.0.1:3425/v1"
    assert loaded.profiles["magpie"]["api_key"] == "magpie"
    assert loaded.profiles["openai"]["model"] == "gpt-test"
    assert loaded.profiles["openai"]["api_key"] == "openai-key"

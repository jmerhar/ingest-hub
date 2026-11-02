"""The configuration file, secret files, settings and turning a configuration into a plan."""

from __future__ import annotations

from pathlib import Path

import httpx2
import pytest
from jigkit.influx import Influx2Sink, Influx3Sink

from ingest_hub.adapters.tuya import TuyaSource
from ingest_hub.config import ConfigError, load_config, read_secret
from ingest_hub.hub import make_sink, plan, source_params
from ingest_hub.settings import Settings

from .conftest import write_config


def config_text(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "sources.toml"
    path.write_text(text)
    return path


def test_a_full_configuration_loads(tmp_path, secrets):
    config = load_config(write_config(tmp_path / "c.toml", "http://db", "http://tuya", secrets))
    assert config.destinations["home"].kind == "influx3"
    assert config.sources["plugs"].enabled is True
    assert config.sources["plugs"].params["base_url"] == "http://tuya"


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("[sources.a]\nkind='tuya'\ndestination='nowhere'\n", "destination 'nowhere'"),
        ("[destinations.Bad]\nkind='influx3'\nurl='u'\ndatabase='d'\n", "not a valid name"),
        ("[destinations.d]\nkind='mongo'\nurl='u'\ndatabase='d'\n", "destinations.d.kind"),
        ("[destinations.d]\nkind='influx3'\nurl='u'\ndatabase='d'\ntoken='x'\n", "Extra"),
        ("surprise = 1\n", "Extra"),
        ("not toml [", "not valid TOML"),
    ],
)
def test_invalid_configurations_say_where(tmp_path, text, message):
    with pytest.raises(ConfigError, match=message):
        load_config(config_text(tmp_path, text))


def test_validation_messages_never_echo_values(tmp_path):
    path = config_text(
        tmp_path, "[destinations.d]\nkind='influx3'\nurl='u'\ndatabase='d'\ntoken='hunter2'\n"
    )
    with pytest.raises(ConfigError) as raised:
        load_config(path)
    assert "hunter2" not in str(raised.value)


def test_a_missing_or_unreadable_file(tmp_path):
    with pytest.raises(ConfigError, match="no configuration file"):
        load_config(tmp_path / "absent.toml")
    with pytest.raises(ConfigError, match="cannot read"):
        load_config(tmp_path)  # a directory


def test_secret_files(tmp_path):
    (tmp_path / "s").write_text("  value \n")
    assert read_secret(tmp_path / "s") == "value"
    (tmp_path / "empty").write_text("\n")
    with pytest.raises(ConfigError, match="is empty"):
        read_secret(tmp_path / "empty")
    with pytest.raises(ConfigError, match="cannot read secret file"):
        read_secret(tmp_path / "missing")


def test_sinks_by_kind(tmp_path, secrets):
    config = load_config(
        config_text(
            tmp_path,
            f"""
[destinations.three]
kind = "influx3"
url = "http://db3"
database = "a"
token_file = "{secrets / "influx_token"}"

[destinations.two]
kind = "influx2"
url = "http://db2"
database = "b"
org = "home"
""",
        )
    )
    three = make_sink(config.destinations["three"])
    two = make_sink(config.destinations["two"])
    assert isinstance(three, Influx3Sink) and three.token == "synthetic-token"
    assert isinstance(two, Influx2Sink) and two.org == "home" and two.token == ""


async def test_plan_builds_enabled_sources_sharing_a_sink(tmp_path, secrets):
    extra_source = f"""
[sources.other]
kind = "tuya"
destination = "home"
heartbeat_url_file = "{secrets / "tuya_id"}"
[sources.other.params]
access_id_file = "{secrets / "tuya_id"}"
access_secret_file = "{secrets / "tuya_secret"}"

[sources.off]
kind = "tuya"
destination = "home"
enabled = false
[sources.off.params]
access_id_file = "{secrets / "tuya_id"}"
access_secret_file = "{secrets / "tuya_secret"}"
"""
    path = write_config(tmp_path / "c.toml", "http://db", "http://tuya", secrets)
    path.write_text(path.read_text() + extra_source)
    config = load_config(path)
    async with httpx2.AsyncClient() as client:
        built = plan(config, client)
        assert [s.name for s in built.specs] == ["plugs", "other"]
        assert built.specs[0].sink is built.specs[1].sink
        assert isinstance(built.specs[0].source, TuyaSource)
        assert built.specs[0].heartbeat_url is None
        assert built.specs[1].heartbeat_url == "synthetic-id"
        # --only runs one source, even a disabled one.
        assert [s.name for s in plan(config, client, "off").specs] == ["off"]
        with pytest.raises(ConfigError, match="no source named"):
            plan(config, client, "nope")


async def test_plan_without_enabled_sources_or_with_an_unknown_kind(tmp_path):
    base = "[destinations.d]\nkind='influx3'\nurl='u'\ndatabase='d'\n"
    async with httpx2.AsyncClient() as client:
        with pytest.raises(ConfigError, match="no enabled sources"):
            plan(load_config(config_text(tmp_path, base)), client)
        config = load_config(
            config_text(tmp_path, base + "[sources.s]\nkind='weather'\ndestination='d'\n")
        )
        with pytest.raises(ConfigError, match="unknown kind 'weather'"):
            plan(config, client)


def test_source_params_are_validated_by_the_adapter(tmp_path):
    config = load_config(
        config_text(
            tmp_path,
            "[destinations.d]\nkind='influx3'\nurl='u'\ndatabase='d'\n"
            "[sources.s]\nkind='tuya'\ndestination='d'\n[sources.s.params]\nregion='mars'\n",
        )
    )
    with pytest.raises(ConfigError, match="source 's' params: .*access_id_file"):
        source_params(config, "s")


def test_settings_default_the_config_into_the_data_directory(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    monkeypatch.delenv("CONFIG_FILE", raising=False)
    assert Settings().resolved_config_file == tmp_path / "config" / "sources.toml"
    monkeypatch.setenv("CONFIG_FILE", "/etc/hub.toml")
    assert Settings().resolved_config_file == Path("/etc/hub.toml")


def test_the_shipped_example_is_valid(tmp_path):
    example = Path(__file__).parents[2] / "examples" / "sources.toml"
    config = load_config(example)
    assert source_params(config, "plugs").table_prefix == "tuya_"  # type: ignore[attr-defined]

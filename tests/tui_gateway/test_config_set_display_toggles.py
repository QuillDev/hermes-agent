"""`config.set` has to accept the Appearance switches the desktop mirrors.

The handler matches an explicit key list and answers 4002 for everything else,
so a renderer mirroring an unlisted key writes nothing at all — and the
renderer's `.catch()` swallows the refusal, which is how message reactions
shipped with a toggle that never reached the backend gating the tool.
"""

import pytest
import hermes_yaml as yaml

from tui_gateway import server

@pytest.fixture
def config_home(tmp_path, monkeypatch):
    """Point the server's config read/write at a temp file."""
    monkeypatch.setattr(server, "_hermes_home", tmp_path)
    server._cfg_cache = server._cfg_sig = server._cfg_path = None
    yield tmp_path / "config.yaml"
    server._cfg_cache = server._cfg_sig = server._cfg_path = None

def _set(key, value):
    return server._methods["config.set"](1, {"key": key, "value": value})

@pytest.mark.parametrize("key", sorted(server._DISPLAY_TOGGLE_KEYS))
def test_a_mirrored_switch_reaches_the_config_file(config_home, key):
    """Both directions land on disk, where the tools' check_fn reads them."""
    assert _set(key, "false")["result"] == {"key": key, "value": False}

    section, name = key.split(".")
    assert yaml.safe_load(config_home.read_text())[section][name] is False

    assert _set(key, "true")["result"] == {"key": key, "value": True}
    assert yaml.safe_load(config_home.read_text())[section][name] is True

def test_a_non_boolean_is_refused_rather_than_written(config_home):
    answer = _set("display.in_app_tips", "sometimes")

    assert answer["error"]["code"] == 4002
    assert not config_home.exists()


def test_session_sharing_setting_is_off_until_explicitly_enabled(config_home):
    from tui_gateway.session_clients import enabled
    get = server._methods['config.get']
    assert get(1, {'key': 'session_clients.enabled'})['result']['value'] is False
    assert not enabled(config_home.parent)
    assert _set('session_clients.enabled', True)['result']['value'] is True
    assert get(1, {'key': 'session_clients.enabled'})['result']['value'] is True
    assert enabled(config_home.parent)
    assert _set('session_clients.enabled', False)['result']['value'] is False
    assert not enabled(config_home.parent)
    assert _set('session_clients.enabled', 'sometimes')['error']['code'] == 4002
    assert yaml.safe_load(config_home.read_text())['session_clients']['enabled'] is False

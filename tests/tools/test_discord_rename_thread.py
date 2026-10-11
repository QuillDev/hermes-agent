"""Thread renaming through the public Discord tool and HTTP boundary."""

import io
import json
import urllib.error
from email.message import Message

import pytest

from tools import discord_tool

THREAD_ID = "1558757681936998400"


class DiscordResponse(io.BytesIO):
    status = 200


class DiscordHTTP:
    """Small stateful Discord API stand-in; only name changes are supported."""

    def __init__(self, channel_type=11, forbidden_method=None):
        self.channel = {"id": THREAD_ID, "type": channel_type, "name": "Original"}
        self.forbidden_method = forbidden_method

    def __call__(self, request, timeout=15):
        if request.get_method() == self.forbidden_method:
            raise urllib.error.HTTPError(
                request.full_url, 403, "Forbidden", Message(),
                io.BytesIO(b'{"message": "Missing Permissions", "code": 50013}'),
            )
        if request.full_url != f"{discord_tool.DISCORD_API_BASE}/channels/{THREAD_ID}":
            raise AssertionError(f"Unexpected endpoint: {request.full_url}")
        if request.get_method() == "PATCH":
            body = json.loads(request.data)
            if set(body) != {"name"}:
                raise AssertionError(f"Unexpected mutation: {body}")
            self.channel.update(body)
        elif request.get_method() != "GET":
            raise AssertionError(f"Unexpected method: {request.get_method()}")
        return DiscordResponse(json.dumps(self.channel).encode())


@pytest.fixture
def discord_api(monkeypatch):
    monkeypatch.setenv("DISCORD_BOT_TOKEN", "test-token")
    monkeypatch.setattr(discord_tool, "_load_allowed_actions_config", lambda: None)
    api = DiscordHTTP()
    monkeypatch.setattr(discord_tool.urllib.request, "urlopen", api)
    return api


def rename(**params):
    return json.loads(discord_tool.discord_core(
        action="rename_thread", channel_id=THREAD_ID, name="Discord debugging", **params,
    ))


@pytest.mark.parametrize("channel_type", [10, 11, 12])
def test_renames_each_thread_type_without_changing_other_fields(discord_api, channel_type):
    discord_api.channel["type"] = channel_type
    result = rename()
    assert result == {"success": True, "thread_id": THREAD_ID, "name": "Discord debugging"}
    assert discord_api.channel == {
        "id": THREAD_ID, "type": channel_type, "name": "Discord debugging",
    }


@pytest.mark.parametrize("channel_type", [0, 1, 2, 4, 5, 13, 15, 16])
def test_refuses_to_rename_non_thread_channels(discord_api, channel_type):
    discord_api.channel["type"] = channel_type
    result = rename()
    assert result["error"] == "Target channel is not a Discord thread."
    assert discord_api.channel["name"] == "Original"


@pytest.mark.parametrize("name", [" ", "\n\t", "x" * 101])
def test_rejects_invalid_titles_without_renaming(discord_api, name):
    result = json.loads(discord_tool.discord_core(
        action="rename_thread", channel_id=THREAD_ID, name=name,
    ))
    assert "name must" in result["error"].lower()
    assert discord_api.channel["name"] == "Original"


@pytest.mark.parametrize("channel_id", ["abc", "../guilds/123", "123", "１２３４５６７８９０１２３４５６７８９"])
def test_rejects_invalid_thread_identifiers_without_renaming(discord_api, channel_id):
    result = json.loads(discord_tool.discord_core(
        action="rename_thread", channel_id=channel_id, name="New title",
    ))
    assert "channel_id" in result["error"]
    assert discord_api.channel["name"] == "Original"


@pytest.mark.parametrize("method", ["GET", "PATCH"])
def test_permission_failure_explains_thread_access_and_leaves_name_unchanged(discord_api, method):
    discord_api.forbidden_method = method
    result = rename()
    assert "403" in result["error"]
    assert "MANAGE_THREADS" in result["error"]
    assert discord_api.channel["name"] == "Original"


def test_disabled_action_cannot_rename_thread(discord_api, monkeypatch):
    monkeypatch.setattr(discord_tool, "_load_allowed_actions_config", lambda: ["fetch_messages"])
    result = rename()
    assert "disabled" in result["error"]
    assert discord_api.channel["name"] == "Original"


@pytest.mark.parametrize("params", [{}, {"channel_id": THREAD_ID}, {"name": "New title"}])
def test_missing_parameters_leave_thread_unchanged(discord_api, params):
    result = json.loads(discord_tool.discord_core(action="rename_thread", **params))
    assert "Missing required" in result["error"]
    assert discord_api.channel["name"] == "Original"


def test_core_schema_exposes_rename_thread(discord_api, monkeypatch):
    monkeypatch.setattr(discord_tool, "_detect_capabilities_nonblocking", lambda token: {})
    schema = discord_tool.get_dynamic_schema_core()
    assert schema is not None
    assert "rename_thread" in schema["parameters"]["properties"]["action"]["enum"]
    assert "rename_thread(channel_id, name)" in schema["description"]

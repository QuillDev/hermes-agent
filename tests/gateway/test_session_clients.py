"""Shared-session delivery keeps profile/thread boundaries and survives gateway restart."""
from types import SimpleNamespace

import pytest

import hermes_bootstrap  # Bootstrap before the home-I/O guard is armed.

from gateway.config import Platform
from gateway.platforms.base import SendResult
from gateway.platforms.event import MessageEvent
from gateway.run_inbound import GatewayInboundMixin
from gateway.run_busy import GatewayBusySessionMixin
from gateway.session import SessionSource
from gateway.session_clients import _cursor, _send_frame


class DiscordTransport:
    def __init__(self):
        self.messages = []
        self.files = []
        self.fail = False

    async def send(self, chat_id, content, **kwargs):
        self.messages.append((chat_id, content, kwargs))
        return SendResult(success=not self.fail)

    async def send_document(self, chat_id, file_path, **kwargs):
        self.files.append((chat_id, file_path))
        return SendResult(success=True)

    async def send_image_file(self, chat_id, image_path, **kwargs):
        self.files.append((chat_id, image_path))
        return SendResult(success=True)

    async def send_image(self, chat_id, image_url, **kwargs):
        self.files.append((chat_id, image_url))
        return SendResult(success=True)


def frame(event, **payload):
    return {"jsonrpc": "2.0", "method": "event", "params": {
        "type": event, "session_id": "runtime", "payload": payload}}


@pytest.mark.asyncio
async def test_final_media_delivery_and_cursor_survive_restart(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr("gateway.platforms.base._HERMES_ROOT", tmp_path)
    monkeypatch.setattr("gateway.platforms.base.MEDIA_DELIVERY_SAFE_ROOTS", (tmp_path / 'cache',))
    from gateway.run import _profile_runtime_scope
    adapter = DiscordTransport()
    report = tmp_path / "report.txt"
    report.write_text("shared result")
    event = frame("message.complete", text=f"Finished\nMEDIA:{report}")
    with _profile_runtime_scope(tmp_path):
        await _send_frame(adapter, "thread-one", event, 0)
    _cursor(tmp_path, "shared-one", "thread-one", 17)
    assert adapter.messages[0][:2] == ("thread-one", "Finished")
    assert adapter.files == [("thread-one", str(report))]
    assert _cursor(tmp_path, "shared-one", "thread-one") == 17
    assert _cursor(tmp_path, "shared-one", "thread-two") == 0
    assert _cursor(tmp_path, "shared-two", "thread-one") == 0


@pytest.mark.asyncio
async def test_progress_is_bounded_and_origin_echo_suppressed():
    adapter = DiscordTransport()
    last = await _send_frame(adapter, "thread", frame("tool.start", name="terminal"), 0)
    await _send_frame(adapter, "thread", frame("message.interim", text="Working"), last)
    await _send_frame(adapter, "thread", frame("session.clients.input", text="hello", origin="discord:thread"), last)
    await _send_frame(adapter, "thread", frame("message.complete", text="Done"), last)
    assert [content for _, content, _ in adapter.messages] == ["Working: terminal", "Done"]
    assert adapter.messages[0][2]["metadata"] == {"_interim_send": True}


@pytest.mark.asyncio
@pytest.mark.parametrize("admitted,paused", [(False, None), (True, "Paused")])
async def test_inbound_gates_reject_before_shared_session_dispatch(admitted, paused, monkeypatch):
    import gateway.session_clients as clients
    async def forbidden(*args):
        pytest.fail("shared session was reached before admission and emergency stop")
    monkeypatch.setattr(clients, "handle_session_client_message", forbidden)
    event = MessageEvent(text="/attach secret", source=SessionSource(
        platform=Platform.DISCORD, chat_id="channel", user_id="owner"), internal=True)
    async def admit(_event):
        return (event, event.source, True) if admitted else None
    runner = SimpleNamespace(_hm_admit_event=admit,
                             _hm_estop_gate=lambda *args: paused)
    assert await GatewayInboundMixin._handle_message(runner, event) == paused


@pytest.mark.asyncio
async def test_real_hub_replay_and_inbound_identity_are_thread_scoped(tmp_path, monkeypatch):
    from tui_gateway import session_clients
    from gateway.session_clients import handle_session_client_message, poll_session_clients

    home = tmp_path / "owner"
    home.mkdir()
    (home / "config.yaml").write_text("session_clients:\n  enabled: true\n")
    hub = session_clients.SessionClientHub(home, start=False)
    hub.claim("stored", "runtime")
    invite = hub.invite("stored")
    hub.attach(invite["token"], "discord", "thread-one", "owner")
    monkeypatch.setattr(session_clients, "get_hub", lambda _home: hub)
    adapter = DiscordTransport()
    runner = SimpleNamespace(
        _resolve_profile_home_for_source=lambda source: home,
        _check_slash_access=lambda source, cmd: GatewayBusySessionMixin._check_slash_access(runner, source, cmd),
        _adapters_for_profile=lambda profile: {Platform.DISCORD: adapter} if profile == "secondary" else {},
    )
    try:
        foreign = MessageEvent(text="do something", source=SessionSource(
            platform=Platform.DISCORD, chat_id="thread-one", user_id="stranger"))
        handled, reply = await handle_session_client_message(runner, foreign)
        assert handled and "another Discord user" in reply
        parent = MessageEvent(text="private conversation", source=SessionSource(
            platform=Platform.DISCORD, chat_id="parent", user_id="owner"))
        assert await handle_session_client_message(runner, parent) == (False, None)
        owner = MessageEvent(text="/status", source=SessionSource(
            platform=Platform.DISCORD, chat_id="thread-one", user_id="owner"))
        assert await handle_session_client_message(runner, owner) == (True, "Shared session: runtime")
        bot = MessageEvent(text="echo", source=owner.source,
                           raw_message=SimpleNamespace(author=SimpleNamespace(bot=True)))
        assert await handle_session_client_message(runner, bot) == (True, None)
        hub.publish("stored", frame("message.complete", text="one answer"))
        await poll_session_clients(runner, home, "default", {})
        assert not adapter.messages
        await poll_session_clients(runner, home, "secondary", {})
        # Fresh polling state models a restarted gateway; the disk cursor suppresses replay.
        await poll_session_clients(runner, home, "secondary", {})
        assert [(chat, text) for chat, text, _ in adapter.messages] == [("thread-one", "one answer")]
        (home / "config.yaml").write_text("session_clients:\n  enabled: false\n")
        handled, refusal = await handle_session_client_message(runner, owner)
        assert handled and "no replacement run" in refusal
    finally:
        hub.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('surface', ['parent', 'existing-thread', 'auto-thread'])
async def test_attach_consumes_invite_for_fresh_thread_only(tmp_path, monkeypatch, surface):
    from tui_gateway import session_clients
    from gateway.session_clients import handle_session_client_message

    (tmp_path / "config.yaml").write_text("session_clients:\n  enabled: true\n")
    hub = session_clients.SessionClientHub(tmp_path, start=False)
    hub.claim("stored", "runtime")
    invite = hub.invite("stored")
    adapter = DiscordTransport()
    threads = []
    async def create_thread(parent_chat_id, name):
        threads.append(parent_chat_id)
        return f"thread-{len(threads)}"
    adapter.create_handoff_thread = create_thread
    monkeypatch.setattr(session_clients, "get_hub", lambda _home: hub)
    runner = SimpleNamespace(_resolve_profile_home_for_source=lambda source: tmp_path,
                             _check_slash_access=lambda source, cmd: GatewayBusySessionMixin._check_slash_access(runner, source, cmd),
                             _delivery_adapter_for=lambda source: adapter)
    event = MessageEvent(text=f"/attach {invite['token']}", source=SessionSource(
        platform=Platform.DISCORD, chat_id=surface, user_id="owner",
        parent_chat_id='parent' if surface != 'parent' else None,
        thread_id=surface if surface != 'parent' else None, auto_thread_created=surface == 'auto-thread'))
    try:
        handled, reply = await handle_session_client_message(runner, event)
        linked_thread = 'auto-thread' if surface == 'auto-thread' else 'thread-1'
        assert handled and reply is not None and linked_thread in reply
        assert hub.link("discord", "parent") is None
        linked = hub.link("discord", linked_thread)
        assert linked is not None and linked['user_id'] == 'owner'
        assert threads == ([] if surface == 'auto-thread' else ['parent'])
        handled, reply = await handle_session_client_message(runner, event)
        assert handled and reply is not None
        assert 'already attached' in reply if surface == 'auto-thread' else 'failed' in reply.lower()
        assert hub.link("discord", "thread-2") is None
    finally:
        hub.close()

"""Two OS processes, real TUI RPC/agent/SQLite, fake model HTTP and Discord only."""
from __future__ import annotations

import asyncio
import multiprocessing
import os
from pathlib import Path
import threading
import time
import traceback
from types import SimpleNamespace

import pytest


def _owner_process(home, ready, stop, surface):
    os.environ['HERMES_HOME'] = home
    os.environ['HERMES_RUNTIME_DIR'] = str(Path(home) / 'runtime')
    try:
        from tui_gateway import server
        from tui_gateway.session_clients import get_hub
        hub = get_hub(home)
        if surface == 'discord':
            from tests.gateway.test_session_clients import DiscordTransport
            from gateway.config import Platform
            from gateway.platforms.event import MessageEvent
            from gateway.session import SessionSource
            from gateway.session_clients import handle_session_client_message
            adapter = DiscordTransport()
            async def create_thread(parent, title):
                return 'creator-thread'
            adapter.create_handoff_thread = create_thread
            runner = SimpleNamespace(_resolve_profile_home_for_source=lambda source: Path(home),
                                     _delivery_adapter_for=lambda source: adapter)
            event = MessageEvent(text='/share', source=SessionSource(
                platform=Platform.DISCORD, chat_id='parent', user_id='owner'))
            handled, response = asyncio.run(handle_session_client_message(runner, event))
            assert handled and 'creator-thread' in response
            record = hub.lookup(hub.link('discord', 'creator-thread')['session_key'])
        else:
            created = server.handle_request({'id': 'create', 'method': 'session.create',
                                             'params': {'source': 'desktop'}})
            sid = created['result']['session_id']
            shared = server.handle_request({'id': 'share', 'method': 'session.clients.share',
                                            'params': {'session_id': sid}})
            assert 'result' in shared, shared
            record = hub.lookup(created['result']['stored_session_id'])
        invitation = hub.share(record['sid'])
        ready.put((record, invitation))
        stop.wait(60)
        server._shutdown_sessions()
        hub.close()
    except BaseException:
        ready.put({'error': traceback.format_exc()})
        raise


class DesktopPeer:
    def __init__(self):
        self.frames = []
        self._closed = False
    def write(self, obj):
        self.frames.append(obj)
        return not self._closed
    def close(self):
        self._closed = True


async def _until(predicate, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        await asyncio.sleep(0.02)
    raise AssertionError('timed out waiting for shared execution outcome')


@pytest.mark.asyncio
@pytest.mark.parametrize('surface', ['desktop', 'discord'])
async def test_desktop_discord_share_live_execution_across_processes(tmp_path, monkeypatch, surface):
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text, ToolCall, Hang, write_hermes_home
    from tests.gateway.test_session_clients import DiscordTransport
    from gateway.config import Platform
    from gateway.platforms.event import MessageEvent
    from gateway.session import SessionSource
    from gateway.session_clients import handle_session_client_message, poll_session_clients
    from tui_gateway import server
    from tui_gateway.session_clients import get_hub
    from tui_gateway.transport import bind_transport, reset_transport
    from hermes_state import SessionDB

    requested = threading.Event()
    release = threading.Event()
    stop_requested = threading.Event()
    request_count = 0

    def model_reply(record):
        nonlocal request_count
        request_count += 1
        if request_count == 1:
            requested.set()
            assert release.wait(20)
            return ToolCall('terminal', {'command': 'printf session-client-test'})
        if request_count == 2:
            return Text('same execution completed')
        stop_requested.set()
        return Hang(seconds=4)

    with FakeLLMServer(model_reply) as provider:
        home = write_hermes_home(tmp_path / 'profile', provider.base_url, extra_config=(
            'session_clients:\n  enabled: true\n'
            'memory:\n  memory_enabled: false\n  user_profile_enabled: false\n'))
        monkeypatch.setenv('HERMES_HOME', str(home))
        monkeypatch.setattr(server, '_hermes_home', home)
        ctx = multiprocessing.get_context('spawn')
        ready, stop = ctx.Queue(), ctx.Event()
        child = ctx.Process(target=_owner_process, args=(str(home), ready, stop, surface))
        child.start()
        hub = None
        try:
            started = await asyncio.to_thread(ready.get, True, 30)
            assert not isinstance(started, dict), started
            record, invitation = started
            key, sid = record['session_key'], record['sid']
            assert record['pid'] == child.pid and record['pid'] != os.getpid()
            hub = get_hub(home)
            adapter = DiscordTransport()
            async def create_thread(parent, title):
                return 'linked-thread'
            adapter.create_handoff_thread = create_thread
            runner = SimpleNamespace(
                _resolve_profile_home_for_source=lambda source: home,
                _delivery_adapter_for=lambda source: adapter,
                _adapters_for_profile=lambda profile: {Platform.DISCORD: adapter})
            source = SessionSource(platform=Platform.DISCORD, chat_id='parent', user_id='owner')
            attach = MessageEvent(text=f"/attach {invitation['token']}", source=source)
            handled, reply = await handle_session_client_message(runner, attach)
            assert handled and 'linked-thread' in reply
            source = SessionSource(platform=Platform.DISCORD, chat_id='linked-thread', user_id='owner')
            desktop = DesktopPeer()
            token = bind_transport(desktop)
            try:
                resumed = await asyncio.to_thread(server.handle_request, {
                    'id': 'resume', 'method': 'session.resume', 'params': {'session_id': key, 'source': 'desktop'}})
            finally:
                reset_transport(token)
            assert resumed['result']['session_id'] == sid, resumed
            assert sid not in server._sessions  # observer process built no second AIAgent
            report = home / 'shared-report.txt'
            report.write_text('attachment bytes across clients')
            attached = await asyncio.to_thread(server.handle_request, {
                'id': 'attach-file', 'method': 'file.attach', 'params': {'session_id': sid, 'path': str(report)}})
            assert attached['result']['attached'], attached
            await _until(lambda: any(f.get('params', {}).get('type') == 'session.clients.attachment'
                                     for f in desktop.frames))
            await poll_session_clients(runner, home, 'default', {})
            assert ('linked-thread', attached['result']['path']) in adapter.files
            upload = home / 'discord-upload.txt'
            upload.write_text('downloaded Discord attachment')
            event = MessageEvent(text='original shared prompt', source=source,
                                 media_urls=[str(upload)], media_types=['text/plain'])
            assert await handle_session_client_message(runner, event) == (True, None)
            await _until(requested.is_set)
            await _until(lambda: any(f.get('params', {}).get('type') == 'session.clients.input'
                                     for f in desktop.frames))
            shared_input = next(f['params']['payload'] for f in desktop.frames
                                if f.get('params', {}).get('type') == 'session.clients.input')
            assert shared_input['text'].endswith('original shared prompt')
            assert shared_input['origin'] == 'discord:linked-thread'
            received = next(f['params']['payload'] for f in desktop.frames
                            if f.get('params', {}).get('type') == 'session.clients.attachment'
                            and f['params']['payload'].get('origin') == 'discord:linked-thread')
            assert received['name'] == upload.name
            assert Path(received['path']).read_bytes() == upload.read_bytes()
            assert len(provider.main_requests()) == 1
            if surface == 'desktop':
                assert await handle_session_client_message(runner, MessageEvent(
                    text='/steer include shared steering', source=source)) == (True, None)
            else:
                steered = await asyncio.to_thread(server.handle_request, {
                    'id': 'steer', 'method': 'session.steer',
                    'params': {'session_id': sid, 'text': 'include shared steering'}})
                assert steered['result']['status'] == 'queued', steered
            # Disconnect EVERY Desktop viewer while the SAME run is live. The
            # owner's persistent mailbox keeps execution alive until completion.
            desktop.close()
            server._close_sessions_for_transport(desktop)
            release.set()
            await _until(lambda: any(e['frame'].get('params', {}).get('type') == 'message.complete'
                                     for e in hub.events(key, 0)))
            await poll_session_clients(runner, home, 'default', {})
            linked_messages = [text for chat, text, _ in adapter.messages if chat == 'linked-thread']
            assert linked_messages.count('same execution completed') == 1
            assert shared_input['text'] not in linked_messages  # no Discord echo
            if surface == 'desktop':
                assert 'include shared steering' not in linked_messages
            if surface == 'discord':
                assert ('creator-thread', shared_input['text']) in [(chat, text) for chat, text, _ in adapter.messages]
            assert any('include shared steering' in str(r['messages']) for r in provider.main_requests())
            # A new Desktop connection reattaches to that owner's runtime, not a
            # cold transcript copy, then observes and cancels the next Discord turn.
            replacement = DesktopPeer()
            token = bind_transport(replacement)
            try:
                resumed = await asyncio.to_thread(server.handle_request, {
                    'id': 'reconnect', 'method': 'session.resume', 'params': {'session_id': key}})
            finally:
                reset_transport(token)
            assert resumed['result']['session_id'] == sid
            # Registration clones RPC functions into server's initialized globals.
            handle_request = server.__dict__['handle_request']
            if surface == 'desktop':
                second = await asyncio.to_thread(handle_request, {
                    'id': 'second', 'method': 'prompt.submit', 'params': {'session_id': sid, 'text': 'second turn'}})
                assert second is not None and second['result']['status'] == 'streaming', second
            else:
                assert await handle_session_client_message(runner, MessageEvent(text='second turn', source=source)) == (True, None)
            await _until(stop_requested.is_set)
            if surface == 'desktop':
                assert await handle_session_client_message(runner, MessageEvent(text='/stop', source=source)) == (True, 'Stop requested.')
            else:
                stopped = await asyncio.to_thread(handle_request, {
                    'id': 'stop', 'method': 'session.interrupt', 'params': {'session_id': sid}})
                assert stopped is not None and stopped['result']['status'] == 'interrupted', stopped
            await _until(lambda: any(f.get('params', {}).get('type') == 'message.complete'
                                     for f in replacement.frames))
            messages = []
            with SessionDB(home / 'state.db') as db:
                messages = db.get_messages_as_conversation(key)
            assert sum(m.get('role') == 'user' and 'original shared prompt' in str(m.get('content')) for m in messages) == 1
            await poll_session_clients(runner, home, 'default', {})
            await poll_session_clients(runner, home, 'default', {})
            assert [text for chat, text, _ in adapter.messages if chat == 'linked-thread'].count('same execution completed') == 1
            assert hub.lookup(key)['pid'] == child.pid
        finally:
            release.set()
            stop.set()
            await asyncio.to_thread(child.join, 15)
            if child.is_alive():
                child.terminate()
                child.join(5)
            if hub:
                hub.close()
            assert child.exitcode == 0

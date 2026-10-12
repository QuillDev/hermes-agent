"""Executable red proof: an active native gateway owner has no broker bridge.

Diagnostic only; kept outside the source tree after execution, not a passing
characterization test that would enshrine the unsafe duplicate-writer behavior.
"""
import asyncio
import threading

import pytest

from tests.gateway.test_session_client_legacy_ownership import _runner, _close_runner
from tests.tui_gateway import test_session_client_regressions as fixtures
from tests.tui_gateway.test_session_client_host import _request
from tests.tui_gateway.test_session_clients_live import DesktopPeer, _until

local_host = fixtures.local_host


@pytest.mark.asyncio
async def test_active_gateway_and_desktop_must_not_run_two_agents(local_host, monkeypatch):
    import hermes_state
    from gateway.platforms.event import MessageEvent
    from tests.fakes.fake_llm_provider import Text
    from tui_gateway import server
    home, _hub, provider = local_host
    monkeypatch.setattr(hermes_state, 'DEFAULT_DB_PATH', home / 'state.db')
    monkeypatch.setattr(server, '_db', None)
    runner, _adapter, source = _runner(home, monkeypatch)
    requested, release = threading.Event(), threading.Event()
    native_turn = None

    def reply(record):
        if len(provider.main_requests()) == 2:
            requested.set()
            assert release.wait(20)
        return Text('native or hosted completion')

    monkeypatch.setattr(provider, '_responder', reply)
    try:
        assert await runner._handle_message(MessageEvent(text='native seed', source=source)) == 'native or hosted completion'
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        native_turn = asyncio.create_task(runner._handle_message(MessageEvent(text='native active turn', source=source)))
        assert await asyncio.to_thread(requested.wait, 10)
        peer = DesktopPeer()
        resumed = await asyncio.to_thread(_request, peer, {'id': 'resume-active', 'method': 'session.resume',
                                 'params': {'session_id': entry.session_id, 'eager_build': True}})
        assert 'result' in resumed, resumed
        sid = resumed['result']['session_id']
        stopped = await asyncio.to_thread(_request, peer, {'id': 'stop-active', 'method': 'session.interrupt',
                                 'params': {'session_id': sid}})
        assert stopped['result']['status'] == 'interrupted', stopped
        release.set()
        native_reply = await native_turn
        native_turn = None
        assert native_reply != 'native or hosted completion', (
            'Desktop acknowledged stop against its new idle agent, but the original '
            'gateway owner still completed its active turn normally')
    finally:
        release.set()
        if native_turn is not None:
            await native_turn
        _close_runner(runner)


def _desktop_process(home, commands, replies):
    """A second OS process enters the real authenticated Desktop RPC seam."""
    import os
    import traceback
    os.environ['HERMES_HOME'] = home
    from tui_gateway import server
    from tui_gateway.session_clients import get_hub
    peer = DesktopPeer()
    hub = get_hub(home)
    try:
        while True:
            request = commands.get(timeout=40)
            if request is None:
                break
            if request['method'] == 'test.frames':
                replies.put({'frames': list(peer.frames), 'local_sessions': list(server._sessions)})
            else:
                replies.put(_request(peer, request))
    except BaseException:
        replies.put({'worker_error': traceback.format_exc()})
        raise
    finally:
        hub.close()


@pytest.mark.asyncio
async def test_cross_process_desktop_steers_original_turn_and_both_clients_execute_once(local_host, monkeypatch):
    import multiprocessing
    import hermes_state
    from hermes_state import SessionDB
    from gateway.platforms.event import MessageEvent
    from tests.fakes.fake_llm_provider import Text, ToolCall
    from tui_gateway import server
    home, hub, provider = local_host
    monkeypatch.setattr(hermes_state, 'DEFAULT_DB_PATH', home / 'state.db')
    monkeypatch.setattr(server, '_db', None)
    runner, adapter, source = _runner(home, monkeypatch)
    runner._wire_adapter_handlers(adapter)
    held, release = threading.Event(), threading.Event()
    stop_held, stop_release = threading.Event(), threading.Event()
    def reply(record):
        if len(provider.main_requests()) == 2:
            held.set()
            assert release.wait(20)
            return ToolCall('terminal', {'command': 'printf incumbent-bridge'})
        if len(provider.main_requests()) == 6:
            stop_held.set()
            assert stop_release.wait(20)
            return Text('late stopped answer must not land')
        return Text('original gateway response')
    monkeypatch.setattr(provider, '_responder', reply)
    ctx = multiprocessing.get_context('spawn')
    commands, replies = ctx.Queue(), ctx.Queue()
    child = ctx.Process(target=_desktop_process, args=(str(home), commands, replies))
    turn = None
    async def rpc(method, **params):
        commands.put({'id': method, 'method': method, 'params': params})
        return await asyncio.to_thread(replies.get, True, 30)
    try:
        assert await runner._handle_message(MessageEvent(text='old Discord history', source=source)) == 'original gateway response'
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        turn = asyncio.create_task(runner._handle_message(MessageEvent(text='held Discord turn', source=source)))
        assert await asyncio.to_thread(held.wait, 10)
        child.start()
        resumed = await rpc('session.resume', session_id=entry.session_id, eager_build=True)
        assert 'result' in resumed, resumed
        sid = resumed['result']['session_id']
        assert resumed['result']['running'] is True
        assert any(m.get('text') == 'old Discord history' for m in resumed['result']['messages']), resumed
        assert (await rpc('test.frames'))['local_sessions'] == []
        steered = await rpc('session.steer', session_id=sid, text='keep the original steer')
        assert steered.get('result', {}).get('status') == 'steered', steered
        release.set()
        assert await turn == 'original gateway response'
        turn = None
        frames = await rpc('test.frames')
        assert 'original gateway response' in ''.join(
            f['params']['payload'].get('text', '') for f in frames['frames']
            if f.get('params', {}).get('type') == 'message.delta'), frames
        assert any('keep the original steer' in str(record.get('messages', []))
                   for record in provider.main_requests()), provider.main_requests()
        desktop = await rpc('prompt.submit', session_id=sid, text='Desktop on original owner')
        assert desktop.get('result', {}).get('status') == 'streaming', desktop
        await _until(lambda: any('Desktop on original owner' in str(r.get('messages', [])) for r in provider.main_requests()))
        await _until(lambda: not runner._is_session_running(entry.session_key))
        await adapter.handle_message(MessageEvent(text='Discord still same destination', source=source))
        await _until(lambda: any('Discord still same destination' in str(r.get('messages', [])) for r in provider.main_requests()))
        await _until(lambda: not adapter._active_sessions)
        with SessionDB(home / 'state.db') as db:
            history = db.get_messages_as_conversation(entry.session_id)
        assert sum(m.get('content') == 'Desktop on original owner' for m in history) == 1
        assert sum(m.get('content') == 'Discord still same destination' for m in history) == 1
        assert len(provider.main_requests()) == 5, provider.main_requests()
        assert len([m for m in adapter.messages if m[1] == 'original gateway response']) == 2, adapter.messages
        assert all(m[0] == 'existing-thread' for m in adapter.messages)
        assert hub.links('discord') == []
        assert (await rpc('test.frames'))['local_sessions'] == []
        replay = await rpc('session.events.since', session_id=sid, last_seen=0)
        assert replay['result']['open_requests'] == []
        finals = [e for e in replay['result']['events'] if e['type'] == 'message.complete']
        assert len(finals) == 4
        assert len({e['payload']['persisted_turn']['final_assistant_row_id'] for e in finals}) == 4
        repeated = await rpc('session.resume', session_id=entry.session_id)
        assert repeated['result']['session_id'] == sid
        assert len(provider.main_requests()) == 5
        interrupted_prompt = await rpc('prompt.submit', session_id=sid, text='hold for cross process stop')
        assert interrupted_prompt['result']['status'] == 'streaming'
        assert await asyncio.to_thread(stop_held.wait, 10)
        active = await rpc('session.active_list', current_session_id=sid)
        assert any(row['id'] == sid and row['status'] == 'working' for row in active['result']['sessions']), active
        stopped = await rpc('session.interrupt', session_id=sid)
        assert stopped['result']['status'] == 'interrupted', stopped
        stop_release.set()
        await _until(lambda: not adapter._active_sessions)
        assert len(provider.main_requests()) == 6
        with SessionDB(home / 'state.db') as db:
            stopped_history = db.get_messages_as_conversation(entry.session_id)
        assert not any(m.get('content') == 'late stopped answer must not land' for m in stopped_history)
        assert not any(m[1] == 'late stopped answer must not land' for m in adapter.messages)
        assert (await rpc('test.frames'))['local_sessions'] == []
    finally:
        release.set()
        stop_release.set()
        if turn:
            await turn
        if child.pid:
            commands.put(None)
            await asyncio.to_thread(child.join, 10)
            if child.is_alive():
                child.terminate()
                await asyncio.to_thread(child.join, 5)
        _close_runner(runner)


@pytest.mark.asyncio
async def test_desktop_first_discovers_native_route_before_any_gateway_turn(local_host, monkeypatch):
    import hermes_state
    from gateway.session_client_bridge import start_gateway_bridge
    from tui_gateway import server
    home, _hub, provider = local_host
    monkeypatch.setattr(hermes_state, 'DEFAULT_DB_PATH', home / 'state.db')
    monkeypatch.setattr(server, '_db', None)
    runner, adapter, source = _runner(home, monkeypatch)
    runner._wire_adapter_handlers(adapter)
    try:
        await start_gateway_bridge(runner)
        entry = runner.session_store.get_or_create_session(source)
        runner.session_store.append_to_transcript(entry.session_id, {'role': 'user', 'content': 'persisted native question'})
        runner.session_store.append_to_transcript(entry.session_id, {'role': 'assistant', 'content': 'persisted native answer'})
        peer = DesktopPeer()
        resumed = await asyncio.to_thread(_request, peer, {'id': 'desktop-first', 'method': 'session.resume',
                                                         'params': {'session_id': entry.session_id, 'eager_build': True}})
        assert 'result' in resumed, resumed
        assert not server._sessions, 'Desktop cold-resume created a twin instead of discovering the native route'
        assert resumed['result']['stored_session_id'] == entry.session_id
        assert any(m.get('text') == 'persisted native answer' for m in resumed['result']['messages'])
        submitted = await asyncio.to_thread(_request, peer, {'id': 'first-prompt', 'method': 'prompt.submit', 'params': {
            'session_id': resumed['result']['session_id'], 'text': 'first prompt from Desktop'}})
        assert submitted['result']['status'] == 'streaming', submitted
        await _until(lambda: any(m[1] == 'local hosted answer' for m in adapter.messages))
        assert len(provider.main_requests()) == 1
        assert adapter.messages[-1][0] == 'existing-thread'
    finally:
        _close_runner(runner)


@pytest.mark.asyncio
async def test_enabling_bridge_discovers_and_stops_already_held_original_agent(local_host, monkeypatch):
    import hermes_state
    from gateway.platforms.event import MessageEvent
    from gateway.session_clients import session_client_watcher
    from tests.fakes.fake_llm_provider import Text
    from tui_gateway import server
    home, _hub, provider = local_host
    monkeypatch.setattr(hermes_state, 'DEFAULT_DB_PATH', home / 'state.db')
    monkeypatch.setattr(server, '_db', None)
    config = home / 'config.yaml'
    config.write_text(config.read_text().replace('enabled: true', 'enabled: false'))
    runner, adapter, source = _runner(home, monkeypatch)
    runner._wire_adapter_handlers(adapter)
    held, release = threading.Event(), threading.Event()
    turn = watcher = None
    def reply(record):
        if len(provider.main_requests()) == 2:
            held.set()
            assert release.wait(20)
        return Text('original pre opt-in answer')
    monkeypatch.setattr(provider, '_responder', reply)
    try:
        assert await runner._handle_message(MessageEvent(text='pre opt-in seed', source=source)) == 'original pre opt-in answer'
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        runner._running = True
        watcher = asyncio.create_task(session_client_watcher(runner))
        turn = asyncio.create_task(runner._handle_message(MessageEvent(text='pre opt-in held turn', source=source)))
        assert await asyncio.to_thread(held.wait, 10)
        config.write_text(config.read_text().replace('enabled: false', 'enabled: true'))
        peer = DesktopPeer()
        resumed = await asyncio.to_thread(_request, peer, {'id': 'enable-resume', 'method': 'session.resume',
                                                         'params': {'session_id': entry.session_id, 'eager_build': True}})
        assert 'result' in resumed, resumed
        assert not server._sessions
        sid = resumed['result']['session_id']
        stopped = await asyncio.to_thread(_request, peer, {'id': 'enable-stop', 'method': 'session.interrupt',
                                                         'params': {'session_id': sid}})
        assert stopped['result']['status'] == 'interrupted'
        release.set()
        assert await turn != 'original pre opt-in answer'
        turn = None
        assert len(provider.main_requests()) == 2
        assert provider.main_requests()[0]['messages'][0] == provider.main_requests()[1]['messages'][0]
    finally:
        release.set()
        if turn:
            await turn
        runner._running = False
        if watcher:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
        _close_runner(runner)


@pytest.mark.asyncio
async def test_held_incumbent_keeps_native_fifo_and_desktop_and_discord_prompts_execute_once(local_host, monkeypatch):
    import hermes_state
    from hermes_state import SessionDB
    from gateway.platforms.event import MessageEvent
    from tests.fakes.fake_llm_provider import Text
    from tui_gateway import server
    home, _hub, provider = local_host
    monkeypatch.setattr(hermes_state, 'DEFAULT_DB_PATH', home / 'state.db')
    monkeypatch.setattr(server, '_db', None)
    runner, adapter, source = _runner(home, monkeypatch)
    runner._wire_adapter_handlers(adapter)
    held, release = threading.Event(), threading.Event()
    def reply(record):
        if len(provider.main_requests()) == 1:
            held.set()
            assert release.wait(20)
        return Text('queued native answer')
    monkeypatch.setattr(provider, '_responder', reply)
    try:
        await adapter.handle_message(MessageEvent(text='held original prompt', source=source))
        assert await asyncio.to_thread(held.wait, 10)
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        pin = entry.prompt_pin
        peer = DesktopPeer()
        resumed = await asyncio.to_thread(_request, peer, {'id': 'queue-resume', 'method': 'session.resume',
                                                         'params': {'session_id': entry.session_id, 'eager_build': True}})
        assert resumed['result']['running'] is True
        sid = resumed['result']['session_id']
        desktop = await asyncio.to_thread(_request, peer, {'id': 'queue-desktop', 'method': 'prompt.submit',
                                                         'params': {'session_id': sid, 'text': 'queued Desktop prompt'}})
        assert desktop['result']['status'] == 'queued', desktop
        await adapter.handle_message(MessageEvent(text='/queue queued Discord prompt', source=source))
        assert runner._queue_depth(entry.session_key, adapter=adapter) == 2
        assert len(provider.main_requests()) == 1
        assert entry.prompt_pin == pin
        release.set()
        await _until(lambda: len(provider.main_requests()) == 3 and not adapter._active_sessions)
        with SessionDB(home / 'state.db') as db:
            history = db.get_messages_as_conversation(entry.session_id)
        for text in ('held original prompt', 'queued Desktop prompt', 'queued Discord prompt'):
            assert sum(m.get('content') == text for m in history) == 1
        assert len([m for m in adapter.messages if m[1] == 'queued native answer']) == 3
        assert all(m[0] == 'existing-thread' for m in adapter.messages)
        system = provider.main_requests()[0]['messages'][0]
        assert all(r['messages'][0] == system for r in provider.main_requests())
        assert not server._sessions
    finally:
        release.set()
        await _until(lambda: not adapter._active_sessions)
        _close_runner(runner)

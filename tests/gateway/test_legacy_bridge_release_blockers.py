"""Release blockers exercised with real native RPC, GatewayRunner and SQLite."""
import asyncio
import sqlite3
import threading
from dataclasses import replace

import pytest

from tests.gateway.test_session_client_legacy_bridge_boundaries import _rpc, _setup
from tests.gateway.test_session_client_legacy_ownership import _close_runner, _runner
from tests.tui_gateway import test_session_client_regressions as fixtures
from tests.tui_gateway.test_session_clients_live import DesktopPeer, _until

local_host = fixtures.local_host


@pytest.mark.asyncio
@pytest.mark.parametrize('busy', [False, True])
@pytest.mark.parametrize('text', ['/new', '/approve', '/queue /new', '  @file:private\n/approve'])
async def test_desktop_commands_rejected_before_mailbox_and_native_execution(local_host, monkeypatch, busy, text):
    from gateway.platforms.event import MessageEvent
    from tests.fakes.fake_llm_provider import Text
    from tui_gateway.session_clients import SessionClientHub
    home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    client = SessionClientHub(home)
    held, release = threading.Event(), threading.Event()
    turn = None

    def reply(record):
        if len(provider.main_requests()) == 2:
            held.set()
            assert release.wait(20)
        return Text('native answer')

    monkeypatch.setattr(provider, '_responder', reply)
    try:
        await runner._handle_message(MessageEvent(text='native seed', source=source))
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        agent = runner._cached_agent_for(entry.session_key)
        before = hub.lookup(entry.session_id)
        if busy:
            turn = asyncio.create_task(runner._handle_message(MessageEvent(text='held turn', source=source)))
            assert await asyncio.to_thread(held.wait, 10)
        with sqlite3.connect(hub.path) as db:
            db.execute('CREATE TABLE command_audit(request TEXT)')
            db.execute('CREATE TRIGGER audit_command AFTER INSERT ON commands BEGIN INSERT INTO command_audit VALUES (NEW.request); END')
        with pytest.raises(ValueError):
            await asyncio.to_thread(client.call, entry.session_id, 'prompt.submit', {'text': text})
        with sqlite3.connect(hub.path) as db:
            assert db.execute('SELECT request FROM command_audit').fetchall() == []
        forged = {'id': 'forged-command', 'method': 'prompt.submit',
                  'params': {'session_id': before['sid'], 'text': text}}
        rejected = await asyncio.to_thread(hub._execute, entry.session_id, forged)
        assert rejected.get('error', {}).get('code') == 4092, rejected
        assert runner.session_store.lookup_by_session_key(entry.session_key).session_id == entry.session_id
        assert hub.lookup(entry.session_id) == before
        assert runner._cached_agent_for(entry.session_key) is agent
        assert runner._queue_depth(entry.session_key, adapter=adapter) == 0
        assert len(provider.main_requests()) == (2 if busy else 1)
    finally:
        release.set()
        if turn is not None:
            await turn
        client.close()
        _close_runner(runner)


@pytest.mark.asyncio
@pytest.mark.parametrize('busy', [False, True])
async def test_plain_desktop_answers_remain_conversation_through_native_fifo(local_host, monkeypatch, busy):
    from gateway.platforms.event import MessageEvent
    from tests.fakes.fake_llm_provider import Text
    from tools import clarify_gateway
    from tui_gateway import server
    _home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    held, release = threading.Event(), threading.Event()
    pending = None

    def reply(record):
        if len(provider.main_requests()) == 2 and busy:
            held.set()
            assert release.wait(20)
        return Text('native conversation answer')

    monkeypatch.setattr(provider, '_responder', reply)
    try:
        await runner._handle_message(MessageEvent(text='seed', source=source))
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        agent = runner._cached_agent_for(entry.session_key)
        peer = DesktopPeer()
        resumed = await _rpc(peer, 'session.resume', session_id=entry.session_id)
        sid = resumed['result']['session_id']
        if busy:
            await adapter.handle_message(MessageEvent(text='held native turn', source=source))
            assert await asyncio.to_thread(held.wait, 10)
            await adapter.handle_message(MessageEvent(text='/queue Discord first', source=source))
        pending = clarify_gateway.register('native-dialog', entry.session_key, 'Choose', ['yes', 'no'])
        submitted = await _rpc(peer, 'prompt.submit', session_id=sid, text='yes')
        assert submitted.get('result', {}).get('status') == ('queued' if busy else 'streaming'), submitted
        if busy:
            second = await _rpc(peer, 'prompt.submit', session_id=sid, text='Desktop third')
            assert second['result']['status'] == 'queued'
            assert runner._queue_depth(entry.session_key, adapter=adapter) == 3
            queued = [adapter._pending_messages[entry.session_key], *runner._overflow_queue(entry.session_key)]
            desktop_events = [event for event in queued if event.text in {'yes', 'Desktop third'}]
            assert len(desktop_events) == 2
            assert all(event.allow_gateway_control is False and event._gateway_accepted
                       and event.metadata['_session_client_origin'] == 'desktop'
                       and event.source == source for event in desktop_events)
            assert pending.response is None and not pending.event.is_set()
        release.set()
        await _until(lambda: pending.response == 'yes' or
                     (len(provider.main_requests()) == (5 if busy else 2) and not adapter._active_sessions))
        # Native turn cleanup cancels obsolete clarify prompts; it must not
        # resolve one with Desktop's conversational answer.
        assert pending.response in (None, clarify_gateway.CANCELLED)
        history = runner.session_store._db_for_key(entry.session_key).get_messages_as_conversation(entry.session_id)
        users = [m['content'] for m in history if m['role'] == 'user']
        assert users == (['seed', 'held native turn', 'Discord first', 'yes', 'Desktop third'] if busy else ['seed', 'yes'])
        assert runner._cached_agent_for(entry.session_key) is agent
        system = provider.main_requests()[0]['messages'][0]
        assert all(record['messages'][0] == system for record in provider.main_requests())
        assert all(message[0] == 'existing-thread' for message in adapter.messages)
        inputs = [e['frame']['params']['payload'] for e in hub.events(entry.session_id, 0)
                  if e['frame']['params']['type'] == 'session.clients.input']
        assert any(p['text'] == 'yes' and p['row_id'] for p in inputs)
        if not busy:
            assert any(p['text'] == 'yes' and p['origin'] == 'desktop' for p in inputs)
        assert not server._sessions
    finally:
        release.set()
        if pending is not None:
            clarify_gateway.resolve_gateway_clarify('native-dialog', 'cleanup')
            clarify_gateway.wait_for_response('native-dialog', 1)
        await _until(lambda: not adapter._active_sessions)
        _close_runner(runner)


def _persist_old_owner(home, key, retired):
    """A real previous native process leaves its conversation admission evidence."""
    import os
    from pathlib import Path
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner
    from gateway.session_client_bridge import register_entry
    os.environ['HERMES_HOME'] = home

    async def previous_generation():
        runner = GatewayRunner(GatewayConfig(sessions_dir=Path(home) / 'sessions', multiplex_profiles=False))
        entry = next(e for e in runner.session_store.list_sessions() if e.session_id == key)
        owner = register_entry(runner, entry, runner._restored_source(entry))
        if retired:
            owner.hub.retire(owner.key, owner.sid)
            owner.hub.close()
            _close_runner(runner)
        else:
            os._exit(0)

    asyncio.run(previous_generation())


@pytest.mark.asyncio
@pytest.mark.parametrize('retired', [False, True])
async def test_startup_isolates_persisted_conversation_fences_and_connects_unrelated_route(local_host, monkeypatch, retired):
    import multiprocessing
    from gateway.platforms.event import MessageEvent
    from gateway.session_client_bridge import start_gateway_bridge
    from tui_gateway import server
    home, hub, provider, runner, _adapter, source = _setup(local_host, monkeypatch)
    old = runner.session_store.get_or_create_session(source)
    runner.session_store.append_to_transcript(old.session_id, {'role': 'user', 'content': 'old accepted history'})
    unrelated_source = replace(source, chat_id='unaffected-thread', thread_id='unaffected-thread')
    normal = runner.session_store.get_or_create_session(unrelated_source)
    runner.session_store.append_to_transcript(normal.session_id, {'role': 'user', 'content': 'unaffected history'})
    _close_runner(runner)
    child = multiprocessing.get_context('spawn').Process(target=_persist_old_owner,
                                                         args=(str(home), old.session_id, retired))
    child.start()
    await asyncio.to_thread(child.join, 10)
    assert child.exitcode == 0
    before = hub.lookup(old.session_id)
    assert before and before['online'] == (0 if retired else 1)
    restarted, adapter, restored_source = _runner(home, monkeypatch)
    restarted._wire_adapter_handlers(adapter)
    try:
        assert restarted.session_store.lookup_by_session_key(old.session_key).session_id == old.session_id
        await start_gateway_bridge(restarted)
        assert await adapter.connect() is True
        assert hub.lookup(old.session_id) == before
        assert hub.lookup(normal.session_id)['online']
        peer = DesktopPeer()
        fenced = await _rpc(peer, 'session.resume', session_id=old.session_id, eager_build=True)
        assert 'error' in fenced, fenced
        assert 'owner' in fenced['error']['message'].lower()
        assert not server._sessions
        blocked = await restarted._handle_message(MessageEvent(text='do not replay old route', source=restored_source))
        assert blocked and 'no replacement run' in blocked
        assert provider.main_requests() == []
        other_source = replace(restored_source, chat_id='unaffected-thread', thread_id='unaffected-thread')
        await adapter.handle_message(MessageEvent(text='unrelated traffic connects', source=other_source))
        await _until(lambda: len(provider.main_requests()) == 1 and not adapter._active_sessions)
        assert any(m[0] == 'unaffected-thread' and m[1] == 'local hosted answer' for m in adapter.messages)
        assert hub.lookup(old.session_id) == before
        history = restarted.session_store._db_for_key(old.session_key).get_messages_as_conversation(old.session_id)
        assert not any('do not replay' in str(m.get('content')) for m in history)
    finally:
        _close_runner(restarted)


@pytest.mark.asyncio
@pytest.mark.parametrize('preowned', [False, True])
async def test_long_verified_compression_keeps_root_authority_and_excludes_branch(local_host, monkeypatch, preowned):
    from gateway.platforms.event import MessageEvent
    from gateway.session_client_bridge import register_entry, start_gateway_bridge
    from tui_gateway import server
    _home, hub, provider, runner, _adapter, source = _setup(local_host, monkeypatch)
    try:
        await start_gateway_bridge(runner)
        if preowned:
            await runner._handle_message(MessageEvent(text='original root prompt', source=source))
        entry = runner.session_store.get_or_create_session(source)
        root = entry.session_id
        peer = DesktopPeer()
        original = hub.lookup(root)
        agent = runner._cached_agent_for(entry.session_key)
        db = runner.session_store._db_for_key(entry.session_key)
        parent = root
        ids = [root]
        for index in range(130):
            child = f'compression-{index}'
            db.end_session(parent, end_reason='compression')
            db.create_session(child, source='discord', parent_session_id=parent)
            ids.append(child)
            parent = child
        db.append_message(parent, 'user', 'tip question')
        db.append_message(parent, 'assistant', 'tip answer')
        entry = runner.session_store.switch_session(entry.session_key, parent)
        if preowned:
            await start_gateway_bridge(runner)
            assert hub.lookup(root)['online'], 'Long compression prematurely retired the original owner'
        resumed = await _rpc(peer, 'session.resume', session_id=root)
        assert 'result' in resumed, resumed
        sid = resumed['result']['session_id']
        if original:
            assert sid == original['sid']
        assert resumed['result']['stored_session_id'] == parent
        assert runner._cached_agent_for(entry.session_key) is agent
        owner = hub.lookup(root)
        assert all(hub.lookup(key) == owner for key in ids)
        repeated = await _rpc(peer, 'session.resume', session_id=ids[5])
        assert repeated['result']['session_id'] == sid
        active = await _rpc(peer, 'session.activate', session_id=sid)
        assert active['result']['stored_session_id'] == parent
        assert len(provider.main_requests()) == (1 if preowned else 0)
        assert not server._sessions
        # A branch's failed registration must not destroy the established actor.
        db.create_session('occupied-branch', source='discord', parent_session_id=parent,
                          model_config={'_branched_from': parent})
        branch_entry = runner.session_store.switch_session(entry.session_key, 'occupied-branch')
        hub.claim('occupied-branch', 'other-actor')
        with pytest.raises(RuntimeError):
            register_entry(runner, branch_entry, source)
        assert hub.lookup(root) == owner
        assert hub.lookup('occupied-branch')['sid'] == 'other-actor'
        db.create_session('unowned-branch', source='discord', parent_session_id=parent,
                          model_config={'_branched_from': parent})
        branch_entry = runner.session_store.switch_session(entry.session_key, 'unowned-branch')
        register_entry(runner, branch_entry, source)
        branch = await _rpc(peer, 'session.resume', session_id='unowned-branch')
        assert branch['result']['session_id'] != sid
        assert branch['result']['stored_session_id'] == 'unowned-branch'
        old_control = await _rpc(peer, 'session.activate', session_id=sid)
        assert 'error' in old_control
        assert hub.lookup(root)['online'] == 0
    finally:
        _close_runner(runner)

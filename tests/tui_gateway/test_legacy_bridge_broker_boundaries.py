"""Legacy ownership boundaries through real Desktop RPC and SQLite mailboxes."""
import asyncio
import json
import time

import pytest

from tests.gateway.test_session_client_legacy_bridge_boundaries import _setup, _rpc
from tests.gateway.test_session_client_legacy_ownership import _close_runner
from tests.tui_gateway import test_session_client_regressions as fixtures
from tests.tui_gateway.test_session_clients_live import DesktopPeer, _until

local_host = fixtures.local_host


@pytest.mark.parametrize('state', ['dead', 'retired', 'live', 'unknown'])
def test_executionless_discovery_generation_fences(tmp_path, monkeypatch, state):
    import psutil
    from gateway.session_client_bridge import GATEWAY_KEY
    from tui_gateway.session_clients import SessionClientHub
    old = SessionClientHub(tmp_path, start=False)
    new = SessionClientHub(tmp_path, start=False)
    consumer = lambda request: {'id': request['id'], 'result': {}}
    try:
        old.register_external(GATEWAY_KEY, '', consumer)
        if state == 'retired':
            old.retire(GATEWAY_KEY, '')
        elif state == 'dead':
            with old._db() as db:
                db.execute('UPDATE owners SET born=-1 WHERE session_key=?', (GATEWAY_KEY,))
        elif state == 'unknown':
            def inaccessible(pid):
                raise psutil.AccessDenied(pid)
            monkeypatch.setattr(psutil, 'Process', inaccessible)
        if state in {'live', 'unknown'}:
            with pytest.raises(RuntimeError):
                new.register_service(GATEWAY_KEY, '', consumer)
            assert new.lookup(GATEWAY_KEY)['owner'] == old.owner
        else:
            new.register_service(GATEWAY_KEY, '', consumer)
            assert new.lookup(GATEWAY_KEY)['owner'] == new.owner
            assert new.lookup(GATEWAY_KEY)['online'] == 1
            new.register_service(GATEWAY_KEY, '', consumer)
            with pytest.raises(RuntimeError):
                new.register_service(GATEWAY_KEY, '', lambda request: {})
        with pytest.raises((RuntimeError, ValueError)):
            new.register_service('conversation', 'gateway:actor', consumer)
    finally:
        new.close()
        old.close()


@pytest.mark.parametrize('retired', [False, True])
def test_discovery_api_cannot_replace_conversation_or_task_owner(tmp_path, retired):
    from gateway.session_client_bridge import GATEWAY_KEY
    from tui_gateway.session_clients import SessionClientHub
    old = SessionClientHub(tmp_path, start=False)
    new = SessionClientHub(tmp_path, start=False)
    consumer = lambda request: {}
    try:
        old.register_external(GATEWAY_KEY, 'gateway:actual-actor', consumer)
        if retired:
            old.retire(GATEWAY_KEY, 'gateway:actual-actor')
        with pytest.raises(RuntimeError):
            new.register_service(GATEWAY_KEY, '', consumer)
        assert new.lookup(GATEWAY_KEY)['sid'] == 'gateway:actual-actor'
        with pytest.raises(RuntimeError):
            new.register_external(GATEWAY_KEY, 'gateway:new-actor', consumer)
        assert new.lookup(GATEWAY_KEY)['owner'] == old.owner
    finally:
        new.close()
        old.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('state', ['dead', 'retired'])
async def test_new_gateway_start_replaces_only_executionless_discovery(local_host, monkeypatch, state):
    from gateway.session_client_bridge import GATEWAY_KEY, GatewayDiscovery, start_gateway_bridge
    from tui_gateway.session_clients import SessionClientHub
    home, hub, provider, runner, _adapter, _source = _setup(local_host, monkeypatch)
    old = SessionClientHub(home, start=False)
    try:
        old.register_service(GATEWAY_KEY, '', GatewayDiscovery(runner, old))
        if state == 'retired':
            old.retire(GATEWAY_KEY, '')
        else:
            with old._db() as db:
                db.execute('UPDATE owners SET born=-1 WHERE session_key=?', (GATEWAY_KEY,))
        await start_gateway_bridge(runner)
        assert hub.lookup(GATEWAY_KEY)['owner'] == hub.owner
        active = await asyncio.to_thread(hub.call, GATEWAY_KEY, 'session.active_list', {})
        assert active['result']['sessions'] == [], active
        old.retire(GATEWAY_KEY, '')
        assert hub.lookup(GATEWAY_KEY)['online'] == 1
        assert provider.main_requests() == []
    finally:
        old.close()
        _close_runner(runner)


@pytest.mark.asyncio
@pytest.mark.parametrize('method,extra', [
    ('prompt.submit', {'display_kind': 'hidden'}),
    ('prompt.submit', {'queued': True}),
    ('prompt.submit', {'voice_turn': True}),
    ('prompt.submit', {'unknown_secret_field': 'sensitive-value'}),
    ('session.history', {}),
    ('config.set', {'key': 'model', 'value': 'sensitive-value'}),
])
async def test_forged_mailbox_prompt_rejected_before_owner_admission(local_host, monkeypatch, method, extra):
    from gateway.platforms.event import MessageEvent
    _home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    marker = 'forged-sensitive-payload-must-not-echo'
    try:
        assert await runner._handle_message(MessageEvent(text='native seed', source=source)) == 'local hosted answer'
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        row = hub.lookup(entry.session_id)
        with pytest.raises(ValueError):
            await asyncio.to_thread(hub.call, entry.session_id, method, {'text': marker, **extra})
        with hub._db() as db:
            assert not any(marker in value['request'] for value in db.execute('SELECT request FROM commands'))
        request = {'id': 'forged', 'method': method, 'params': {
            'session_id': row['sid'], 'text': marker, **extra}}
        with hub._db() as db:
            # Enter beneath hub.call: on-disk mailbox contents are untrusted at
            # execution, even if a writer bypasses normal ingress validation.
            db.execute('INSERT INTO commands VALUES (?,?,?,?,?,?,NULL)',
                       ('forged', hub.owner, row['session_key'], json.dumps(request), time.time() + 30, 'pending'))
        def response():
            with hub._db() as db:
                value = db.execute('SELECT response FROM commands WHERE id=?', ('forged',)).fetchone()
                return json.loads(value['response']) if value and value['response'] else None
        await _until(lambda: response() is not None)
        result = response()
        assert 'error' in result, result
        assert result['error']['message'] == 'Unsupported incumbent gateway control'
        assert marker not in json.dumps(result)
        with hub._db() as db:
            persisted = db.execute('SELECT response FROM commands WHERE id=?', ('forged',)).fetchone()['response']
        assert marker not in persisted
        assert len(provider.main_requests()) == 1
        assert not any(marker in str(message) for message in adapter.messages)
        db = runner.session_store._db_for_key(entry.session_key)
        assert not any(marker in str(message) for message in db.get_messages_as_conversation(entry.session_id))
    finally:
        await _until(lambda: not adapter._active_sessions)
        _close_runner(runner)


@pytest.mark.asyncio
async def test_compressed_desktop_reconnect_streams_and_retains_stable_incumbent(local_host, monkeypatch):
    from gateway.platforms.event import MessageEvent
    from gateway.session_client_bridge import start_gateway_bridge
    _home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    try:
        assert await runner._handle_message(MessageEvent(text='root question', source=source)) == 'local hosted answer'
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        root = entry.session_id
        incumbent = runner._cached_agent_for(entry.session_key)
        db = runner.session_store._db_for_key(entry.session_key)
        db.end_session(root, end_reason='compression')
        db.create_session('reconnected-compression-tip', source='discord', parent_session_id=root)
        db.append_message('reconnected-compression-tip', 'user', 'compressed question')
        db.append_message('reconnected-compression-tip', 'assistant', 'compressed answer')
        runner.session_store.switch_session(entry.session_key, 'reconnected-compression-tip')
        await start_gateway_bridge(runner)
        peer = DesktopPeer()
        resumed = await _rpc(peer, 'session.resume', session_id=root)
        assert resumed['result']['stored_session_id'] == 'reconnected-compression-tip'
        sid = resumed['result']['session_id']
        second = DesktopPeer()
        again = await _rpc(second, 'session.resume', session_id='reconnected-compression-tip')
        assert again['result']['session_id'] == sid
        assert hub.has_viewers(root), 'Viewer lease must retain the stable publishing/cache owner'
        incumbent._last_activity_ts = 0
        assert runner._sweep_idle_cached_agents() == 0
        assert runner._cached_agent_for(entry.session_key) is incumbent
        submitted = await _rpc(peer, 'prompt.submit', session_id=sid, text='reconnected new turn')
        assert submitted['result']['status'] == 'streaming'
        await _until(lambda: not adapter._active_sessions and len(provider.main_requests()) == 2)
        await _until(lambda: any(f.get('params', {}).get('session_id') == sid and
                                f['params'].get('type') == 'message.complete' for f in peer.frames))
        assert hub.event_head(root) == hub.event_head('reconnected-compression-tip')
        assert hub.events(root, 0) == hub.events('reconnected-compression-tip', 0)
        closed = await _rpc(peer, 'session.close', session_id=sid)
        assert closed['result']['closed'] is True
        assert hub.has_viewers(root), 'One detached socket must not release another viewer'
        closed = await _rpc(second, 'session.close', session_id=sid)
        assert closed['result']['closed'] is True
        assert not hub.has_viewers(root)
        assert not hub.has_viewers('reconnected-compression-tip')
        # Native ended-row recovery may replace the pre-compression cache on
        # submission; age the actual current cache, not that discarded actor.
        incumbent = runner._cached_agent_for(entry.session_key)
        incumbent._last_activity_ts = 0
        assert runner._sweep_idle_cached_agents() == 1
    finally:
        await _until(lambda: not adapter._active_sessions)
        _close_runner(runner)

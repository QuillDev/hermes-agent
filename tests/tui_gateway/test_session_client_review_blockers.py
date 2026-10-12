"""Broker snapshots never carry dialogs; hosted turns remain visible to watchdog RPCs."""
import asyncio
import json
import sqlite3
import threading

import psutil
import pytest

from tests.tui_gateway import test_session_client_regressions as fixtures
from tests.tui_gateway.test_session_client_host import _request
from tests.tui_gateway.test_session_clients_live import DesktopPeer, _until

hosted = fixtures.hosted
local_host = fixtures.local_host


@pytest.mark.parametrize('method', ['session.activate', 'session.resume', 'session.events.since'])
def test_broker_snapshots_cancel_preexisting_dialogs_without_storing_them(local_host, method):
    from tui_gateway import server, server_requests
    home, hub, provider = local_host
    peer = DesktopPeer()
    # Native sessions can already have unanswered questions when explicitly shared.
    created = _request(peer, {'id': 'native', 'method': 'session.create', 'params': {}})['result']
    sid = created['session_id']
    outcomes = []
    settle = server_requests.send_async('secret', sid, {
        'env_var': 'SYNTHETIC_TEST_KEY', 'prompt': 'synthetic-dialog-canary',
        'metadata': {'private': 'synthetic-dialog-metadata'},
    }, outcomes.append)
    assert server_requests.open_requests(sid)
    try:
        shared = _request(peer, {'id': 'share', 'method': 'session.clients.share', 'params': {'session_id': sid}})
        assert 'result' in shared, shared
        key = server._sessions[sid]['session_key']
        response = hub.call(key, method, {})
        assert 'result' in response, response
        assert not response['result'].get('open_requests'), response
        assert server_requests.open_requests(sid) == []
        assert outcomes == [None]
        assert not any('synthetic-dialog' in json.dumps(event) for event in hub.events(key, 0))
        # Include WAL pages: deleting a leaked response after acknowledgement is not enough.
        for path in home.joinpath('session-clients').glob('broker.sqlite*'):
            assert b'synthetic-dialog' not in path.read_bytes()
        assert provider.main_requests() == []
    finally:
        settle('test cleanup')
        server_requests.cancel(sid)


@pytest.mark.parametrize('method,params', [
    ('vault.unlock_prompt', {'backend': 'bitwarden', 'display_name': 'Synthetic vault'}),
    ('vault.save_login', {'origin': 'https://example.test', 'site': 'Synthetic site'}),
    ('approval', {'request_id': 'synthetic-approval', 'command': 'synthetic-dialog-command'}),
    ('clarify', {'questions': [{'qid': 'q', 'question': 'synthetic-dialog-question'}]}),
])
def test_hosted_dialogs_fail_immediately_without_pending_cards_or_capability_claims(local_host, method, params):
    from tui_gateway import server, server_requests
    from tui_gateway.session_client_host import HOST_KEY
    _home, hub, provider = local_host
    created = hub.call(HOST_KEY, 'session.create', {})['result']
    sid, key = created['session_id'], created['stored_session_id']
    transport = server._sessions[sid]['transport']
    server_requests.advertise(transport, True)
    outcomes = []
    settle = server_requests.send_async(method, sid, params, outcomes.append)
    try:
        assert outcomes == [None]  # No deadline or client response is needed to settle.
        assert server_requests.send(method, sid, params, timeout=None) is None
        assert not server_requests.answers_requests(transport)
        assert server_requests.open_requests(sid) == []
        assert server_requests.pending_kind(sid) == ''
        response = hub.call(key, 'session.activate', {})
        assert response['result']['status'] != 'waiting'
        assert not response['result'].get('open_requests')
        assert not any('synthetic-dialog' in json.dumps(event) for event in hub.events(key, 0))
        assert provider.main_requests() == []
    finally:
        settle('test cleanup')
        server_requests.cancel(sid)
        server_requests.forget(transport)


@pytest.mark.asyncio
async def test_viewing_backend_lists_silent_hosted_turn_and_dead_owner_without_failover(hosted, monkeypatch):
    from tui_gateway import server
    from tui_gateway.session_client_host import HOST_KEY
    _home, hub, _peer, provider, rpc = hosted
    requested, release = threading.Event(), threading.Event()
    def reply(record):
        requested.set()
        assert release.wait(30)
        from tests.fakes.fake_llm_provider import Text
        return Text('silent hosted turn finished')
    monkeypatch.setattr(provider, '_responder', reply)
    created = rpc('session.create', source='desktop')['result']
    sid, key = created['session_id'], created['stored_session_id']
    original_owner = hub.lookup(HOST_KEY)
    assert sid not in server._sessions
    try:
        assert rpc('prompt.submit', session_id=sid, text='silent hosted turn')['result']['status'] == 'streaming'
        assert await asyncio.to_thread(requested.wait, 15)
        # The watchdog uses precisely this RPC while no stream events arrive.
        listed = rpc('session.active_list', current_session_id=sid)
        assert 'result' in listed, listed
        rows = {row['id']: row for row in listed['result']['sessions']}
        assert rows[sid]['status'] == 'working', listed
        assert rows[sid]['current'] is True
        assert hub.lookup(HOST_KEY) == original_owner
        release.set()
        await _until(lambda: any(e['frame'].get('params', {}).get('payload', {}).get('text') == 'silent hosted turn finished'
                                 for e in hub.events(key, 0)))
        await _until(lambda: not rpc('session.activate', session_id=sid)['result']['running'])
        assert next(row for row in rpc('session.active_list')['result']['sessions'] if row['id'] == sid)['status'] == 'idle'
        process = psutil.Process(original_owner['pid'])
        process.terminate()
        await asyncio.to_thread(process.wait, 15)
        assert rpc('session.active_list')['result']['sessions'] == []
        assert hub.lookup(HOST_KEY)['owner'] == original_owner['owner']
        rejected = rpc('prompt.submit', session_id=sid, text='must not replace owner')
        assert rejected.get('error', {}).get('code') == 4092, rejected
        assert len(provider.main_requests()) == 1
        with sqlite3.connect(hub.path) as db:
            assert not any('must not replace owner' in row[0] for row in db.execute('SELECT request FROM commands'))
    finally:
        release.set()


@pytest.mark.parametrize('method', ['session.activate', 'session.resume', 'session.events.since'])
def test_cross_broker_snapshots_leave_native_dialog_answerable_but_never_persist_it(local_host, method):
    from tui_gateway import server_requests
    from tui_gateway.session_clients import SessionClientHub
    home, _host, provider = local_host
    native_peer = DesktopPeer()
    created = _request(native_peer, {'id': 'native', 'method': 'session.create', 'params': {}})['result']
    sid, key = created['session_id'], created['stored_session_id']
    outcomes = []
    settle = server_requests.send_async('secret', sid, {
        'env_var': 'SYNTHETIC_TEST_KEY', 'prompt': 'synthetic-dialog-canary',
    }, outcomes.append)
    legacy = SessionClientHub(home, start=False)
    viewer = SessionClientHub(home, start=False)
    try:
        # Legacy native ownership can retain its original answering transport.
        # The second client is still forbidden from replaying its dialogs.
        legacy.share(sid)
        legacy.start()
        # A real SQLite trigger observes the exact durable value BEFORE the
        # viewer acknowledges/deletes it; sanitizing on read is too late.
        with sqlite3.connect(legacy.path) as db:
            db.executescript('''
                CREATE TABLE response_audit (response TEXT);
                CREATE TRIGGER audit_response AFTER UPDATE OF response ON commands
                WHEN NEW.response IS NOT NULL BEGIN
                    INSERT INTO response_audit VALUES (NEW.response);
                END;
            ''')
        response = viewer.call(key, method, {})
        assert 'result' in response, response
        pending = server_requests.open_requests(sid)
        assert len(pending) == 1
        assert outcomes == []
        with sqlite3.connect(legacy.path) as db:
            written = [row[0] for row in db.execute('SELECT response FROM response_audit')]
            assert len(written) == 1
            assert not any('synthetic-dialog' in row for row in written)
            assert not json.loads(written[0])['result'].get('open_requests')
            assert not response['result'].get('open_requests'), response
            assert not any('synthetic-dialog' in str(tuple(row)) for row in db.execute('SELECT * FROM commands'))
        for path in home.joinpath('session-clients').glob('broker.sqlite*'):
            assert b'synthetic-dialog' not in path.read_bytes()
        assert server_requests.resolve_response({'id': pending[0]['id'], 'result': {'value': 'synthetic-native-answer'}}, native_peer)
        assert outcomes == [{'value': 'synthetic-native-answer'}]
        assert provider.main_requests() == []
    finally:
        settle('test cleanup')
        server_requests.cancel(sid)
        viewer.close()
        legacy.close()


def test_active_list_separates_native_profiles_and_does_not_start_absent_hosts(local_host, tmp_path):
    from tests.fakes.fake_llm_provider import write_hermes_home
    from tui_gateway import server
    from tui_gateway.session_client_host import HOST_KEY
    from hermes_cli.profiles import get_profile_dir
    home, hub, provider = local_host
    secondary_path = get_profile_dir('secondary')
    assert secondary_path.is_relative_to(tmp_path)
    secondary = write_hermes_home(secondary_path, provider.base_url)
    peer = DesktopPeer()
    first = _request(peer, {'id': 'launch', 'method': 'session.create', 'params': {}})['result']
    other_response = _request(peer, {'id': 'other', 'method': 'session.create', 'params': {'profile': 'secondary'}})
    assert 'result' in other_response, other_response
    other = other_response['result']
    host = hub.lookup(HOST_KEY)
    launch = _request(peer, {'id': 'launch-list', 'method': 'session.active_list', 'params': {'current_session_id': other['session_id']}})
    named = _request(peer, {'id': 'named-list', 'method': 'session.active_list', 'params': {'profile': 'secondary', 'current_session_id': other['session_id']}})
    assert [row['id'] for row in launch['result']['sessions']] == [first['session_id']]
    assert not launch['result']['sessions'][0]['current']
    assert [row['id'] for row in named['result']['sessions']] == [other['session_id']]
    assert named['result']['sessions'][0]['current']
    assert hub.lookup(HOST_KEY) == host
    assert not (secondary / 'session-clients').exists()
    rejected = _request(peer, {'id': 'invalid', 'method': 'session.active_list', 'params': {'profile': 'missing-profile'}})
    assert 'error' in rejected
    assert hub.lookup(HOST_KEY) == host
    assert home.exists()
    assert provider.main_requests() == []


@pytest.mark.platforms('posix')
def test_unresponsive_live_host_is_unknown_not_an_empty_watchdog_snapshot(hosted):
    from tui_gateway.session_client_host import HOST_KEY
    _home, hub, _peer, provider, rpc = hosted
    created = rpc('session.create', source='desktop')['result']
    owner = hub.lookup(HOST_KEY)
    process = psutil.Process(owner['pid'])
    process.suspend()
    try:
        listed = rpc('session.active_list', current_session_id=created['session_id'])
        assert listed.get('error', {}).get('code') == 4092, listed
        assert 'result' not in listed
        assert hub.lookup(HOST_KEY) == owner
        assert provider.main_requests() == []
        with sqlite3.connect(hub.path) as db:
            pending = [json.loads(row[0]) for row in db.execute('SELECT request FROM commands')]
        assert all(request['method'] == 'session.active_list' for request in pending)
    finally:
        process.resume()


@pytest.mark.asyncio
@pytest.mark.parametrize('isolated', [False, True])
async def test_hosted_model_dialog_attempts_settle_in_native_and_isolated_turns(hosted, monkeypatch, isolated):
    from tests.fakes.fake_llm_provider import Text, ToolCall
    home, hub, peer, provider, rpc = hosted
    config = home / 'config.yaml'
    config.write_text(config.read_text() + f'\ndashboard:\n  turn_isolation: {str(isolated).lower()}\n')
    def reply(record):
        if len(provider.main_requests()) == 1:
            return ToolCall('clarify', {'questions': [{'question': 'Synthetic unsupported question'}]})
        return Text('unsupported dialog settled')
    monkeypatch.setattr(provider, '_responder', reply)
    from hermes_state import SessionDB
    # Create prewarms on a timer and can legitimately select native execution.
    # Cold resume explicitly selects lazy isolation or a built native agent.
    stored_key = 'synthetic-dialog-admission'
    with SessionDB(home / 'state.db') as db:
        db.create_session(stored_key, source='desktop')
    resumed = rpc('session.resume', session_id=stored_key, source='desktop',
                  **({'lazy': True} if isolated else {'eager_build': True}))
    assert 'result' in resumed, resumed
    created = resumed['result']
    sid, key = created['session_id'], created['stored_session_id']
    try:
        sent = rpc('prompt.submit', session_id=sid, text='exercise a native clarification request')
        assert sent['result']['status'] == 'streaming', sent
        assert bool(sent['result'].get('turn_isolation')) is isolated
        await _until(lambda: any(e['frame'].get('params', {}).get('payload', {}).get('text') == 'unsupported dialog settled'
                                 for e in hub.events(key, 0)), timeout=15)
        requests = provider.main_requests()
        assert len(requests) == 2
        tool_reply = next(m for m in reversed(requests[-1]['messages']) if m['role'] == 'tool')
        result = json.loads(tool_reply['content'])
        assert result['outcome'] == 'undelivered', result
        assert all(item['user_response'] is None for item in result['responses'])
        assert not rpc('session.activate', session_id=sid)['result'].get('open_requests')
        assert not any(f.get('method') == 'clarify' for f in peer.frames)
    finally:
        rpc('session.interrupt', session_id=sid)

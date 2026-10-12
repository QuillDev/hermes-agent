"""Bounded admission rollout: real OS hosts, native RPC/agent and fake model HTTP."""
from __future__ import annotations

import asyncio
import multiprocessing
import os
from pathlib import Path
import sqlite3
import threading
import traceback

import pytest

LEGACY_KEY = '__hermes_session_execution_host__'


def _legacy_process(home, ready, stop):
    os.environ['HERMES_HOME'] = home
    os.environ['HERMES_RUNTIME_DIR'] = str(Path(home) / 'session-clients' / 'runtime')
    try:
        import hermes_bootstrap
        from tui_gateway import server
        from tui_gateway.session_clients import get_hub
        hub = get_hub(home, execution_host=True, service_key=LEGACY_KEY)
        # A pre-upgrade native consumer has no forum-create hook.
        import gateway.session_forum_sync as forum
        forum.record_native_creation = lambda *a, **kw: None
        result = hub.call(LEGACY_KEY, 'session.create', {
            'source': 'desktop', 'idempotency_key': 'legacy-lost-response', 'model': 'original-held-model'})
        ready.put(result)
        stop.wait(60)
        server._shutdown_sessions()
        hub.close()
    except BaseException:
        ready.put({'failure': traceback.format_exc()})
        raise


@pytest.fixture
def rollout(tmp_path, monkeypatch):
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text, write_hermes_home
    from tui_gateway import server
    from tui_gateway.session_clients import get_hub
    from tests.tui_gateway.test_session_client_host import _request
    from tests.tui_gateway.test_session_clients_live import DesktopPeer

    with FakeLLMServer(lambda record: Text('completed exactly once')) as provider:
        home = write_hermes_home(tmp_path / 'profile', provider.base_url, extra_config=(
            'session_clients:\n  enabled: true\n  discord_forum:\n    automatic: true\n'
            '    activation_id: generation-test\n    guild_id: "123"\n    forum_id: "456"\n'
            'memory:\n  memory_enabled: false\n  user_profile_enabled: false\n'
            'dashboard:\n  turn_isolation: false\n'))
        monkeypatch.setenv('HERMES_HOME', str(home))
        monkeypatch.setattr(server, '_hermes_home', home)
        hub = get_hub(home)
        children = []
        peer = DesktopPeer()
        def rpc(method, **params):
            return _request(peer, {'id': method, 'method': method, 'params': params})
        def legacy():
            ctx = multiprocessing.get_context('spawn')
            ready, stop = ctx.Queue(), ctx.Event()
            child = ctx.Process(target=_legacy_process, args=(str(home), ready, stop))
            children.append((child, stop))
            child.start()
            result = ready.get(timeout=30)
            assert 'result' in result, result
            return child, result['result']
        try:
            yield home, hub, provider, rpc, legacy
        finally:
            for child, stop in children:
                # Termination can strand the Event condition lock in wait().
                if child.is_alive():
                    stop.set()
                child.join(15)
                if child.is_alive():
                    child.terminate()
                    child.join(10)
            import psutil
            from tui_gateway.session_client_host import HOST_KEY
            row = hub.lookup(HOST_KEY)
            if row and row['pid'] != os.getpid() and psutil.pid_exists(row['pid']):
                process = psutil.Process(row['pid'])
                if process.create_time() == row['born']:
                    process.terminate()
                    try:
                        process.wait(15)
                    except psutil.TimeoutExpired:
                        process.kill()
                        process.wait(5)
            hub.close()


@pytest.mark.parametrize('method', ['session.create', 'session.branch_stored'])
@pytest.mark.parametrize('token', [42, True, False, [1], {'key': 'value'}, [], {}, '', ' ', '\t\n', '\u2003', ' padded-token ', ' hermes-host-v2:padded'])
def test_invalid_create_key_is_rejected_before_any_admission(rollout, method, token):
    from tui_gateway import server
    home, hub, provider, rpc, _legacy = rollout
    with sqlite3.connect(hub.path) as db:
        db.executescript('''
            CREATE TABLE command_admission_audit (request TEXT);
            CREATE TRIGGER audit_admission AFTER INSERT ON commands BEGIN
                INSERT INTO command_admission_audit VALUES (NEW.request);
            END;
        ''')
    before_sessions = set(server._sessions)
    responses = []
    for _ in range(2):
        params = {'source': 'desktop', 'idempotency_key': token}
        if method == 'session.branch_stored':
            params['parent_session_id'] = 'not-admitted-parent'
        responses.append(rpc(method, **params))
    # Inspect durable and runtime effects even if the response is wrong.
    with sqlite3.connect(hub.path) as db:
        counts = {table: db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
                  for table in ('owners', 'create_routes', 'commands', 'command_admission_audit')}
    assert counts == dict.fromkeys(counts, 0), (responses, counts)
    assert set(server._sessions) == before_sessions
    if (home / 'state.db').exists():
        with sqlite3.connect(home / 'state.db') as db:
            assert db.execute('SELECT COUNT(*) FROM sessions').fetchone()[0] == 0
    assert provider.requests == []
    assert not (home / 'session-clients' / 'host.log').exists()
    assert all(r.get('error', {}).get('code') == -32602 for r in responses), responses
    assert all('idempotency_key' in r['error']['message'] for r in responses)


@pytest.mark.parametrize('params', [{}, {'idempotency_key': None}])
def test_absent_or_null_create_key_keeps_unkeyed_native_semantics(rollout, params):
    from tui_gateway.session_client_host import HOST_KEY
    _home, hub, provider, rpc, _legacy = rollout
    first = rpc('session.create', source='desktop', **params)['result']
    second = rpc('session.create', source='desktop', **params)['result']
    assert first['session_id'] != second['session_id']
    assert hub.lookup(first['stored_session_id'])['owner'] == hub.lookup(second['stored_session_id'])['owner']
    assert hub.lookup(HOST_KEY)['owner'] == hub.lookup(first['stored_session_id'])['owner']
    with sqlite3.connect(hub.path) as db:
        assert db.execute('SELECT COUNT(*) FROM create_routes').fetchone()[0] == 0
    assert provider.main_requests() == []


@pytest.mark.parametrize('token', ['opaque interior space:\u03bb', 'hermes-host-v2:exact interior space:\u03bb'])
def test_valid_create_key_is_forwarded_exactly_and_deduplicated(rollout, token):
    import hashlib
    import json
    from tui_gateway.session_client_host import HOST_KEY
    home, hub, provider, rpc, _legacy = rollout
    with sqlite3.connect(hub.path) as db:
        db.executescript('''
            CREATE TABLE exact_admission_audit (request TEXT);
            CREATE TRIGGER audit_exact AFTER INSERT ON commands BEGIN
                INSERT INTO exact_admission_audit VALUES (NEW.request);
            END;
        ''')
    first = rpc('session.create', source='desktop', idempotency_key=token)['result']
    owner = hub.lookup(first['stored_session_id'])
    retry = rpc('session.create', source='desktop', idempotency_key=token)['result']
    assert retry['session_id'] == first['session_id']
    assert retry['stored_session_id'] == first['stored_session_id']
    assert hub.lookup(first['stored_session_id']) == owner
    with sqlite3.connect(hub.path) as db:
        requests = [json.loads(row[0]) for row in db.execute('SELECT request FROM exact_admission_audit')]
        assert [r['params']['idempotency_key'] for r in requests] == [token, token]
        assert list(db.execute('SELECT identity,service_key,owner FROM create_routes')) == [
            (hashlib.sha256(token.encode()).hexdigest(), HOST_KEY, owner['owner'])]
    with sqlite3.connect(home / 'session_clients_gateway.db') as db:
        assert list(db.execute('SELECT identity FROM forum_create_decisions')) == [(hashlib.sha256(token.encode()).hexdigest(),)]
    assert provider.main_requests() == []


def test_new_desktop_create_uses_updated_native_host_without_moving_legacy(rollout):
    from gateway.session_forum_sync import automatic_config
    home, hub, provider, rpc, legacy = rollout
    child, original = legacy()
    old = hub.lookup(original['stored_session_id'])
    fresh = rpc('session.create', source='desktop')
    assert 'result' in fresh, fresh
    new = hub.lookup(fresh['result']['stored_session_id'])
    assert new['pid'] != old['pid'] == child.pid
    assert hub.lookup(original['stored_session_id']) == old
    assert hub.lookup(LEGACY_KEY)['owner'] == old['owner']
    resumed = rpc('session.resume', session_id=original['stored_session_id'])
    assert resumed['result']['session_id'] == original['session_id'], resumed
    with sqlite3.connect(home / 'session_clients_gateway.db') as db:
        enrolled = dict(db.execute('SELECT session_key,scope FROM forum_enrollments'))
    assert enrolled[fresh['result']['stored_session_id']] == automatic_config(home)['scope']
    assert original['stored_session_id'] not in enrolled
    listed = rpc('session.active_list')
    assert {original['session_id'], fresh['result']['session_id']} <= {
        row['id'] for row in listed['result']['sessions']}
    assert provider.main_requests() == []


def test_legacy_lost_create_response_retries_on_the_original_process(rollout):
    _home, hub, provider, rpc, legacy = rollout
    child, original = legacy()
    owner = hub.lookup(original['stored_session_id'])
    retry = rpc('session.create', source='desktop', idempotency_key='legacy-lost-response', model='original-held-model')
    assert retry['result']['session_id'] == original['session_id'], retry
    assert retry['result']['stored_session_id'] == original['stored_session_id']
    assert hub.lookup(original['stored_session_id']) == owner
    assert owner['pid'] == child.pid
    assert provider.main_requests() == []


@pytest.mark.parametrize('token', ['legacy-lost-response', 'legacy-unknown-token'])
def test_dead_legacy_create_owner_is_not_probed_or_replaced(rollout, token):
    from tui_gateway.session_client_host import HOST_KEY
    _home, hub, provider, rpc, legacy = rollout
    child, _original = legacy()
    old_service = hub.lookup(LEGACY_KEY)
    child.terminate()
    child.join(15)
    assert not child.is_alive()
    failed = rpc('session.create', source='desktop', idempotency_key=token)
    assert failed.get('error', {}).get('code') == 4092, failed
    assert hub.lookup(LEGACY_KEY)['owner'] == old_service['owner']
    assert hub.lookup(HOST_KEY) is None
    assert provider.main_requests() == []


def test_marked_new_key_is_exact_and_retries_one_new_canonical_with_legacy_live(rollout):
    import hashlib
    from tui_gateway.session_client_host import HOST_KEY, new_create_key
    home, hub, provider, rpc, legacy = rollout
    child, _original = legacy()
    token = new_create_key()
    created = rpc('session.create', source='desktop', idempotency_key=token)['result']
    assert hub.lookup(created['stored_session_id'])['pid'] != child.pid
    for _ in range(3):
        retry = rpc('session.create', source='desktop', idempotency_key=token)
        assert retry['result']['session_id'] == created['session_id']
        assert retry['result']['stored_session_id'] == created['stored_session_id']
    with sqlite3.connect(home / 'session_clients_gateway.db') as db:
        assert list(db.execute('SELECT identity FROM forum_create_decisions')) == [(hashlib.sha256(token.encode()).hexdigest(),)]
    with sqlite3.connect(hub.path) as db:
        assert list(db.execute('SELECT service_key,owner FROM create_routes')) == [(HOST_KEY, hub.lookup(HOST_KEY)['owner'])]
    assert provider.main_requests() == []


def test_first_install_unmarked_key_is_bound_before_dispatch_and_survives_later_legacy_boot(rollout):
    import hashlib
    from tui_gateway.session_client_host import HOST_KEY
    _home, hub, provider, rpc, legacy = rollout
    token = 'opaque-user-token-do-not-rewrite'
    with sqlite3.connect(hub.path) as db:
        db.executescript('''
            CREATE TABLE admission_audit (service_key TEXT, owner TEXT);
            CREATE TRIGGER audit_create_route AFTER INSERT ON commands
            WHEN json_extract(NEW.request, '$.params.idempotency_key') IS NOT NULL
            BEGIN
                INSERT INTO admission_audit SELECT service_key,owner FROM create_routes
                    WHERE service_key=NEW.session_key AND owner=NEW.owner;
            END;
        ''')
    created = rpc('session.create', source='desktop', idempotency_key=token)['result']
    new = hub.lookup(created['stored_session_id'])
    child, _original = legacy()
    assert child.pid != new['pid']
    retry = rpc('session.create', source='desktop', idempotency_key=token)
    assert retry['result']['session_id'] == created['session_id'], retry
    with sqlite3.connect(hub.path) as db:
        assert list(db.execute('SELECT * FROM admission_audit')) == [(HOST_KEY, new['owner']), (HOST_KEY, new['owner'])]
        route = db.execute('SELECT service_key,owner FROM create_routes WHERE identity=?', (hashlib.sha256(token.encode()).hexdigest(),)).fetchone()
    assert route == (HOST_KEY, new['owner'])
    assert provider.main_requests() == []


def test_marked_key_never_recreates_after_bound_new_host_dies(rollout):
    import psutil
    from tui_gateway.session_client_host import HOST_KEY, new_create_key
    _home, hub, provider, rpc, _legacy = rollout
    token = new_create_key()
    created = rpc('session.create', source='desktop', idempotency_key=token)['result']
    owner = hub.lookup(created['stored_session_id'])
    process = psutil.Process(owner['pid'])
    process.terminate()
    process.wait(15)
    retry = rpc('session.create', source='desktop', idempotency_key=token)
    assert retry.get('error', {}).get('code') == 4092, retry
    assert hub.lookup(HOST_KEY)['owner'] == owner['owner']
    assert provider.main_requests() == []


@pytest.mark.asyncio
async def test_old_held_model_and_fifo_finish_once_while_new_native_create_works(rollout, monkeypatch):
    from tests.fakes.fake_llm_provider import Text
    from tests.tui_gateway.test_session_clients_live import _until
    from hermes_state import SessionDB
    home, hub, provider, rpc, legacy = rollout
    entered, release = threading.Event(), threading.Event()
    def respond(record):
        body = record['body']
        if body['model'] == 'original-held-model' and not entered.is_set():
            entered.set()
            assert release.wait(30)
        return Text('answer:' + body['messages'][-1]['content'])
    monkeypatch.setattr(provider, '_responder', respond)
    child, original = legacy()
    key, sid = original['stored_session_id'], original['session_id']
    old = hub.lookup(key)
    try:
        resumed = rpc('session.resume', session_id=key)
        assert resumed['result']['session_id'] == sid
        assert rpc('prompt.submit', session_id=sid, text='held original')['result']['status'] == 'streaming'
        assert await asyncio.to_thread(entered.wait, 15)
        assert rpc('prompt.submit', session_id=sid, text='FIFO queued', queued=True)['result']['status'] == 'queued'
        fresh = rpc('session.create', source='desktop', model='updated-model')['result']
        assert hub.lookup(fresh['stored_session_id'])['pid'] != child.pid
        assert rpc('prompt.submit', session_id=fresh['session_id'], text='fresh new')['result']['status'] == 'streaming'
        await _until(lambda: any(e['frame'].get('params', {}).get('payload', {}).get('text') == 'answer:fresh new'
                                for e in hub.events(fresh['stored_session_id'], 0)))
        assert hub.lookup(key) == old
        assert len([r for r in provider.main_requests() if r['model'] == 'original-held-model']) == 1
    finally:
        release.set()
    await _until(lambda: len([e for e in hub.events(key, 0) if e['frame'].get('params', {}).get('type') == 'message.complete']) == 2)
    assert hub.lookup(key) == old
    assert rpc('session.resume', session_id=key)['result']['session_id'] == sid
    requests = [r for r in provider.main_requests() if r['model'] == 'original-held-model']
    assert len(requests) == 2
    assert requests[0]['messages'][-1]['content'].startswith('held original')
    assert requests[1]['messages'][-1]['content'] == 'FIFO queued'
    assert requests[1]['messages'][0] == requests[0]['messages'][0]
    with SessionDB(home / 'state.db') as db:
        messages = db.get_messages_as_conversation(key)
    assert [m['content'] for m in messages if m['role'] == 'user'] == ['held original', 'FIFO queued']

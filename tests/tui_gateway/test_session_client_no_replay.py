"""Hosted mailboxes exclude profile credentials and never replay crash-interrupted work."""
import asyncio
import json
import sqlite3
import threading
import time

import psutil
import pytest

from tests.tui_gateway import test_session_client_regressions as fixtures
from tests.tui_gateway.test_session_client_host import _request
from tests.tui_gateway.test_session_clients_live import DesktopPeer, _until

hosted = fixtures.hosted
local_host = fixtures.local_host


def test_profile_credentials_never_enter_broker_even_before_acknowledgement(local_host):
    from tui_gateway.session_clients import SessionClientHub
    home, host, provider = local_host
    secret = 'synthetic-expanded-credential-canary'
    (home / '.env').write_text(f'SESSION_TEST_CREDENTIAL={secret}\n')
    config = home / 'config.yaml'
    config.write_text(config.read_text() + '\nsynthetic_provider:\n  api_key: ${SESSION_TEST_CREDENTIAL}\n')
    peer = DesktopPeer()
    created = _request(peer, {'id': 'create', 'method': 'session.create', 'params': {'source': 'desktop'}})['result']
    key, sid = created['stored_session_id'], created['session_id']
    viewer = SessionClientHub(home, start=False)
    host.start()
    # Hold a real connection so the WAL remains observable after delete-on-ack.
    with sqlite3.connect(host.path) as audit:
        audit.executescript('''
            CREATE TABLE credential_response_audit (response TEXT);
            CREATE TRIGGER credential_audit AFTER UPDATE OF response ON commands
            WHEN NEW.response IS NOT NULL BEGIN
                INSERT INTO credential_response_audit VALUES (NEW.response);
            END;
        ''')
        try:
            full = _request(peer, {'id': 'full', 'method': 'config.get', 'params': {'session_id': sid, 'key': 'full'}})
            assert full['result']['config']['synthetic_provider']['api_key'] == secret, full
            try:
                viewer.call(key, 'config.get', {'key': 'full'})
            except ValueError as exc:
                assert 'session-specific getter' in str(exc)
            written = [row[0] for row in audit.execute('SELECT response FROM credential_response_audit')]
            assert all(secret not in row for row in written), written
            with pytest.raises(ValueError, match='session-specific getter'):
                viewer.call(key, 'config.get', {'key': 'full'})
            # An untrusted producer cannot bypass the ingress check with a forged
            # mailbox row: the execution boundary must reject it before dispatch.
            request = {'id': 'forged-full', 'method': 'config.get', 'params': {'session_id': sid, 'key': 'full'}}
            audit.execute('INSERT INTO commands VALUES (?,?,?,?,?,?,NULL)',
                          ('forged-full', host.owner, key, json.dumps(request), time.time() + 15, 'pending'))
            audit.commit()
            response = None
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                response = audit.execute('SELECT response FROM commands WHERE id=?', ('forged-full',)).fetchone()[0]
                if response:
                    break
                time.sleep(0.03)
            assert response and 'error' in json.loads(response), response
            changed = viewer.call(key, 'config.set', {'key': 'reasoning', 'value': 'low'})
            assert changed['result']['value'] == 'low', changed
            assert viewer.call(key, 'config.get', {'key': 'reasoning'})['result']['value'] == 'low'
            assert viewer.call(key, 'config.get', {'key': 'fast'})['result']['value'] == 'normal'
            written = [row[0] for row in audit.execute('SELECT response FROM credential_response_audit')]
            assert written and all(secret not in row for row in written)
            for path in home.joinpath('session-clients').glob('broker.sqlite*'):
                assert secret.encode() not in path.read_bytes(), path
            assert provider.main_requests() == []
        finally:
            viewer.close()


def test_viewing_profile_getters_resolve_the_hosted_profile_without_broker_commands(local_host, tmp_path):
    from hermes_cli.profiles import get_profile_dir
    from tests.fakes.fake_llm_provider import write_hermes_home
    from tui_gateway.session_clients import get_hub
    home, _hub, provider = local_host
    named_home = get_profile_dir('secondary')
    assert named_home.is_relative_to(tmp_path)
    named = write_hermes_home(named_home, provider.base_url, extra_config='session_clients:\n  enabled: true\n')
    for path, value in [(home, 'synthetic-launch-only'), (named, 'synthetic-secondary-only')]:
        (path / '.env').write_text(f'SCOPED_CONFIG_CREDENTIAL={value}\n')
        config = path / 'config.yaml'
        config.write_text(config.read_text() + '\nsynthetic_provider:\n  api_key: ${SCOPED_CONFIG_CREDENTIAL}\n')
    named_host = get_hub(named, execution_host=True, start=False)
    peer = DesktopPeer()
    try:
        created = _request(peer, {'id': 'secondary-create', 'method': 'session.create',
                                 'params': {'source': 'desktop', 'profile': 'secondary'}})['result']
        sid = created['session_id']
        for key in ['full', 'theme', 'provider']:
            response = _request(peer, {'id': key, 'method': 'config.get', 'params': {'session_id': sid, 'key': key}})
            assert 'result' in response, response
            if key == 'full':
                assert response['result']['config']['synthetic_provider']['api_key'] == 'synthetic-secondary-only'
                assert 'synthetic-launch-only' not in json.dumps(response)
        foreign = _request(peer, {'id': 'foreign-profile', 'method': 'config.get',
                                 'params': {'session_id': sid, 'key': 'full', 'profile': 'default'}})
        assert foreign.get('error', {}).get('code') == 4092, foreign
        for profile_home in [home, named]:
            with sqlite3.connect(profile_home / 'session-clients' / 'broker.sqlite') as db:
                assert list(db.execute('SELECT * FROM commands')) == []
            for path in profile_home.joinpath('session-clients').glob('broker.sqlite*'):
                assert b'synthetic-secondary-only' not in path.read_bytes()
                assert b'synthetic-launch-only' not in path.read_bytes()
        assert provider.main_requests() == []
    finally:
        named_host.close()


async def _interrupt_by_host_death(hosted, monkeypatch, isolated):
    from tests.fakes.fake_llm_provider import Text
    from tui_gateway.session_client_host import HOST_KEY
    from tui_gateway.turn_marker import read_turn_marker, marker_writer_state
    home, hub, _peer, provider, rpc = hosted
    config = home / 'config.yaml'
    config.write_text(config.read_text() + f'\ndashboard:\n  turn_isolation: {str(isolated).lower()}\n')
    requested, release = threading.Event(), threading.Event()
    def reply(record):
        if len(provider.main_requests()) == 2:
            requested.set()
            assert release.wait(30)
        return Text('durable seed answer' if len(provider.main_requests()) == 1 else 'new explicit answer')
    monkeypatch.setattr(provider, '_responder', reply)
    created = rpc('session.create', source='desktop', model='hosted-pinned-model')['result']
    sid, key = created['session_id'], created['stored_session_id']
    assert rpc('prompt.submit', session_id=sid, text='durable seed prompt')['result']['status'] == 'streaming'
    await _until(lambda: any(e['frame'].get('params', {}).get('payload', {}).get('text') == 'durable seed answer'
                             for e in hub.events(key, 0)))
    await _until(lambda: not rpc('session.activate', session_id=sid)['result']['running'])
    assert rpc('prompt.submit', session_id=sid, text='already-admitted interrupted task')['result']['status'] == 'streaming'
    assert await asyncio.to_thread(requested.wait, 15)
    marker = read_turn_marker(home, key)
    assert marker and marker['prompt'] == 'already-admitted interrupted task', marker
    process = psutil.Process(hub.lookup(HOST_KEY)['pid'])
    children = process.children(recursive=True)
    try:
        # SIGKILL: no graceful marker retirement or shutdown hooks can mask replay.
        for child in children:
            if child.is_running() and child.status() != psutil.STATUS_ZOMBIE:
                child.kill()
        process.kill()
        await asyncio.to_thread(process.wait, 10)
        await _until(lambda: marker_writer_state(marker) == 'dead')
    finally:
        release.set()
    return home, hub, provider, rpc, key, sid, marker


@pytest.mark.asyncio
@pytest.mark.parametrize('isolated', [False, True])
async def test_hosted_native_and_isolated_markers_are_non_retryable(hosted, monkeypatch, isolated):
    _home, _hub, provider, _rpc, _key, _sid, marker = await _interrupt_by_host_death(hosted, monkeypatch, isolated)
    assert marker['auto_continue'] is False, marker
    assert len(provider.main_requests()) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize('isolated', [False, True])
@pytest.mark.parametrize('resume_params', [{}, {'eager_build': True}, {'defer_history': True}])
async def test_explicit_reopen_restores_runtime_without_replaying_legacy_interrupted_task(hosted, monkeypatch, isolated, resume_params):
    from tui_gateway.turn_marker import _marker_path
    home, hub, provider, rpc, key, old_sid, marker = await _interrupt_by_host_death(hosted, monkeypatch, isolated)
    # Existing builds wrote retryable markers. Preserve the actual dead writer
    # identity/fresh timestamp; do not let the new marker bit hide the resume bug.
    marker['auto_continue'] = True
    _marker_path(home).write_text(json.dumps({key: marker}))
    resumed = rpc('session.resume', session_id=key, **resume_params)
    assert 'result' in resumed, resumed
    sid = resumed['result']['session_id']
    assert sid != old_sid
    assert not resumed['result'].get('auto_continue'), resumed
    await _until(lambda: not rpc('session.activate', session_id=sid)['result'].get('hydrating'))
    # Observe a bounded quiet interval after hydration: no external request may
    # be sent just by opening, even when the old marker said it was retryable.
    await asyncio.sleep(2)
    assert len(provider.main_requests()) == 2, provider.main_requests()
    snapshot = rpc('session.activate', session_id=sid)['result']
    assert not snapshot['running'], snapshot
    assert 'durable seed answer' in json.dumps(snapshot), snapshot
    assert rpc('prompt.submit', session_id=sid, text='NEW explicit prompt')['result']['status'] == 'streaming'
    await _until(lambda: any(e['frame'].get('params', {}).get('payload', {}).get('text') == 'new explicit answer'
                             for e in hub.events(key, 0)), timeout=15)
    requests = provider.main_requests()
    assert len(requests) == 3
    assert requests[-1]['model'] == 'hosted-pinned-model'
    # Canonical replay merges consecutive user messages when the interrupted
    # turn had no assistant output; preserved history is not an automatic retry.
    assert requests[-1]['messages'][-1]['content'].endswith('NEW explicit prompt')
    assert not any('app or its backend process stopped' in json.dumps(m) for m in requests[-1]['messages'])
    assert 'durable seed answer' in json.dumps(requests[-1]['messages'])


@pytest.mark.asyncio
async def test_unshared_native_resume_still_recovers_a_fresh_dead_writer(local_host):
    import subprocess
    import sys
    from hermes_state import SessionDB
    from tui_gateway.turn_marker import read_turn_marker, marker_writer_state
    home, _hub, provider = local_host
    config = home / 'config.yaml'
    config.write_text(config.read_text().replace('enabled: true', 'enabled: false'))
    key = 'unshared-native-recovery'
    with SessionDB(home / 'state.db') as db:
        db.create_session(key, source='desktop')
        db.append_message(key, 'user', 'native stored prompt')
        db.append_message(key, 'assistant', 'native stored answer')
    writer = await asyncio.to_thread(subprocess.run, [sys.executable, '-c',
        'import sys; from tui_gateway.turn_marker import record_turn_start; '
        'record_turn_start(sys.argv[1], sys.argv[2], "normal native interrupted task")', str(home), key],
        capture_output=True, text=True, timeout=15, check=False)
    assert writer.returncode == 0, writer.stderr
    marker = read_turn_marker(home, key)
    assert marker and marker_writer_state(marker) == 'dead'
    peer = DesktopPeer()
    resumed = await asyncio.to_thread(_request, peer, {
        'id': 'unshared-resume', 'method': 'session.resume', 'params': {'session_id': key, 'eager_build': True}})
    assert resumed['result']['auto_continue']['attempt'] == 1, resumed
    await _until(lambda: any(f.get('params', {}).get('type') == 'message.complete' for f in peer.frames))
    requests = provider.main_requests()
    assert len(requests) == 1
    assert 'normal native interrupted task' in requests[0]['messages'][-1]['content']
    assert 'app or its backend process stopped' in requests[0]['messages'][-1]['content']
    assert 'native stored answer' in json.dumps(requests[0]['messages'])
    assert read_turn_marker(home, key) is None

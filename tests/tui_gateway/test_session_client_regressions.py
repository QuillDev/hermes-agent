"""Hosted RPC parity, single producers/admission, and native runtime retirement."""
import asyncio
from pathlib import Path
import sqlite3
import threading

import psutil
import pytest
import hermes_yaml as yaml

from tests.fakes.fake_llm_provider import FakeLLMServer, Text, write_hermes_home
from tests.tui_gateway.test_session_client_host import _request
from tests.tui_gateway.test_session_clients_live import DesktopPeer, _until


@pytest.fixture
def hosted(tmp_path, monkeypatch):
    from tui_gateway import server
    from tui_gateway.session_clients import get_hub
    from tui_gateway.session_client_host import HOST_KEY

    with FakeLLMServer(lambda record: Text('hosted regression answer')) as provider:
        home = write_hermes_home(tmp_path / 'profile', provider.base_url, extra_config=(
            'session_clients:\n  enabled: true\n'
            'memory:\n  memory_enabled: false\n  user_profile_enabled: false\n'))
        monkeypatch.setenv('HERMES_HOME', str(home))
        monkeypatch.setattr(server, '_hermes_home', home)
        hub = get_hub(home)
        peer = DesktopPeer()
        def rpc(method, **params):
            return _request(peer, {'id': method, 'method': method, 'params': params})
        try:
            yield home, hub, peer, provider, rpc
        finally:
            host = hub.lookup(HOST_KEY)
            if host and psutil.pid_exists(host['pid']):
                process = psutil.Process(host['pid'])
                if process.create_time() == host['born']:
                    process.terminate()
                    try:
                        process.wait(15)
                    except psutil.TimeoutExpired:
                        process.kill()
                        process.wait(5)
            hub.close()


@pytest.mark.asyncio
async def test_hosted_desktop_controls_and_private_branches_do_not_publish_to_parent(hosted, tmp_path):
    home, hub, _peer, provider, rpc = hosted
    parent = rpc('session.create', source='desktop')['result']
    sid, key = parent['session_id'], parent['stored_session_id']
    changed = rpc('config.set', session_id=sid, key='reasoning', value='low')
    assert changed.get('result', {}).get('value') == 'low', changed
    theme = rpc('config.set', session_id=sid, key='theme', value='dark')
    assert theme.get('result', {}).get('value') == 'dark', theme
    config = yaml.safe_load((home / 'config.yaml').read_text())
    assert config['display']['tui_theme'] == 'dark'
    assert config.get('agent', {}).get('reasoning_effort') != 'low'
    with pytest.raises(ValueError, match='not a shared-session control'):
        hub.call(key, 'model.save_key', {'api_key': 'do-not-store-this-credential'})
    with sqlite3.connect(hub.path) as db:
        assert not any('do-not-store-this-credential' in row[0] for row in db.execute('SELECT request FROM commands'))
    titled = rpc('session.title', session_id=sid, title='Hosted parent')
    assert titled.get('result', {}).get('title') == 'Hosted parent', titled
    moved = rpc('session.cwd.set', session_id=sid, cwd=str(tmp_path))
    assert moved.get('result', {}).get('cwd') == str(tmp_path), moved
    status = rpc('slash.exec', session_id=sid, command='/status')
    assert status.get('result', {}).get('output'), status
    submitted = rpc('prompt.submit', session_id=sid, text='parent prompt')
    assert submitted.get('result', {}).get('status') == 'streaming', submitted
    await _until(lambda: any(e['frame'].get('params', {}).get('type') == 'message.complete' for e in hub.events(key, 0)))
    shared = rpc('session.clients.share', session_id=sid)['result']
    hub.attach(shared['token'], 'discord', 'parent-thread', 'owner')
    for method in ('session.branch', 'session.branch_whole', 'session.branch_stored'):
        branch = (rpc(method, parent_session_id=key, source='desktop') if method == 'session.branch_stored'
                  else rpc(method, session_id=sid, name=method))
        assert 'result' in branch, branch
        child = branch['result']
        child_sid, child_key = child['session_id'], child['stored_session_id']
        assert child_key != key
        assert hub.lookup(child_key)['sid'] == child_sid
        head = hub.event_head(key)
        answer = rpc('prompt.submit', session_id=child_sid, text='private branch prompt')
        assert answer.get('result', {}).get('status') == 'streaming', answer
        await _until(lambda: any(e['frame'].get('params', {}).get('type') == 'message.complete' for e in hub.events(child_key, 0)))
        assert not any(e['frame'].get('params', {}).get('type') in {'message.complete', 'session.clients.input'}
                       for e in hub.events(key, head))
        assert hub.link('discord', 'parent-thread')['session_key'] == key
    assert len(provider.main_requests()) == 4


@pytest.mark.asyncio
async def test_create_retry_and_warm_resume_publish_each_final_once(hosted):
    _home, hub, _peer, provider, rpc = hosted
    first = rpc('session.create', source='desktop', idempotency_key='lost-create')['result']
    for _ in range(3):
        repeated = rpc('session.create', source='desktop', idempotency_key='lost-create')
        assert repeated['result']['session_id'] == first['session_id']
    resumed = rpc('session.resume', session_id=first['stored_session_id'])
    assert resumed['result']['session_id'] == first['session_id'], resumed
    sent = rpc('prompt.submit', session_id=first['session_id'], text='one final only')
    assert sent.get('result', {}).get('status') == 'streaming', sent
    key = first['stored_session_id']
    await _until(lambda: any(e['frame'].get('params', {}).get('type') == 'message.complete' for e in hub.events(key, 0)))
    finals = [e for e in hub.events(key, 0) if e['frame'].get('params', {}).get('type') == 'message.complete']
    assert len(finals) == 1, finals
    assert len(provider.main_requests()) == 1


@pytest.fixture
def local_host(tmp_path, monkeypatch):
    from tui_gateway import server
    from tui_gateway.session_clients import get_hub
    from hermes_cli import _early_recovery
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    # The real checkout's .git metadata lives under the operator's home. Give
    # startup recovery an isolated install root; leave the home-I/O guard on.
    monkeypatch.setattr(_early_recovery, '_project_root', lambda: tmp_path)
    with FakeLLMServer(lambda record: Text('local hosted answer')) as provider:
        home = write_hermes_home(tmp_path / 'profile', provider.base_url, extra_config=(
            'session_clients:\n  enabled: true\n'
            'memory:\n  memory_enabled: false\n  user_profile_enabled: false\n'))
        monkeypatch.setenv('HERMES_HOME', str(home))
        monkeypatch.setattr(server, '_hermes_home', home)
        hub = get_hub(home, execution_host=True, start=False)
        try:
            yield home, hub, provider
        finally:
            server._shutdown_sessions()
            hub.close()


@pytest.mark.asyncio
async def test_concurrent_cold_resumes_admit_one_runtime(local_host):
    from hermes_state import SessionDB
    from tui_gateway import server
    from tui_gateway.session_client_host import HOST_KEY
    home, hub, provider = local_host
    with SessionDB(home / 'state.db') as db:
        db.create_session('cold-stored', source='desktop')
        db.append_message('cold-stored', 'user', 'stored prompt')
        db.append_message('cold-stored', 'assistant', 'stored answer')
    first = hub.call(HOST_KEY, 'session.resume', {'session_id': 'cold-stored', 'eager_build': True})
    assert 'result' in first, first
    results = await asyncio.gather(*[
        asyncio.to_thread(hub.call, HOST_KEY, 'session.resume', {'session_id': 'cold-stored', 'eager_build': True}) for _ in range(4)])
    assert all('result' in result for result in results), results
    assert len({r['result']['session_id'] for r in results}) == 1, results
    listed = server.handle_request({'id': 'list', 'method': 'session.active_list', 'params': {}})
    assert len(listed['result']['sessions']) == 1, listed
    assert provider.main_requests() == []


@pytest.mark.asyncio
async def test_native_reaper_retires_unused_hosted_runtimes_but_keeps_viewers_and_allows_explicit_resume(local_host, monkeypatch):
    from hermes_state import SessionDB
    from tui_gateway import server
    from tui_gateway.session_client_host import HOST_KEY
    home, hub, provider = local_host
    monkeypatch.setattr(server, '_SESSION_TTL_S', 0)
    kept = hub.call(HOST_KEY, 'session.create', {})['result']
    abandoned = hub.call(HOST_KEY, 'session.create', {})['result']
    stored = hub.call(HOST_KEY, 'session.create', {'messages': [
        {'role': 'user', 'content': 'seed'}, {'role': 'assistant', 'content': 'answer'}]})['result']
    viewer = DesktopPeer()
    hub.subscribe(kept['stored_session_id'], viewer)
    await asyncio.sleep(2)
    await asyncio.to_thread(server._reap_idle_sessions)
    assert hub.lookup(abandoned['stored_session_id'])['online'] == 0
    assert hub.lookup(stored['stored_session_id'])['online'] == 0
    assert hub.lookup(kept['stored_session_id'])['online'] == 1
    with SessionDB(home / 'state.db') as db:
        assert db.get_session(abandoned['stored_session_id']) is None
        assert db.get_session(stored['stored_session_id'])['ended_at'] is None
    listed = server.handle_request({'id': 'list', 'method': 'session.active_list', 'params': {}})
    assert [s['id'] for s in listed['result']['sessions']] == [kept['session_id']], listed
    with pytest.raises(RuntimeError, match='unavailable'):
        hub.call(stored['stored_session_id'], 'prompt.submit', {'text': 'must not fail over'})
    resumed = hub.call(HOST_KEY, 'session.resume', {'session_id': stored['stored_session_id']})
    assert 'result' in resumed, resumed
    assert resumed['result']['session_id'] != stored['session_id']
    hub.unsubscribe(kept['stored_session_id'], viewer)
    await asyncio.sleep(2)
    await asyncio.to_thread(server._reap_idle_sessions)
    assert hub.lookup(kept['stored_session_id'])['online'] == 0
    assert provider.main_requests() == []


@pytest.mark.asyncio
async def test_hosted_lru_releases_idle_drafts_without_ending_active_turns_or_attached_viewers(local_host, monkeypatch):
    from tui_gateway import server
    from tui_gateway.session_client_host import HOST_KEY
    home, hub, provider = local_host
    requested, release = threading.Event(), threading.Event()
    def reply(record):
        requested.set()
        assert release.wait(20)
        return Text('survived hosted retirement')
    monkeypatch.setattr(provider, '_responder', reply)  # Fake provider network boundary.
    monkeypatch.setattr(server, '_SESSION_TTL_S', 0)
    active = hub.call(HOST_KEY, 'session.create', {})['result']
    attached = hub.call(HOST_KEY, 'session.create', {})['result']
    abandoned = hub.call(HOST_KEY, 'session.create', {})['result']
    viewer = DesktopPeer()
    hub.subscribe(attached['stored_session_id'], viewer)
    config_path = home / 'config.yaml'
    config_path.write_text(config_path.read_text() + '\nmax_live_sessions: 1\n')
    try:
        submitted = hub.call(active['stored_session_id'], 'prompt.submit', {'text': 'keep running without a viewer'})
        assert submitted.get('result', {}).get('status') == 'streaming', submitted
        if not await asyncio.to_thread(requested.wait, 10):
            raise RuntimeError('\n'.join(str(e['frame']['params'].get('payload')) for e in hub.events(active['stored_session_id'], 0) if e['frame']['params'].get('type') == 'error'))
        await asyncio.sleep(2)
        await asyncio.to_thread(server._enforce_session_cap)
        assert hub.lookup(abandoned['stored_session_id'])['online'] == 0
        assert hub.lookup(active['stored_session_id'])['online'] == 1
        assert hub.lookup(attached['stored_session_id'])['online'] == 1
        release.set()
        key = active['stored_session_id']
        await _until(lambda: any(e['frame'].get('params', {}).get('payload', {}).get('text') == 'survived hosted retirement'
                                 for e in hub.events(key, 0)))
        await _until(lambda: not hub.call(key, 'session.activate', {})['result']['running'])
        await asyncio.to_thread(server._reap_idle_sessions)
        assert hub.lookup(active['stored_session_id'])['online'] == 0
        assert hub.lookup(attached['stored_session_id'])['online'] == 1
        assert len(provider.main_requests()) == 1
    finally:
        release.set()

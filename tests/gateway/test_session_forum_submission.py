"""Revocation at real native Discord HTTP submission and empty branch admission."""
import asyncio
import sqlite3

import pytest

from gateway.session_clients import _cursor, poll_session_clients
from tests.gateway.test_session_forum_sync import _configure, _exchange

pytest_plugins = ['tests.gateway.test_session_forum_sync']
from tests.tui_gateway.test_session_client_host import _request
from tests.tui_gateway.test_session_clients_live import DesktopPeer, _until


@pytest.mark.asyncio
@pytest.mark.parametrize('boundary', ['lookup', 'chunk', 'attachment', 'retry'])
async def test_revoked_native_submission_stays_private(native_case, monkeypatch, boundary):
    import discord
    from aiohttp import web
    home, hub, provider, runner, api, _adapter = native_case
    _, key = await _exchange()
    await poll_session_clients(runner, home, 'default', {})
    tid = api.threads[0]['id']
    before = _cursor(home, key, tid)
    sent = len(api.posts)
    payload = {'text': 'PRIVATE_LOOKUP'}
    if boundary == 'chunk':
        payload['text'] = 'A' * 2000 + 'PRIVATE_NEXT_CHUNK'
        api.message_hook = lambda text: _configure(home, automatic=False)
    elif boundary == 'lookup':
        from aiohttp import ClientResponse
        original = ClientResponse.start
        async def lookup(response, connection):
            result = await original(response, connection)
            if response.method == 'GET' and response.url.path == f'/channels/{tid}':
                _configure(home, automatic=False)
            return result
        monkeypatch.setattr(ClientResponse, 'start', lookup)
    elif boundary == 'attachment':
        monkeypatch.setattr('gateway.platforms.base._HERMES_ROOT', home)
        monkeypatch.setattr('gateway.platforms.base.MEDIA_DELIVERY_SAFE_ROOTS', (home / 'cache',))
        file = home / 'cache' / 'private.txt'
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text('PRIVATE_UPLOAD')
        payload = {'attachments': [{'path': str(file)}]}
        original = discord.File
        def prepare(*args, **kwargs):
            result = original(*args, **kwargs)
            _configure(home, automatic=False)
            return result
        monkeypatch.setattr(discord, 'File', prepare)
    else:
        # A genuine SDK 429 sleep/retry, not a mocked adapter/helper retry.
        import json
        original = api.payload
        attempts = []
        async def limited(request):
            result = await original(request)
            if request.path == f'/channels/{tid}/messages':
                attempts.append(result)
                if len(attempts) == 1:
                    _configure(home, automatic=False)
                    raise web.HTTPTooManyRequests(body=json.dumps({'retry_after': 0.01, 'global': False}).encode(),
                        content_type='application/json', headers={'Via': 'loopback'})
            return result
        monkeypatch.setattr(api, 'payload', limited)
    seq = hub.publish(key, {'method': 'event', 'params': {'type': 'message.complete', 'payload': payload}})
    await poll_session_clients(runner, home, 'default', {})
    texts = [text for _, text in api.posts][sent:]
    if boundary == 'chunk':
        assert len(texts) == 1 and 'PRIVATE_NEXT_CHUNK' not in texts[0]
    else:
        assert texts == []
    if boundary == 'retry':
        assert len(attempts) == 1
    assert api.uploads == []
    assert before <= _cursor(home, key, tid) < seq
    assert len(provider.main_requests()) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('stale_positive', [False, True])
async def test_empty_branch_is_private_before_after_prompt_and_retry(native_case, stale_positive, monkeypatch):
    from gateway.session_forum_sync import automatic_config
    from hermes_state import SessionDB
    from tui_gateway import server
    home, hub, _provider, runner, api, _adapter = native_case
    db = SessionDB(home / 'state.db')
    db.create_session('private-parent', source='desktop')
    monkeypatch.setattr(server, '_db', db)
    monkeypatch.setattr(server, '_db_error', None)
    peer = DesktopPeer()
    params = {'source': 'desktop', 'parent_session_id': 'private-parent', 'messages': [],
              'idempotency_key': 'empty-private-branch'}
    created = _request(peer, {'id': 'branch', 'method': 'session.create', 'params': params})['result']
    key = created['stored_session_id']
    assert db.get_session(key)['parent_session_id'] == 'private-parent'
    with sqlite3.connect(home / 'session_clients_gateway.db') as db:
        scope = db.execute('SELECT scope FROM forum_enrollments WHERE session_key=?', (key,)).fetchone()[0]
    if not stale_positive:
        assert scope == ''
    else:
        with sqlite3.connect(home / 'session_clients_gateway.db') as db:
            db.execute('UPDATE forum_enrollments SET scope=? WHERE session_key=?', (automatic_config(home)['scope'], key))
    await poll_session_clients(runner, home, 'default', {})
    assert api.posts == []
    _request(peer, {'id': 'prompt', 'method': 'prompt.submit',
                    'params': {'session_id': created['session_id'], 'text': 'PRIVATE_BRANCH'}})
    await _until(lambda: any(f.get('params', {}).get('type') == 'message.complete' for f in peer.frames))
    await poll_session_clients(runner, home, 'default', {})
    assert api.posts == [] and api.creates == [] and hub.links('discord') == []
    server._shutdown_sessions()
    server._idempotency_keys.clear()
    await _exchange('PRIVATE_COLD_RETRY', idempotency_key='empty-private-branch')
    await poll_session_clients(runner, home, 'default', {})
    assert api.posts == []
    await _exchange('ordinary new conversation')
    await poll_session_clients(runner, home, 'default', {})
    assert len(api.threads) == 1
    assert [text for _, text in api.posts][1:] == ['ordinary new conversation', 'one native answer']
    server._shutdown_sessions()
    server._db.close()


async def _cold_empty_branch(home, first):
    from hermes_state import SessionDB
    from tui_gateway import server
    from tui_gateway.session_clients import get_hub
    from tests.gateway.test_session_forum_sync import _cross_process_poll
    hub = get_hub(home, execution_host=True)
    try:
        if first:
            with SessionDB(home / 'state.db') as db:
                db.create_session('private-parent', source='desktop')
        if first:
            await _exchange('PRIVATE_COLD_BRANCH', idempotency_key='cold-empty-branch',
                            parent_session_id='private-parent', messages=[])
        else:
            from tests.gateway.test_session_forum_sync import DesktopPeer, _request
            retried = _request(DesktopPeer(), {'id': 'retry', 'method': 'session.create',
                'params': {'source': 'desktop', 'idempotency_key': 'cold-empty-branch'}})
            assert retried.get('error', {}).get('code') == 4092, retried
            with hub._db() as db:
                assert db.execute('SELECT COUNT(*) FROM commands').fetchone()[0] == 0
            import sqlite3
            with sqlite3.connect(home / 'session_clients_gateway.db') as db:
                decisions = list(db.execute('SELECT scope FROM forum_create_decisions'))
            assert decisions == [('',)]
        await _cross_process_poll(home)
    finally:
        server._shutdown_sessions()
        hub.close()


@pytest.mark.asyncio
async def test_empty_branch_negative_survives_actual_host_process_restart(native_case, tmp_path):
    import os
    import sys
    import discord
    from tests.fakes.fake_llm_provider import write_hermes_home
    from tests.gateway.test_session_forum_sync import config
    _home, _hub, provider, _runner_instance, api, _adapter = native_case
    home = write_hermes_home(tmp_path / 'cold-branch', provider.base_url, extra_config=config())
    env = {**os.environ, 'HERMES_HOME': str(home), 'HERMES_RUNTIME_DIR': str(home / 'runtime')}
    for first in (True, False):
        script = ('import hermes_bootstrap; import run_agent; import asyncio; import discord; from pathlib import Path; '
                  'from tests.gateway.test_session_forum_submission import _cold_empty_branch; '
                  'discord.http.Route.BASE=' + repr(discord.http.Route.BASE) + '; '
                  'asyncio.run(_cold_empty_branch(Path(' + repr(str(home)) + '),' + repr(first) + '))')
        child = await asyncio.create_subprocess_exec(sys.executable, '-c', script, env=env,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        output, error = await asyncio.wait_for(child.communicate(), 30)
        assert child.returncode == 0, (output.decode(), error.decode())
        assert api.posts == [] and api.creates == [] and api.threads == []
    assert len(provider.main_requests()) == 1

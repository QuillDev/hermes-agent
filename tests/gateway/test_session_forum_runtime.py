"""Automatic-only runtime exclusion, using aiohttp's real non-eager writer."""
import asyncio
import sqlite3
from types import SimpleNamespace

import pytest

from gateway import session_forum_sync as policy
from gateway.session_clients import _cursor, handle_session_client_message, poll_session_clients
from tests.gateway.test_session_forum_sync import _exchange

pytest_plugins = ['tests.gateway.test_session_forum_sync']


def _non_eager(monkeypatch):
    # Select the installed aiohttp's actual Python 3.11 scheduling branch, not
    # a replacement send/writer or a mocked runtime gate. Leave stdlib sys alone.
    import aiohttp.client_reqrep
    runtime = SimpleNamespace(version_info=(3, 11))
    monkeypatch.setattr(aiohttp.client_reqrep, 'sys', runtime)
    monkeypatch.setattr(policy, 'sys', runtime, raising=False)


@pytest.mark.asyncio
async def test_unsupported_native_create_stays_private_even_after_runtime_upgrade(native_case, monkeypatch):
    home, hub, provider, runner, api, _adapter = native_case
    with monkeypatch.context() as runtime:
        _non_eager(runtime)
        assert policy.automatic_config(home) is None
        _, key = await _exchange('PRIVATE_UNSUPPORTED', idempotency_key='unsupported-create')
        await poll_session_clients(runner, home, 'default', {})
    with sqlite3.connect(home / 'session_clients_gateway.db') as db:
        assert db.execute('SELECT scope FROM forum_enrollments WHERE session_key=?', (key,)).fetchone()[0] == ''
    assert policy.automatic_config(home) is not None
    await _exchange('PRIVATE_RETRY', idempotency_key='unsupported-create')
    await poll_session_clients(runner, home, 'default', {})
    assert api.creates == [] and api.posts == [] and api.threads == []
    assert hub.links('discord') == []
    assert len(provider.main_requests()) == 2
    await _exchange('supported new conversation')
    await poll_session_clients(runner, home, 'default', {})
    assert len(api.threads) == 1


@pytest.mark.asyncio
async def test_unsupported_existing_automatic_link_rejects_controls_and_delivery(native_case, monkeypatch):
    from tui_gateway.session_clients import SessionClientHub
    from gateway.platforms.event import MessageEvent
    from tests.gateway.test_session_forum_sync import GUILD, OWNER
    home, hub, provider, runner, api, _adapter = native_case
    _, key = await _exchange()
    await poll_session_clients(runner, home, 'default', {})
    tid = api.threads[0]['id']
    record = policy.automatic_destination(home, tid)
    cursor = _cursor(home, key, tid)
    posts = list(api.posts)
    remote = SessionClientHub(home, start=False)
    _non_eager(monkeypatch)
    try:
        for method, params in [('prompt.submit', {'text': 'PRIVATE_CONTROL'}), ('session.interrupt', {}),
                               ('session.steer', {'text': 'PRIVATE_CONTROL'}),
                               ('file.attach', {'path': '/not-admitted'}), ('image.attach', {'path': '/not-admitted'})]:
            with pytest.raises(RuntimeError, match='Python 3.12'):
                await asyncio.to_thread(remote.call, key, method, params,
                    automatic_forum={'scope': record['scope'], 'chat_id': tid})
        source = _adapter.build_source(chat_id=tid, user_id=OWNER, guild_id=GUILD,
                                       parent_chat_id=api.channels[0]['id'], chat_type='thread', thread_id=tid)
        result = await handle_session_client_message(runner, MessageEvent(text='PRIVATE_INTAKE', source=source))
        assert result[0] is True and 'unavailable' in result[1]
        hub.publish(key, {'method': 'event', 'params': {'type': 'message.complete', 'payload': {'text': 'PRIVATE_WRITE'}}})
        await poll_session_clients(runner, home, 'default', {})
        with hub._db() as db:
            assert db.execute('SELECT COUNT(*) FROM commands').fetchone()[0] == 0
        assert api.posts == posts
        assert _cursor(home, key, tid) == cursor
        assert len(provider.main_requests()) == 1
    finally:
        remote.close()

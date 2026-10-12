"""Incumbent bridge boundaries through native RPC, routing, admission and SQLite."""
import asyncio
from dataclasses import replace
from pathlib import Path

import pytest

from tests.gateway.test_session_client_legacy_ownership import _runner, _close_runner
from tests.tui_gateway import test_session_client_regressions as fixtures
from tests.tui_gateway.test_session_client_host import _request
from tests.tui_gateway.test_session_clients_live import DesktopPeer, _until

local_host = fixtures.local_host


async def _rpc(peer, method, **params):
    return await asyncio.to_thread(_request, peer, {'id': method, 'method': method, 'params': params})


def _setup(local_host, monkeypatch):
    import hermes_state
    from tui_gateway import server
    home, hub, provider = local_host
    monkeypatch.setattr(hermes_state, 'DEFAULT_DB_PATH', home / 'state.db')
    monkeypatch.setattr(server, '_db', None)
    runner, adapter, source = _runner(home, monkeypatch)
    runner._wire_adapter_handlers(adapter)
    return home, hub, provider, runner, adapter, source


@pytest.mark.asyncio
@pytest.mark.parametrize('per_user', [False, True])
async def test_existing_authorization_and_exact_user_lanes_are_not_first_member_links(local_host, monkeypatch, per_user):
    from gateway.platforms.event import MessageEvent
    home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    runner.config.thread_sessions_per_user = per_user
    try:
        assert await runner._handle_message(MessageEvent(text='original authorized member', source=source)) == 'local hosted answer'
        first = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        peer = DesktopPeer()
        resumed = await _rpc(peer, 'session.resume', session_id=first.session_id)
        assert 'result' in resumed, resumed
        before = list(provider.main_requests())
        assert await runner._handle_message(MessageEvent(text='unauthorized must not enter', source=replace(source, user_id='intruder'))) is None
        assert provider.main_requests() == before
        second_source = replace(source, user_id='second-member')
        assert await runner._handle_message(MessageEvent(text='second authorized member', source=second_source)) == 'local hosted answer'
        second = runner.session_store.lookup_by_session_key(runner._session_key_for_source(second_source))
        assert (first.session_id != second.session_id) is per_user
        assert len(provider.main_requests()) == 2
        assert hub.links('discord') == []
        assert all(m[0] == 'existing-thread' for m in adapter.messages)
        assert all('unauthorized must not enter' not in str(r['messages']) for r in provider.main_requests())
        assert home == hub.home
    finally:
        _close_runner(runner)


@pytest.mark.asyncio
async def test_disabled_bridge_refuses_desktop_without_replacing_original_discord_lane(local_host, monkeypatch):
    from gateway.platforms.event import MessageEvent
    from tui_gateway import server
    home, _hub, provider, runner, _adapter, source = _setup(local_host, monkeypatch)
    try:
        assert await runner._handle_message(MessageEvent(text='native owner', source=source)) == 'local hosted answer'
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        peer = DesktopPeer()
        resumed = await _rpc(peer, 'session.resume', session_id=entry.session_id)
        config = home / 'config.yaml'
        config.write_text(config.read_text().replace('enabled: true', 'enabled: false'))
        blocked = await _rpc(peer, 'prompt.submit', session_id=resumed['result']['session_id'], text='must not execute')
        assert blocked.get('error', {}).get('code') == 4093, blocked
        assert await runner._handle_message(MessageEvent(text='native lane continues', source=source)) == 'local hosted answer'
        assert len(provider.main_requests()) == 2
        assert not server._sessions
    finally:
        _close_runner(runner)


@pytest.mark.asyncio
async def test_private_desktop_and_other_profile_do_not_publish_to_legacy_destination(local_host, monkeypatch):
    from gateway.platforms.event import MessageEvent
    from tests.fakes.fake_llm_provider import write_hermes_home
    from gateway.session_client_bridge import start_gateway_bridge
    _home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    try:
        await start_gateway_bridge(runner)
        assert await runner._handle_message(MessageEvent(text='native conversation', source=source)) == 'local hosted answer'
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        peer = DesktopPeer()
        resumed = await _rpc(peer, 'session.resume', session_id=entry.session_id)
        other = Path.home() / '.hermes' / 'profiles' / 'other'
        write_hermes_home(other, provider.base_url)
        foreign = await _rpc(peer, 'session.activate', session_id=resumed['result']['session_id'], profile='other')
        assert 'error' in foreign, foreign
        before = list(adapter.messages)
        created = await _rpc(peer, 'session.create', source='desktop')
        assert created['result']['stored_session_id'] != entry.session_id
        submitted = await _rpc(peer, 'prompt.submit', session_id=created['result']['session_id'], text='private Desktop draft')
        assert submitted['result']['status'] == 'streaming', submitted
        await _until(lambda: any(f.get('params', {}).get('type') == 'message.complete' and
                                f['params'].get('session_id') == created['result']['session_id'] for f in peer.frames))
        assert adapter.messages == before
        assert hub.links('discord') == []
        assert not any('private Desktop draft' in str(e) for e in hub.events(entry.session_id, 0))
        assert len(provider.main_requests()) == 2
    finally:
        _close_runner(runner)


@pytest.mark.asyncio
async def test_internal_completion_uses_same_owner_and_native_destination(local_host, monkeypatch):
    from gateway.platforms.event import MessageEvent
    from hermes_state import SessionDB
    home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    try:
        assert await runner._handle_message(MessageEvent(text='native parent', source=source)) == 'local hosted answer'
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        peer = DesktopPeer()
        resumed = await _rpc(peer, 'session.resume', session_id=entry.session_id)
        pinned = entry.prompt_pin
        accepted = await runner._inject_watch_notification('late background completion',
                                                          {'session_key': entry.session_key,
                                                           'parent_session_id': entry.session_id})
        assert accepted is True
        await _until(lambda: not adapter._active_sessions)
        assert adapter.messages[-1][0] == 'existing-thread'
        assert entry.prompt_pin == pinned
        assert len(provider.main_requests()) == 2
        active = await _rpc(peer, 'session.activate', session_id=resumed['result']['session_id'])
        assert active['result']['stored_session_id'] == entry.session_id
        with SessionDB(home / 'state.db') as db:
            history = db.get_messages_as_conversation(entry.session_id)
        assert sum('late background completion' in str(m.get('content')) for m in history) == 1
        assert len([e for e in hub.events(entry.session_id, 0) if e['frame']['params']['type'] == 'message.complete']) == 2
    finally:
        _close_runner(runner)


@pytest.mark.asyncio
async def test_compression_lineage_retains_owner_but_branch_does_not_inherit_its_controls(local_host, monkeypatch):
    from gateway.platforms.event import MessageEvent
    from gateway.session_client_bridge import start_gateway_bridge
    _home, hub, _provider, runner, _adapter, source = _setup(local_host, monkeypatch)
    try:
        assert await runner._handle_message(MessageEvent(text='ancestor question', source=source)) == 'local hosted answer'
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        root = entry.session_id
        peer = DesktopPeer()
        first = await _rpc(peer, 'session.resume', session_id=root)
        sid = first['result']['session_id']
        db = runner.session_store._db_for_key(entry.session_key)
        db.end_session(root, end_reason='compression')
        db.create_session('compressed-native-tip', source='discord', parent_session_id=root)
        db.append_message('compressed-native-tip', 'user', 'tip question')
        db.append_message('compressed-native-tip', 'assistant', 'tip answer')
        runner.session_store.switch_session(entry.session_key, 'compressed-native-tip')
        await start_gateway_bridge(runner)
        compressed = await _rpc(peer, 'session.resume', session_id=root)
        assert compressed['result']['session_id'] == sid
        assert compressed['result']['stored_session_id'] == 'compressed-native-tip'
        assert any(m.get('text') == 'ancestor question' for m in compressed['result']['messages'])
        assert hub.lookup('compressed-native-tip')['sid'] == sid
        db.create_session('native-branch', source='discord', parent_session_id='compressed-native-tip',
                          model_config={'_branched_from': 'compressed-native-tip'})
        runner.session_store.switch_session(entry.session_key, 'native-branch')
        await start_gateway_bridge(runner)
        old_control = await _rpc(peer, 'session.activate', session_id=sid)
        assert 'error' in old_control, 'Old view acquired unrelated branch authority'
        branch = await _rpc(peer, 'session.resume', session_id='native-branch')
        assert branch['result']['session_id'] != sid
        assert branch['result']['stored_session_id'] == 'native-branch'
    finally:
        _close_runner(runner)


@pytest.mark.asyncio
async def test_retired_owner_refuses_late_notification_before_adapter_acceptance(local_host, monkeypatch):
    from gateway.platforms.event import MessageEvent
    from gateway.session_client_bridge import retire_gateway_bridge
    from tui_gateway import server
    _home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    try:
        assert await runner._handle_message(MessageEvent(text='completed old owner', source=source)) == 'local hosted answer'
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        before = list(adapter.messages)
        runner._executor_closing = True
        retire_gateway_bridge(runner)
        accepted = await runner._inject_watch_notification('late completion must remain retryable',
                                                          {'session_key': entry.session_key,
                                                           'parent_session_id': entry.session_id})
        assert accepted is False, 'Retired owner acknowledged and lost a native notification'
        peer = DesktopPeer()
        resumed = await _rpc(peer, 'session.resume', session_id=entry.session_id)
        assert 'error' in resumed, resumed
        assert not server._sessions
        assert len(provider.main_requests()) == 1
        assert adapter.messages == before
        assert not hub.lookup(entry.session_id)['online']
    finally:
        _close_runner(runner)


@pytest.mark.asyncio
async def test_unknown_legacy_incumbent_is_not_a_native_cold_resume_probe(local_host, monkeypatch):
    from gateway.platforms.event import MessageEvent
    from tui_gateway import server
    home, _hub, provider, runner, _adapter, source = _setup(local_host, monkeypatch)
    config = home / 'config.yaml'
    config.write_text(config.read_text().replace('enabled: true', 'enabled: false'))
    try:
        assert await runner._handle_message(MessageEvent(text='pre opt-in native owner', source=source)) == 'local hosted answer'
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        config.write_text(config.read_text().replace('enabled: false', 'enabled: true'))
        peer = DesktopPeer()
        resumed = await _rpc(peer, 'session.resume', session_id=entry.session_id, eager_build=True)
        assert 'error' in resumed, 'Unknown native incumbent was probed by building a second agent'
        assert not server._sessions
        assert len(provider.main_requests()) == 1
    finally:
        _close_runner(runner)


@pytest.mark.asyncio
async def test_hidden_rewind_voice_and_client_queue_replay_fail_before_durable_mailbox(local_host, monkeypatch):
    import sqlite3
    from gateway.platforms.event import MessageEvent
    from tui_gateway.session_clients import SessionClientHub
    home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    client = SessionClientHub(home)
    marker = 'must-not-enter-the-broker-or-public-destination'
    try:
        assert await runner._handle_message(MessageEvent(text='plain native seed', source=source)) == 'local hosted answer'
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        for extra in ({'queued': True}, {'display_kind': 'hidden'}, {'truncate_before_row_id': 1},
                      {'voice_turn': True}, {'text': [{'type': 'text', 'text': marker}]}):
            with pytest.raises(ValueError):
                await asyncio.to_thread(client.call, entry.session_id, 'prompt.submit', {'text': marker, **extra})
        with sqlite3.connect(hub.path) as db:
            assert not any(marker in row[0] for row in db.execute('SELECT request FROM commands'))
        assert len(provider.main_requests()) == 1
        assert not any(marker in m[1] for m in adapter.messages)
    finally:
        await _until(lambda: not adapter._active_sessions)
        client.close()
        _close_runner(runner)


@pytest.mark.asyncio
async def test_attached_desktop_retains_actual_incumbent_and_its_prompt_prefix_through_idle_sweep(local_host, monkeypatch):
    from gateway.platforms.event import MessageEvent
    _home, _hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    try:
        assert await runner._handle_message(MessageEvent(text='warm incumbent', source=source)) == 'local hosted answer'
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        peer = DesktopPeer()
        resumed = await _rpc(peer, 'session.resume', session_id=entry.session_id)
        incumbent = runner._cached_agent_for(entry.session_key)
        # The input fixture is an idle-aged actual cached agent, not a fake clock,
        # repository or replacement actor. Ownership must retain this very agent.
        incumbent._last_activity_ts = 0
        assert runner._sweep_idle_cached_agents() == 0, 'Desktop attachment lost the actual native cached incumbent'
        assert runner._cached_agent_for(entry.session_key) is incumbent
        submitted = await _rpc(peer, 'prompt.submit', session_id=resumed['result']['session_id'], text='same pinned prefix')
        assert submitted['result']['status'] == 'streaming'
        await _until(lambda: len(provider.main_requests()) == 2 and not adapter._active_sessions)
        assert provider.main_requests()[0]['messages'][0] == provider.main_requests()[1]['messages'][0]
        closed = await _rpc(peer, 'session.close', session_id=resumed['result']['session_id'])
        assert closed['result']['closed'] is True
        incumbent._last_activity_ts = 0
        assert runner._sweep_idle_cached_agents() == 1, 'Detached, unviewed incumbents must remain normally evictable'
    finally:
        _close_runner(runner)

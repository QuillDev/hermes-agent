"""Atomic native notification admission uses the real runner, broker and adapter."""
import asyncio
import threading

import pytest

from gateway.platforms.event import MessageEvent
from gateway.wake import WakeNotAccepted, deliver_wake
from tests.gateway.test_session_client_legacy_bridge_boundaries import _setup
from tests.gateway.test_session_client_legacy_ownership import _close_runner
from tests.tui_gateway import test_session_client_regressions as fixtures
from tests.tui_gateway.test_session_clients_live import _until

local_host = fixtures.local_host


@pytest.mark.asyncio
async def test_deliver_wake_refuses_retired_owner_before_native_receipt(local_host, monkeypatch):
    _home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    adapter.gateway_runner = runner  # connect-time native back-reference
    try:
        await runner._handle_message(MessageEvent(text='original native owner', source=source))
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        owner = hub.lookup(entry.session_id)
        hub.retire(owner['session_key'], owner['sid'])
        before = list(adapter.messages)
        with pytest.raises(WakeNotAccepted):
            await deliver_wake(adapter, text='must remain retryable', source=source)
        assert not adapter._active_sessions
        assert not adapter._pending_messages
        assert len(provider.main_requests()) == 1
        assert adapter.messages == before
    finally:
        await _until(lambda: not adapter._active_sessions)
        _close_runner(runner)


def _retire(runner, hub, entry):
    from gateway.session_client_bridge import retire_gateway_bridge
    return retire_gateway_bridge(runner)


@pytest.mark.asyncio
async def test_retirement_between_lookup_and_reservation_refuses_receipt(local_host, monkeypatch):
    from gateway import session_client_admission as admission
    _home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    adapter.gateway_runner = runner
    looked_up, proceed = threading.Event(), threading.Event()
    wake = None
    try:
        await runner._handle_message(MessageEvent(text='native seed', source=source))
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        original = admission._owner_target

        def barrier(*args):
            target = original(*args)
            looked_up.set()
            assert proceed.wait(15)
            return target

        monkeypatch.setattr(admission, '_owner_target', barrier)
        wake = asyncio.create_task(deliver_wake(adapter, text='lookup raced retirement', source=source))
        assert await asyncio.to_thread(looked_up.wait, 10)
        assert _retire(runner, hub, entry) is True
        assert not hub.lookup(entry.session_id)['online']
        proceed.set()
        with pytest.raises(WakeNotAccepted):
            await wake
        assert not adapter._active_sessions
        assert not adapter._pending_messages
        assert len(provider.main_requests()) == 1
    finally:
        proceed.set()
        if wake is not None:
            await asyncio.gather(wake, return_exceptions=True)
        _close_runner(runner)


@pytest.mark.asyncio
@pytest.mark.parametrize('category', ['result', 'diagnostic'])
async def test_normal_native_wake_keeps_original_owner_and_notification_policy(local_host, monkeypatch, category):
    from hermes_state import SessionDB
    home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    adapter.gateway_runner = runner
    try:
        await runner._handle_message(MessageEvent(text='native seed', source=source))
        key = runner._session_key_for_source(source)
        entry = runner.session_store.lookup_by_session_key(key)
        owner = hub.lookup(entry.session_id)
        agent = runner._cached_agent_for(key)
        pin = entry.prompt_pin
        await deliver_wake(adapter, text='native wake payload', source=source, notification_category=category)
        await _until(lambda: not adapter._active_sessions)
        assert runner._cached_agent_for(key) is agent
        assert entry.prompt_pin == pin
        assert hub.lookup(entry.session_id) == owner
        assert len(provider.main_requests()) == 2
        with SessionDB(home / 'state.db') as db:
            history = db.get_messages_as_conversation(entry.session_id)
        assert sum('native wake payload' in str(row.get('content')) for row in history) == 1
        assert all(message[0] == 'existing-thread' for message in adapter.messages)
        assert runner._legacy_notification_lifetime.leases == 0
    finally:
        await _until(lambda: not adapter._active_sessions)
        _close_runner(runner)


@pytest.mark.asyncio
async def test_reserved_busy_fifo_survives_retirement_without_replay(local_host, monkeypatch):
    from hermes_state import SessionDB
    from tests.fakes.fake_llm_provider import Text
    home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    adapter.gateway_runner = runner
    active, release_provider = threading.Event(), threading.Event()
    entered_busy, release_busy = asyncio.Event(), asyncio.Event()
    wake = None

    def reply(record):
        if len(provider.main_requests()) == 2:
            active.set()
            assert release_provider.wait(20)
        return Text('native completion')

    monkeypatch.setattr(provider, '_responder', reply)
    try:
        await runner._handle_message(MessageEvent(text='native seed', source=source))
        key = runner._session_key_for_source(source)
        entry = runner.session_store.lookup_by_session_key(key)
        agent = runner._cached_agent_for(key)
        await adapter.handle_message(MessageEvent(text='active human turn', source=source))
        assert await asyncio.to_thread(active.wait, 10)
        await deliver_wake(adapter, text='first queued wake', source=source)
        original_busy = adapter._busy_session_handler

        async def barrier(event, session_key):
            entered_busy.set()
            await release_busy.wait()
            return await original_busy(event, session_key)

        adapter.set_busy_session_handler(barrier)
        wake = asyncio.create_task(deliver_wake(adapter, text='second queued wake', source=source))
        await asyncio.wait_for(entered_busy.wait(), 10)
        assert _retire(runner, hub, entry) is False
        assert hub.lookup(entry.session_id)['online']
        release_busy.set()
        await wake
        with pytest.raises(WakeNotAccepted):
            await deliver_wake(adapter, text='late wake must stay retryable', source=source)
        assert runner._queue_depth(key, adapter=adapter) == 2
        release_provider.set()
        await _until(lambda: not adapter._active_sessions)
        await _until(lambda: not hub.lookup(entry.session_id)['online'])
        assert len(provider.main_requests()) == 4
        assert runner._cached_agent_for(key) is agent
        assert runner._queue_depth(key, adapter=adapter) == 0
        with SessionDB(home / 'state.db') as db:
            history = db.get_messages_as_conversation(entry.session_id)
        wakes = [row['content'] for row in history if row.get('role') == 'user' and 'queued wake' in str(row.get('content'))]
        assert len(wakes) == 2
        assert 'first queued wake' in wakes[0]
        assert 'second queued wake' in wakes[1]
        assert not any('late wake must' in str(row.get('content')) for row in history)
        assert runner._legacy_notification_lifetime.leases == 0
    finally:
        release_busy.set()
        release_provider.set()
        if wake is not None:
            await asyncio.gather(wake, return_exceptions=True)
        await _until(lambda: not adapter._active_sessions)
        _close_runner(runner)


@pytest.mark.asyncio
@pytest.mark.parametrize('busy', [False, True])
@pytest.mark.parametrize('producer', ['kanban', 'process', 'delegation'])
async def test_unresponsive_owner_refuses_every_native_producer_before_fifo(local_host, monkeypatch, busy, producer):
    from tests.fakes.fake_llm_provider import Text
    _home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    adapter.gateway_runner = runner
    active, release = threading.Event(), threading.Event()

    def reply(record):
        if busy and len(provider.main_requests()) == 2:
            active.set()
            assert release.wait(20)
        return Text('native answer')

    monkeypatch.setattr(provider, '_responder', reply)
    try:
        await runner._handle_message(MessageEvent(text='native seed', source=source))
        key = runner._session_key_for_source(source)
        entry = runner.session_store.lookup_by_session_key(key)
        if busy:
            await adapter.handle_message(MessageEvent(text='active native human', source=source))
            assert await asyncio.to_thread(active.wait, 10)
            await deliver_wake(adapter, text='already accepted wake', source=source)
        owner = hub.lookup(entry.session_id)
        hub.retire(owner['session_key'], owner['sid'])
        # A durable live row with no responding incumbent must not be interpreted
        # as a native execution capability (including a root-mailbox generation).
        hub.claim(owner['session_key'], 'unresponsive-root-mailbox')
        before = runner._queue_depth(key, adapter=adapter)
        if producer == 'kanban':
            with pytest.raises(WakeNotAccepted):
                await deliver_wake(adapter, text='new wake must remain retryable', source=source)
        else:
            accepted = await runner._inject_watch_notification('new wake must remain retryable',
                {'session_key': key, 'parent_session_id': entry.session_id,
                 'delegation_id': 'delegation-completion' if producer == 'delegation' else ''})
            assert accepted is False
        assert runner._queue_depth(key, adapter=adapter) == before
        assert len(provider.main_requests()) == (2 if busy else 1)
    finally:
        release.set()
        await _until(lambda: not adapter._active_sessions)
        _close_runner(runner)


@pytest.mark.asyncio
async def test_disabled_unowned_native_route_is_not_globally_gated(local_host, monkeypatch):
    _home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    adapter.gateway_runner = runner
    config = hub.home / 'config.yaml'
    config.write_text(config.read_text().replace('enabled: true', 'enabled: false'))
    try:
        await runner._handle_message(MessageEvent(text='feature off parent', source=source))
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        assert hub.lookup(entry.session_id) is None
        assert _retire_unowned(runner) is True
        await deliver_wake(adapter, text='ordinary native completion', source=source)
        await _until(lambda: not adapter._active_sessions)
        assert len(provider.main_requests()) == 2
    finally:
        await _until(lambda: not adapter._active_sessions)
        _close_runner(runner)


def _retire_unowned(runner):
    from gateway.session_client_admission import retire_internal_admission
    return retire_internal_admission(runner, lambda: None)


@pytest.mark.asyncio
@pytest.mark.parametrize('closing_flag', ['_draining', '_executor_closing'])
async def test_unavailable_native_lifetime_refuses_receipt_while_broker_is_live(local_host, monkeypatch, closing_flag):
    _home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    adapter.gateway_runner = runner
    try:
        await runner._handle_message(MessageEvent(text='native seed', source=source))
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        assert hub.lookup(entry.session_id)['online']
        setattr(runner, closing_flag, True)
        with pytest.raises(WakeNotAccepted):
            await deliver_wake(adapter, text='do not schedule onto closing owner', source=source)
        assert not adapter._active_sessions
        assert not adapter._pending_messages
        assert len(provider.main_requests()) == 1
    finally:
        setattr(runner, closing_flag, False)
        _close_runner(runner)


@pytest.mark.asyncio
async def test_different_direct_binding_cannot_acknowledge_native_wake(local_host, monkeypatch):
    from tests.tui_gateway.test_session_client_host import _request
    from tests.tui_gateway.test_session_clients_live import DesktopPeer
    _home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    adapter.gateway_runner = runner
    try:
        await runner._handle_message(MessageEvent(text='original native parent', source=source))
        peer = DesktopPeer()
        created = await asyncio.to_thread(_request, peer, {
            'id': 'private-owner', 'method': 'session.create', 'params': {'source': 'desktop'}})
        assert 'result' in created, created
        invitation = await asyncio.to_thread(hub.share, created['result']['session_id'])
        await asyncio.to_thread(hub.attach, invitation['token'], 'discord', source.chat_id, source.user_id)
        with pytest.raises(WakeNotAccepted):
            await deliver_wake(adapter, text='must not run on either owner', source=source)
        assert not adapter._active_sessions
        assert not adapter._pending_messages
        assert len(provider.main_requests()) == 1
    finally:
        _close_runner(runner)

"""Native incumbent controls reject unsafe payloads and retain receiving-bot policy."""
import asyncio
import sqlite3
import threading

import pytest

from tests.gateway.test_session_client_legacy_bridge_boundaries import _setup, _rpc
from tests.gateway.test_session_client_legacy_ownership import _close_runner, OfflineDiscord
from tests.tui_gateway import test_session_client_regressions as fixtures
from tests.tui_gateway.test_session_clients_live import DesktopPeer, _until

local_host = fixtures.local_host


@pytest.mark.asyncio
@pytest.mark.parametrize('method,extra', [
    ('image.attach_bytes', {'data': 'private-marker'}),
    ('file.attach', {'path': 'private-marker'}),
    ('pdf.attach', {'path': 'private-marker'}),
    ('slash.exec', {'command': 'private-marker'}),
    ('session.interrupt', {'secret': 'private-marker'}),
    ('session.steer', {'text': 'okay', 'attachments': ['private-marker']}),
    ('prompt.submit', {'text': 'okay', 'attachments': ['private-marker']}),
])
async def test_unsupported_payload_never_enters_commands_or_executes(local_host, monkeypatch, method, extra):
    from gateway.platforms.event import MessageEvent
    from tui_gateway.session_clients import SessionClientHub
    home, hub, provider, runner, adapter, source = _setup(local_host, monkeypatch)
    client = SessionClientHub(home)
    try:
        await runner._handle_message(MessageEvent(text='seed', source=source))
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        with sqlite3.connect(hub.path) as db:
            db.execute('CREATE TABLE control_audit(request TEXT)')
            db.execute('CREATE TRIGGER audit_control AFTER INSERT ON commands BEGIN INSERT INTO control_audit VALUES (NEW.request); END')
        with pytest.raises(ValueError):
            await asyncio.to_thread(client.call, entry.session_id, method, extra)
        with sqlite3.connect(hub.path) as db:
            assert db.execute('SELECT request FROM control_audit').fetchall() == []
        owner = hub.lookup(entry.session_id)
        forged = {'id': 'forged', 'method': method, 'params': {'session_id': owner['sid'], **extra}}
        rejected = await asyncio.to_thread(hub._execute, entry.session_id, forged)
        assert rejected.get('error', {}).get('code') == 4092, rejected
        assert len(provider.main_requests()) == 1
        assert not any('private-marker' in str(message) for message in adapter.messages)
    finally:
        client.close()
        _close_runner(runner)


@pytest.mark.asyncio
async def test_denied_desktop_stop_keeps_original_turn_and_presentation_running(local_host, monkeypatch):
    from gateway.config import Platform, PlatformConfig
    from gateway.platforms.event import MessageEvent
    from tests.fakes.fake_llm_provider import Text
    _home, hub, provider, runner, _adapter, source = _setup(local_host, monkeypatch)
    runner.config.platforms[Platform.DISCORD] = PlatformConfig(extra={'group_allow_admin_from': ['admin']})
    held, release = threading.Event(), threading.Event()
    turn = None

    def reply(record):
        if len(provider.main_requests()) == 2:
            held.set()
            assert release.wait(20)
        return Text('original answer')

    monkeypatch.setattr(provider, '_responder', reply)
    try:
        await runner._handle_message(MessageEvent(text='seed', source=source))
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        turn = asyncio.create_task(runner._handle_message(MessageEvent(text='held native work', source=source)))
        assert await asyncio.to_thread(held.wait, 10)
        agent = runner._cached_agent_for(entry.session_key)
        peer = DesktopPeer()
        resumed = await _rpc(peer, 'session.resume', session_id=entry.session_id)
        sid = resumed['result']['session_id']
        head = hub.event_head(entry.session_id)
        stopped = await _rpc(peer, 'session.interrupt', session_id=sid)
        assert stopped.get('error', {}).get('code') == 4092, stopped
        active = await _rpc(peer, 'session.activate', session_id=sid)
        assert active['result']['running'] is True
        assert runner._cached_agent_for(entry.session_key) is agent
        assert not any(e['frame']['params']['type'] == 'message.complete' for e in hub.events(entry.session_id, head))
        release.set()
        assert await turn == 'original answer'
        turn = None
        assert not any(e['frame']['params'].get('payload', {}).get('status') == 'interrupted'
                       for e in hub.events(entry.session_id, head))
    finally:
        release.set()
        if turn is not None:
            await turn
        _close_runner(runner)


@pytest.mark.asyncio
@pytest.mark.parametrize('receiving_allows', [True, False])
async def test_cached_source_uses_receiving_bot_not_conflicting_runtime_bot(local_host, monkeypatch, receiving_allows):
    from gateway.config import Platform, PlatformConfig, GatewayConfig
    from gateway.platforms.event import MessageEvent
    from gateway.session_identity import resolve_identity
    from hermes_cli.profiles import get_profile_dir
    home, _hub, provider, runner, runtime, source = _setup(local_host, monkeypatch)
    receiver = OfflineDiscord()
    receiver.set_owner_profile('receiver')
    receiving_home = get_profile_dir('receiver')
    receiving_home.mkdir(parents=True, exist_ok=True)
    (receiving_home / '.env').write_text('DISCORD_ALLOWED_USERS=authorized-member\n')
    runner.config.multiplex_profiles = True
    runner._profile_adapters = {'receiver': {Platform.DISCORD: receiver}}
    runner._profile_configs = {'receiver': GatewayConfig(platforms={Platform.DISCORD: PlatformConfig(
        extra={'group_allow_admin_from': ['admin'], 'group_user_allowed_commands': ['stop'] if receiving_allows else []})})}
    runner.config.platforms[Platform.DISCORD] = PlatformConfig(extra={
        'group_allow_admin_from': ['admin'], 'group_user_allowed_commands': [] if receiving_allows else ['stop']})
    source.profile = 'default'
    source._transport_adapter_ref = __import__('weakref').ref(receiver)
    resolve_identity(source, runner=runner, adapter=receiver, transport_profile='receiver', primary_home=home)
    runner._wire_adapter_handlers(receiver)
    from tests.fakes.fake_llm_provider import Text
    held, release = threading.Event(), threading.Event()
    turn = None

    def reply(record):
        if len(provider.main_requests()) == 2:
            held.set()
            assert release.wait(20)
        return Text('receiving bot answer')

    monkeypatch.setattr(provider, '_responder', reply)
    try:
        assert await runner._handle_message(MessageEvent(text='seed via receiver', source=source)) == 'receiving bot answer'
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        turn = asyncio.create_task(runner._handle_message(MessageEvent(text='held via receiver', source=source)))
        assert await asyncio.to_thread(held.wait, 10)
        peer = DesktopPeer()
        resumed = await _rpc(peer, 'session.resume', session_id=entry.session_id)
        assert 'result' in resumed, resumed
        stopped = await _rpc(peer, 'session.interrupt', session_id=resumed['result']['session_id'])
        if receiving_allows:
            assert stopped.get('result', {}).get('status') == 'interrupted', stopped
        else:
            assert stopped.get('error', {}).get('code') == 4092, stopped
            assert runner._is_session_running(entry.session_key)
        release.set()
        await turn
        turn = None
        submitted = await _rpc(peer, 'prompt.submit', session_id=resumed['result']['session_id'], text='receiving destination')
        assert 'result' in submitted, submitted
        await _until(lambda: any('receiving bot answer' in m[1] for m in receiver.messages))
        await _until(lambda: not receiver._active_sessions)
        assert runtime.messages == []
        assert any('receiving bot answer' in m[1] for m in receiver.messages)
    finally:
        release.set()
        if turn is not None:
            await turn
        _close_runner(runner)


@pytest.mark.asyncio
async def test_discovery_restart_replaces_only_executionless_service(local_host, monkeypatch):
    from gateway.session_client_bridge import GATEWAY_KEY, ensure_discovery
    from gateway.platforms.event import MessageEvent
    from tui_gateway.session_clients import SessionClientHub
    from tests.gateway.test_session_client_legacy_ownership import _runner
    home, hub, _provider, runner, _adapter, source = _setup(local_host, monkeypatch)
    restarted, _network, _source = _runner(home, monkeypatch)
    replacement = SessionClientHub(home)
    try:
        await runner._handle_message(MessageEvent(text='original conversation fence', source=source))
        entry = runner.session_store.lookup_by_session_key(runner._session_key_for_source(source))
        original = hub.lookup(entry.session_id)
        hub.retire(GATEWAY_KEY, '')
        ensure_discovery(restarted, replacement)
        assert replacement.lookup(GATEWAY_KEY)['online']
        assert replacement.lookup(GATEWAY_KEY)['owner'] == replacement.owner
        assert replacement.lookup(entry.session_id) == original
        with pytest.raises(RuntimeError):
            replacement.register_external(entry.session_id, 'gateway:replacement', lambda request: request)
        assert replacement.lookup(entry.session_id) == original
    finally:
        replacement.close()
        _close_runner(restarted)
        _close_runner(runner)

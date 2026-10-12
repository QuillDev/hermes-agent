"""Legacy Discord routes must not start a second writer after Desktop adoption.

Real GatewayRunner, SessionStore, RPC, native AIAgent and SQLite; only the model
HTTP provider and Discord network are fake. This fence is not a migration bridge.
"""
import weakref

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent
from gateway.session import SessionSource
from tests.tui_gateway.test_session_client_host import _request
from tests.tui_gateway import test_session_client_regressions as fixtures
from tests.tui_gateway.test_session_clients_live import DesktopPeer, _until


local_host = fixtures.local_host


class OfflineDiscord(BasePlatformAdapter):
    """External Discord network boundary, retaining native adapter lifecycle."""
    def __init__(self):
        super().__init__(PlatformConfig(enabled=True, typing_indicator=False), Platform.DISCORD)
        self.messages = []

    async def connect(self, *, is_reconnect=False):
        return True

    async def disconnect(self):
        return None

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        self.messages.append((chat_id, content, metadata))
        return SendResult(success=True, message_id=str(len(self.messages)))

    async def get_chat_info(self, chat_id):
        return {'name': 'Existing conversation', 'type': 'thread'}


def _runner(home, monkeypatch):
    from gateway.run import GatewayRunner
    monkeypatch.setenv('DISCORD_ALLOWED_USERS', 'authorized-member,second-member')
    runner = GatewayRunner(GatewayConfig(sessions_dir=home / 'sessions', multiplex_profiles=False))
    adapter = OfflineDiscord()
    runner.adapters[Platform.DISCORD] = adapter
    source = SessionSource(platform=Platform.DISCORD, chat_id='existing-thread',
                           thread_id='existing-thread', chat_type='thread', user_id='authorized-member')
    source._transport_adapter_ref = weakref.ref(adapter)
    return runner, adapter, source


def _close_runner(runner):
    runner._executor_closing = True
    if runner._executor:
        runner._executor.shutdown(wait=True)
    if runner._housekeeping_executor:
        runner._housekeeping_executor.shutdown(wait=True)
    for cached in runner._agent_cache.values():
        cached[0].close()
    runner.session_store.close_all_db_handles()
    runner._session_db_handle_cache.close_all(lambda db: db._db.close())


@pytest.mark.asyncio
@pytest.mark.parametrize('owner_state', ['live', 'disabled', 'retired'])
async def test_desktop_first_legacy_route_does_not_execute_a_second_turn(local_host, monkeypatch, owner_state):
    import hermes_state
    from hermes_state import SessionDB
    from tui_gateway import server
    home, hub, provider = local_host
    # The autouse fixture repoints DEFAULT_DB_PATH between tests; use this case's
    # actual profile for both the real native gateway and Desktop agent.
    monkeypatch.setattr(hermes_state, 'DEFAULT_DB_PATH', home / 'state.db')
    monkeypatch.setattr(server, '_db', None)
    runner, adapter, source = _runner(home, monkeypatch)
    try:
        entry = runner.session_store.get_or_create_session(source)
        runner.session_store.append_to_transcript(entry.session_id, {'role': 'user', 'content': 'legacy question'})
        runner.session_store.append_to_transcript(entry.session_id, {'role': 'assistant', 'content': 'legacy answer'})
        peer = DesktopPeer()
        # Seed a route that ALREADY points at a native hosted owner (e.g. a
        # pre-bridge adoption). Current resume must not manufacture that state
        # by probing an undiscovered legacy incumbent with cold-resume cleanup.
        created = _request(peer, {'id': 'already-owned', 'method': 'session.create',
                                  'params': {'source': 'desktop', 'messages': [
                                      {'role': 'user', 'content': 'legacy question'},
                                      {'role': 'assistant', 'content': 'legacy answer'}]}})
        assert 'result' in created, created
        entry = runner.session_store.switch_session(entry.session_key, created['result']['stored_session_id'])
        assert entry is not None
        resumed = _request(peer, {'id': 'desktop-adopt', 'method': 'session.resume',
                                 'params': {'session_id': entry.session_id, 'eager_build': True}})
        assert 'result' in resumed, resumed
        sid = resumed['result']['session_id']
        submitted = _request(peer, {'id': 'desktop-turn', 'method': 'prompt.submit',
                                   'params': {'session_id': sid, 'text': 'desktop continuation'}})
        assert submitted['result']['status'] == 'streaming', submitted
        await _until(lambda: any(f.get('params', {}).get('type') == 'message.complete' for f in peer.frames))
        await _until(lambda: not _request(peer, {'id': 'idle', 'method': 'session.activate',
                                              'params': {'session_id': sid}})['result']['running'])
        if owner_state == 'disabled':
            config = home / 'config.yaml'
            config.write_text(config.read_text().replace('enabled: true', 'enabled: false'))
        elif owner_state == 'retired':
            from tui_gateway import server
            server._close_session_by_id(sid, end_reason='idle_timeout')
        owner = hub.lookup(entry.session_id)
        route_before = entry.to_dict()
        response = await runner._handle_message(MessageEvent(text='Discord must not fork this owner', source=source))
        assert response and 'no replacement run' in response, response
        assert len(provider.main_requests()) == 1, provider.main_requests()
        assert hub.lookup(entry.session_id) == owner
        assert runner.session_store.lookup_by_session_key(entry.session_key).to_dict() == route_before
        assert hub.links('discord') == []
        with SessionDB(home / 'state.db') as db:
            history = db.get_messages_as_conversation(entry.session_id)
        assert sum(m.get('content') == 'desktop continuation' for m in history) == 1
        assert sum(m.get('content') == 'local hosted answer' for m in history) == 1
        assert not any('Discord must not fork' in str(m.get('content')) for m in history)
        assert adapter.messages == []
    finally:
        _close_runner(runner)

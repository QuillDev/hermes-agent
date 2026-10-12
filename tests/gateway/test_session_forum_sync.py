"""Automatic posts use real native RPC, Hub, SQLite and Discord SDK; only HTTP is fake."""
from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest
import pytest_asyncio
from aiohttp import web

from gateway.config import Platform, PlatformConfig
from gateway.platforms.event import MessageEvent
from gateway.session import SessionSource
from gateway.session_clients import handle_session_client_message, poll_session_clients
from tests.tui_gateway.test_session_client_host import _request
from tests.tui_gateway.test_session_clients_live import DesktopPeer, _until

GUILD = '1479890967405527113'
BOT = '111111111111111111'
OWNER = '222222222222222222'


class DiscordAPI:
    """Stateful Discord REST boundary, including ambiguous successful writes."""
    def __init__(self):
        self.channels = []
        self.threads = []
        self.messages = {}
        self.posts = []
        self.fail_after_create = False
        self.features = []
        self.reject_forum_status = None
        self.permissions = str((1 << 53) - 1)
        self.creates = []
        self.hold_create = False
        self.create_entered = asyncio.Event()
        self.release_create = asyncio.Event()
        self.hold_lookup = False
        self.lookup_entered = asyncio.Event()
        self.release_lookup = asyncio.Event()
        self.message_hook = None
        self.uploads = []

    def guild(self):
        return {'id': GUILD, 'name': 'test guild', 'owner_id': OWNER, 'features': self.features,
                'roles': [{'id': GUILD, 'name': '@everyone', 'permissions': '0', 'position': 0},
                          {'id': '333333333333333333', 'name': 'bot',
                           'permissions': self.permissions, 'position': 1}],
                'members': [self.member(BOT), self.member(OWNER)], 'channels': []}

    def user(self, uid):
        return {'id': uid, 'username': 'bot' if uid == BOT else 'owner', 'discriminator': '0',
                'avatar': None, 'bot': uid == BOT}

    def member(self, uid):
        return {'user': self.user(uid), 'roles': ['333333333333333333'] if uid == BOT else [],
                'joined_at': '2026-01-01T00:00:00+00:00', 'flags': 0}

    def message(self, cid, content):
        values = self.messages.setdefault(cid, [])
        item = {'id': str(900000000000000000 + len(self.posts)), 'channel_id': cid,
                'content': content, 'author': self.user(BOT), 'timestamp': '2026-10-12T00:00:00+00:00',
                'edited_timestamp': None, 'type': 0, 'attachments': [], 'embeds': [], 'mentions': [],
                'mention_roles': [], 'pinned': False, 'mention_everyone': False, 'tts': False}
        values.append(item)
        self.posts.append((cid, content))
        return item

    async def payload(self, request):
        if not request.content_type.startswith('multipart/'):
            return await request.json()
        fields = await request.multipart()
        payload = {}
        async for field in fields:
            data = await field.read()
            if field.name == 'payload_json':
                payload = json.loads(data)
            else:
                self.uploads.append((field.filename, bytes(data)))
        payload.setdefault('content', '')
        return payload

    async def handle(self, request):
        parts = request.path.strip('/').split('/')
        if request.method == 'GET':
            if parts == ['users', '@me']:
                value = self.user(BOT)
            elif parts == ['guilds', GUILD]:
                if self.hold_lookup:
                    self.lookup_entered.set()
                    await self.release_lookup.wait()
                value = self.guild()
            elif parts == ['guilds', GUILD, 'channels']:
                value = self.channels
            elif parts[:3] == ['guilds', GUILD, 'members']:
                value = self.member(parts[3])
            elif parts == ['guilds', GUILD, 'threads', 'active']:
                value = {'threads': [t for t in self.threads if not t['thread_metadata']['archived']],
                         'members': [], 'has_more': False}
            elif len(parts) == 5 and parts[2:] == ['threads', 'archived', 'public']:
                value = {'threads': [t for t in self.threads if t['thread_metadata']['archived']],
                         'members': [], 'has_more': False}
            elif len(parts) == 4 and parts[2] == 'messages':
                value = self.messages[parts[1]][0]
            elif len(parts) == 2 and parts[0] == 'channels':
                value = next(c for c in [*self.channels, *self.threads] if c['id'] == parts[1])
            else:
                return web.json_response({'message': 'unknown route'}, status=404)
            return web.Response(body=json.dumps(value).encode(), content_type='application/json')
        if request.method == 'POST':
            payload = await self.payload(request)
            if parts == ['guilds', GUILD, 'channels']:
                if self.hold_create:
                    self.create_entered.set()
                    await self.release_create.wait()
                if self.reject_forum_status:
                    return web.json_response({'message': 'Forum unavailable for this guild'}, status=self.reject_forum_status)
                value = {'id': '444444444444444444', 'guild_id': GUILD, 'position': 0, 'flags': 0, **payload}
                self.channels.append(value)
                self.creates.append(request.path)
            elif len(parts) == 3 and parts[2] == 'threads':
                tid = str(555555555555555555 + len(self.threads))
                value = {'id': tid, 'guild_id': GUILD, 'parent_id': parts[1], 'owner_id': BOT,
                         'type': 11, 'name': payload['name'], 'message_count': 1, 'member_count': 1,
                         'rate_limit_per_user': 0, 'thread_metadata': {'archived': False, 'locked': False,
                         'auto_archive_duration': 1440, 'archive_timestamp': '2026-10-12T00:00:00+00:00'}}
                self.threads.append(value)
                starter = self.message(tid, payload['message']['content'])
                starter['id'] = tid
                value = {**value, 'message': starter}
                self.creates.append(request.path)
            elif len(parts) == 3 and parts[2] == 'messages':
                value = self.message(parts[1], payload['content'])
                if payload.get('attachments'):
                    value['attachments'] = [{'id': '777777777777777777', 'filename': name, 'size': len(data),
                                             'url': 'https://cdn.test/' + name, 'proxy_url': 'https://cdn.test/' + name}
                                            for name, data in self.uploads[-len(payload['attachments']):]]
                if self.message_hook:
                    self.message_hook(payload['content'])
            else:
                return web.json_response({'message': 'unknown route'}, status=404)
            if self.fail_after_create and parts[-1] == 'threads':
                self.fail_after_create = False
                return web.json_response({'message': 'ambiguous upstream failure'}, status=503)
            return web.Response(body=json.dumps(value).encode(), content_type='application/json')
        return web.Response(status=204)


@pytest_asyncio.fixture
async def discord_api(monkeypatch):
    # Gateway's conftest installs optional-library mocks before collection. This
    # contract needs the actual SDK and only fakes its external HTTP boundary.
    import importlib
    from importlib.machinery import PathFinder
    import sys
    from types import ModuleType
    if not isinstance(sys.modules.get('discord'), ModuleType):
        if PathFinder.find_spec('discord') is None:
            pytest.skip('Real Discord SDK required: build the PM test environment with --extra discord')
        for name in list(sys.modules):
            if name == 'discord' or name.startswith('discord.'):
                sys.modules.pop(name)
        importlib.import_module('discord')
    import discord
    from discord.ext import commands
    from plugins.platforms.discord.adapter import DiscordAdapter
    api = DiscordAPI()
    app = web.Application()
    app.router.add_route('*', '/{path:.*}', api.handle)
    http = web.AppRunner(app)
    await http.setup()
    site = web.TCPSite(http, '127.0.0.1', 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    monkeypatch.setattr(discord.http.Route, 'BASE', f'http://127.0.0.1:{port}')
    client = commands.Bot(command_prefix='!', intents=discord.Intents.none())
    await client._async_setup_hook()
    data = await client.http.static_login('isolated-test-token')
    client._connection.user = discord.ClientUser(state=client._connection, data=data)
    client._connection._add_guild_from_data(api.guild())
    adapter = DiscordAdapter(PlatformConfig(enabled=True, extra={'allow_from': [OWNER], 'group_allow_from': [OWNER]}))
    adapter._client = client
    try:
        yield api, adapter
    finally:
        await client.close()
        await http.cleanup()


def _runner(home, adapter):
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner
    runner = GatewayRunner(GatewayConfig(sessions_dir=home / 'sessions', multiplex_profiles=False,
                                        platforms={Platform.DISCORD: adapter.config}))
    runner.adapters[Platform.DISCORD] = adapter
    adapter.set_authorization_check(runner._make_adapter_auth_check(Platform.DISCORD))
    return runner


def _poll_process(home, url):
    """A second gateway process: real RPC mailbox and SDK, no model/actor admission."""
    import os
    os.environ['HERMES_HOME'] = home
    os.environ['HERMES_RUNTIME_DIR'] = home + '/poll-runtime'
    import hermes_bootstrap
    import discord
    from discord.ext import commands
    from pathlib import Path
    from plugins.platforms.discord.adapter import DiscordAdapter
    from tests.gateway.test_session_client_legacy_ownership import _close_runner
    from tui_gateway.session_clients import get_hub

    async def run():
        discord.http.Route.BASE = url
        client = commands.Bot(command_prefix='!', intents=discord.Intents.none())
        await client._async_setup_hook()
        client._connection.user = discord.ClientUser(state=client._connection,
                                                     data=await client.http.static_login('isolated-test-token'))
        client._connection._add_guild_from_data(await client.http.get_guild(int(GUILD)))
        adapter = DiscordAdapter(PlatformConfig(enabled=True, extra={'allow_from': [OWNER], 'group_allow_from': [OWNER]}))
        adapter._client = client
        runner = _runner(Path(home), adapter)
        hub = get_hub(home)
        try:
            await poll_session_clients(runner, Path(home), 'default', {})
            from tui_gateway import server
            assert server._sessions == {}  # Polling never creates a replacement actor.
        finally:
            _close_runner(runner)
            hub.close()
            await client.close()
    asyncio.run(run())


async def _cross_process_poll(home):
    import sys
    import discord
    child = await asyncio.create_subprocess_exec(sys.executable, '-c',
        'from tests.gateway.test_session_forum_sync import _poll_process; _poll_process(' +
        repr(str(home)) + ',' + repr(discord.http.Route.BASE) + ')',
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    output, error = await asyncio.wait_for(child.communicate(), 30)
    assert child.returncode == 0, (output.decode(), error.decode())


def config(automatic=True, activation='first', guild=GUILD):
    return ('session_clients:\n  enabled: true\n  discord_forum:\n'
            f'    automatic: {str(automatic).lower()}\n    activation_id: {activation}\n'
            f'    guild_id: "{guild}"\n    forum_name: hermes-sessions\n')


def _configure(home, **settings):
    import hermes_yaml as yaml
    from hermes_cli.config import atomic_config_write
    path = home / 'config.yaml'
    current = yaml.safe_load(path.read_text())
    current['session_clients'] = yaml.safe_load(config(**settings))['session_clients']
    atomic_config_write(path, current)


@pytest.mark.asyncio
async def test_new_persistent_desktop_first_exchange_and_forum_reply_keep_one_owner(tmp_path, monkeypatch, discord_api):
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text, write_hermes_home
    from tui_gateway import server
    from tui_gateway.session_clients import get_hub
    from hermes_state import SessionDB
    api, adapter = discord_api
    peer = DesktopPeer()
    with FakeLLMServer(lambda record: Text('canonical answer')) as provider:
        home = write_hermes_home(tmp_path / 'profile', provider.base_url, extra_config=config())
        monkeypatch.setenv('HERMES_HOME', str(home))
        monkeypatch.setattr(server, '_hermes_home', home)
        hub = get_hub(home, execution_host=True)
        runner = _runner(home, adapter)
        try:
            created = _request(peer, {'id': 'create', 'method': 'session.create', 'params': {'source': 'desktop'}})
            assert 'result' in created, created
            sid, key = created['result']['session_id'], created['result']['stored_session_id']
            original = hub.lookup(key)
            await poll_session_clients(runner, home, 'default', {})
            assert api.threads == []  # A Desktop draft isn't a persistent conversation.
            submitted = _request(peer, {'id': 'prompt', 'method': 'prompt.submit',
                                       'params': {'session_id': sid, 'text': 'first desktop prompt'}})
            assert submitted['result']['status'] == 'streaming', submitted
            await _until(lambda: any(f.get('params', {}).get('type') == 'message.complete' for f in peer.frames))
            # The first exchange completed BEFORE the forum existed. Concurrent polls must catch both.
            api.hold_create = True
            creating = asyncio.create_task(poll_session_clients(runner, home, 'default', {}))
            await asyncio.wait_for(api.create_entered.wait(), 20)
            try:
                # Both OS processes contend while the first claim has an unacknowledged POST.
                await asyncio.gather(*(poll_session_clients(runner, home, 'default', {}) for _ in range(3)),
                                     _cross_process_poll(home), _cross_process_poll(home))
                assert api.threads == []
            finally:
                api.release_create.set()
                await creating
            assert len(api.threads) == 1
            tid = api.threads[0]['id']
            assert api.threads[0]['parent_id'] == api.channels[0]['id']
            assert api.channels[0]['guild_id'] == GUILD and api.channels[0]['type'] == 15
            assert hub.link('discord', tid)['session_key'] == key
            assert [text for chat, text in api.posts if chat == tid][1:] == ['first desktop prompt', 'canonical answer']
            await poll_session_clients(runner, home, 'default', {})  # Fresh watcher state / restart.
            assert len(api.threads) == 1 and len(api.posts) == 3
            source = adapter.build_source(chat_id=tid, user_id=OWNER, guild_id=GUILD,
                                          parent_chat_id=api.channels[0]['id'], chat_type='thread', thread_id=tid)
            assert await runner._handle_message(MessageEvent(text='forum followup', source=source)) is None
            await _until(lambda: len(provider.main_requests()) == 2 and not server._sessions[sid].get('running'))
            await poll_session_clients(runner, home, 'default', {})
            assert hub.lookup(key)['sid'] == original['sid'] and hub.lookup(key)['owner'] == original['owner']
            with SessionDB(home / 'state.db') as db:
                history = db.get_messages_as_conversation(key)
            assert [m['content'] for m in history if m['role'] == 'user'] == ['first desktop prompt', 'forum followup']
            assert len(provider.main_requests()) == 2
            assert [text for _, text in api.posts].count('forum followup') == 0
            assert [text for _, text in api.posts].count('canonical answer') == 2
        finally:
            from tests.gateway.test_session_client_legacy_ownership import _close_runner
            _close_runner(runner)
            server._shutdown_sessions()
            hub.close()


@pytest_asyncio.fixture
async def native_case(tmp_path, monkeypatch, discord_api):
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text, write_hermes_home
    from tui_gateway import server
    from tui_gateway.session_clients import get_hub
    from tests.gateway.test_session_client_legacy_ownership import _close_runner
    api, adapter = discord_api
    with FakeLLMServer(lambda record: Text('one native answer')) as provider:
        home = write_hermes_home(tmp_path / 'profile', provider.base_url, extra_config=config())
        monkeypatch.setenv('HERMES_HOME', str(home))
        monkeypatch.setattr(server, '_hermes_home', home)
        hub = get_hub(home, execution_host=True)
        runner = _runner(home, adapter)
        try:
            yield home, hub, provider, runner, api, adapter
        finally:
            _close_runner(runner)
            server._shutdown_sessions()
            get_hub(home, start=False).close()


async def _exchange(text='new prompt', **create_params):
    peer = DesktopPeer()
    created = _request(peer, {'id': 'create', 'method': 'session.create',
                             'params': {'source': 'desktop', **create_params}})
    assert 'result' in created, created
    sid, key = created['result']['session_id'], created['result']['stored_session_id']
    submitted = _request(peer, {'id': 'prompt', 'method': 'prompt.submit',
                               'params': {'session_id': sid, 'text': text}})
    assert submitted['result']['status'] == 'streaming', submitted
    await _until(lambda: any(f.get('params', {}).get('type') == 'message.complete' for f in peer.frames))
    return sid, key


@pytest.mark.asyncio
async def test_ambiguous_created_post_is_reconciled_after_restart_even_when_archived(native_case):
    import sqlite3
    from gateway.run import GatewayRunner
    from tests.gateway.test_session_client_legacy_ownership import _close_runner
    home, hub, provider, runner, api, adapter = native_case
    sid, key = await _exchange()
    api.fail_after_create = True
    await poll_session_clients(runner, home, 'default', {})
    assert len(api.threads) == 1 and hub.links('discord') == []
    assert len(provider.main_requests()) == 1
    # Model a terminated poll process's durable claim, not a TTL/timestamp guess.
    with sqlite3.connect(home / 'session_clients_gateway.db') as db:
        db.execute("INSERT OR REPLACE INTO forum_poll_claim VALUES (1,'crashed',2147483647,0)")
    api.threads[0]['thread_metadata']['archived'] = True
    restarted = _runner(home, adapter)
    try:
        await poll_session_clients(restarted, home, 'default', {})
        await poll_session_clients(restarted, home, 'default', {})
        assert len(api.threads) == 1
        tid = api.threads[0]['id']
        assert hub.link('discord', tid)['session_key'] == key
        assert [text for chat, text in api.posts if chat == tid][1:] == ['new prompt', 'one native answer']
        assert hub.lookup(key)['sid'] == sid
        assert len(provider.main_requests()) == 1
    finally:
        _close_runner(restarted)


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['guild-capability', 'permissions', 'owner-policy', 'wrong-guild'])
async def test_unavailable_forum_fails_closed_without_replaying_admitted_task(native_case, failure):
    home, hub, provider, runner, api, adapter = native_case
    if failure == 'guild-capability':
        api.reject_forum_status = 400
    elif failure == 'permissions':
        api.permissions = '0'
    elif failure == 'owner-policy':
        adapter.config.extra['allow_from'] = []
        adapter.config.extra['group_allow_from'] = []
    else:
        _configure(home, guild='999999999999999999')
    sid, key = await _exchange()
    await asyncio.gather(*(poll_session_clients(runner, home, 'default', {}) for _ in range(3)))
    await poll_session_clients(runner, home, 'default', {})
    assert api.creates == [] and api.posts == []
    assert hub.links('discord') == []
    assert len(provider.main_requests()) == 1
    assert hub.lookup(key)['sid'] == sid


@pytest.mark.asyncio
@pytest.mark.parametrize('private', [True, False])
async def test_existing_forum_without_provisioning_permissions_is_reused_only_when_private(native_case, private):
    home, hub, provider, runner, api, _adapter = native_case
    view = 1 << 10
    api.permissions = str(view | (1 << 11) | (1 << 16) | (1 << 38))
    api.channels.append({'id': '444444444444444444', 'guild_id': GUILD, 'name': 'hermes-sessions',
                         'type': 15, 'position': 0, 'flags': 0, 'permission_overwrites': [
                             {'id': GUILD, 'type': 0, 'allow': '0', 'deny': str(view) if private else '0'},
                             {'id': BOT, 'type': 1, 'allow': api.permissions, 'deny': '0'},
                             {'id': OWNER, 'type': 1, 'allow': api.permissions, 'deny': '0'}]})
    _, key = await _exchange()
    await poll_session_clients(runner, home, 'default', {})
    if private:
        assert len(api.threads) == 1
        assert hub.link('discord', api.threads[0]['id'])['session_key'] == key
        assert api.creates == ['/channels/444444444444444444/threads']
        assert [text for _, text in api.posts][1:] == ['new prompt', 'one native answer']
    else:
        assert api.threads == [] and api.posts == [] and hub.links('discord') == []
    assert len(provider.main_requests()) == 1


@pytest.mark.asyncio
async def test_deleted_post_is_never_blindly_recreated(native_case):
    home, hub, provider, runner, api, _adapter = native_case
    _, key = await _exchange()
    api.fail_after_create = True
    await poll_session_clients(runner, home, 'default', {})
    assert len(api.threads) == 1
    api.threads.clear()  # Could also represent a created post whose visibility is uncertain.
    await poll_session_clients(runner, home, 'default', {})
    await poll_session_clients(runner, home, 'default', {})
    assert api.threads == [] and hub.links('discord') == []
    assert api.creates.count('/channels/444444444444444444/threads') == 1
    assert len(provider.main_requests()) == 1
    assert hub.lookup(key) is not None


@pytest.mark.asyncio
async def test_activation_excludes_preexisting_drafts_and_imported_history(native_case):
    home, hub, provider, runner, api, _adapter = native_case
    _configure(home, automatic=False)
    peer = DesktopPeer()
    old = _request(peer, {'id': 'old-draft', 'method': 'session.create', 'params': {'source': 'desktop'}})['result']
    await _exchange('preexisting private conversation')
    _configure(home)
    submitted = _request(peer, {'id': 'old-first-prompt', 'method': 'prompt.submit',
                               'params': {'session_id': old['session_id'], 'text': 'draft created before activation'}})
    assert submitted['result']['status'] == 'streaming'
    await _until(lambda: any(f.get('params', {}).get('type') == 'message.complete' for f in peer.frames))
    await _exchange('imported conversation', messages=[{'role': 'user', 'content': 'old private question'},
                                                      {'role': 'assistant', 'content': 'old private answer'}])
    await poll_session_clients(runner, home, 'default', {})
    assert api.posts == [] and hub.links('discord') == []
    _, new_key = await _exchange('qualifying new conversation')
    await poll_session_clients(runner, home, 'default', {})
    assert len(api.threads) == 1
    assert hub.links('discord')[0]['session_key'] == new_key
    assert [text for _, text in api.posts][1:] == ['qualifying new conversation', 'one native answer']
    assert len(provider.main_requests()) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['preenable', 'imported', 'unstamped', 'old-draft', 'restart'])
async def test_create_retry_never_upgrades_original_private_admission(native_case, kind):
    import sqlite3
    from tui_gateway import server
    home, hub, _provider, runner, api, _adapter = native_case
    if kind != 'imported':
        _configure(home, automatic=False)
    params = {'source': 'desktop', 'idempotency_key': 'private-logical-create'}
    if kind == 'imported':
        params['messages'] = [{'role': 'user', 'content': 'imported private history'}]
    peer = DesktopPeer()
    original = _request(peer, {'id': 'original', 'method': 'session.create', 'params': params})['result']
    if kind != 'old-draft':
        _request(peer, {'id': 'private', 'method': 'prompt.submit',
                        'params': {'session_id': original['session_id'], 'text': 'old private prompt'}})
        await _until(lambda: any(f.get('params', {}).get('type') == 'message.complete' for f in peer.frames))
    if kind == 'unstamped':
        with sqlite3.connect(home / 'session_clients_gateway.db') as db:
            db.execute('CREATE TABLE IF NOT EXISTS forum_enrollments (session_key TEXT PRIMARY KEY, scope TEXT, sent INTEGER DEFAULT 0, chat_id TEXT, guild_id TEXT, forum_id TEXT, bot_id TEXT, user_id TEXT)')
            db.execute('DELETE FROM forum_enrollments')
    if kind == 'restart':
        server._shutdown_sessions()
        server._idempotency_keys.clear()
    _configure(home, activation='new-epoch')
    retry = _request(peer, {'id': 'retry', 'method': 'session.create',
                           'params': {'source': 'desktop', 'idempotency_key': 'private-logical-create'}})['result']
    if kind in {'old-draft', 'restart'}:
        peer.frames.clear()
        _request(peer, {'id': 'later', 'method': 'prompt.submit',
                        'params': {'session_id': retry['session_id'], 'text': 'later private prompt'}})
        await _until(lambda: any(f.get('params', {}).get('type') == 'message.complete' for f in peer.frames))
    await poll_session_clients(runner, home, 'default', {})
    assert api.posts == [] and api.threads == [] and hub.links('discord') == []
    assert hub.lookup(retry['stored_session_id']) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize('revocation', ['disabled', 'activation', 'guild'])
async def test_intake_revoked_during_discord_authorization_admits_nothing(native_case, revocation):
    from hermes_state import SessionDB
    home, hub, provider, runner, api, adapter = native_case
    sid, key = await _exchange()
    await poll_session_clients(runner, home, 'default', {})
    tid, fid = api.threads[0]['id'], api.channels[0]['id']
    source = adapter.build_source(chat_id=tid, user_id=OWNER, guild_id=GUILD,
                                  parent_chat_id=fid, chat_type='thread', thread_id=tid)
    before = hub.event_head(key)
    api.hold_lookup = True
    task = asyncio.create_task(handle_session_client_message(runner, MessageEvent(text='revoked private prompt', source=source)))
    await asyncio.wait_for(api.lookup_entered.wait(), 10)
    _configure(home, **{'disabled': {'automatic': False}, 'activation': {'activation': 'revoked'},
                       'guild': {'guild': '999999999999999999'}}[revocation])
    api.release_lookup.set()
    handled, reply = await task
    assert handled and reply is not None
    assert hub.event_head(key) == before
    with SessionDB(home / 'state.db') as db:
        assert 'revoked private prompt' not in str(db.get_messages_as_conversation(key))
    assert len(provider.main_requests()) == 1 and len(api.posts) == 3
    assert hub.lookup(key)['sid'] == sid


@pytest.mark.asyncio
@pytest.mark.parametrize('revocation', ['disabled', 'activation'])
async def test_batch_revocation_after_first_send_suppresses_next_frame_without_cursor_advance(native_case, revocation):
    from gateway.session_clients import _cursor
    home, hub, provider, runner, api, _adapter = native_case
    _, key = await _exchange()
    await poll_session_clients(runner, home, 'default', {})
    tid = api.threads[0]['id']
    before = _cursor(home, key, tid)
    first = hub.publish(key, {'method': 'event', 'params': {'type': 'message.complete', 'payload': {'text': 'allowed in-flight'}}})
    second = hub.publish(key, {'method': 'event', 'params': {'type': 'message.complete', 'payload': {'text': 'must stay private'}}})
    api.message_hook = lambda _text: _configure(home, **({'automatic': False} if revocation == 'disabled' else {'activation': 'revoked'}))
    await poll_session_clients(runner, home, 'default', {})
    assert [text for _, text in api.posts][3:] == ['allowed in-flight']
    assert before <= _cursor(home, key, tid) <= first < second
    assert len(provider.main_requests()) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('automatic', [True, False])
@pytest.mark.parametrize('command', ['stop', 'steer', 's', 'cancel', 'context', 'ctx'])
async def test_receiving_bot_slash_denial_preserves_original_native_turn(native_case, automatic, command):
    import copy
    import threading
    from tests.fakes.fake_llm_provider import Text
    from tui_gateway import server
    home, hub, provider, runner, api, adapter = native_case
    sid, key = await _exchange()
    await poll_session_clients(runner, home, 'default', {})
    tid, fid = api.threads[0]['id'], api.channels[0]['id']
    if not automatic:
        import sqlite3
        with sqlite3.connect(home / 'session_clients_gateway.db') as db:
            db.execute('UPDATE forum_enrollments SET chat_id=NULL WHERE session_key=?', (key,))
    adapter.config.extra['group_allow_admin_from'] = ['999999999999999999']
    adapter.config.extra['group_user_allowed_commands'] = []
    entered, release = threading.Event(), threading.Event()
    def blocked(_record):
        entered.set()
        assert release.wait(15)
        return Text('original turn completed')
    provider._responder = blocked
    peer = DesktopPeer()
    _request(peer, {'id': 'running', 'method': 'prompt.submit',
                    'params': {'session_id': sid, 'text': 'original live turn'}})
    await _until(entered.is_set)
    session = server._sessions[sid]
    original_agent = session['agent']
    inflight = copy.deepcopy(session['inflight_turn'])
    head = hub.event_head(key)
    source = adapter.build_source(chat_id=tid, user_id=OWNER, guild_id=GUILD,
                                  parent_chat_id=fid, chat_type='thread', thread_id=tid)
    try:
        reply = await runner._handle_message(MessageEvent(text=f'/{command} forbidden correction', source=source))
        assert isinstance(reply, str) and ('admin' in reply.lower() or 'not supported' in reply.lower())
        assert session['agent'] is original_agent and session['running']
        assert session['inflight_turn'] == inflight and not session.get('_turn_user_input')
        assert not any(row['frame'].get('params', {}).get('type') == 'session.clients.input' for row in hub.events(key, head))
        assert len(provider.main_requests()) == 2
    finally:
        release.set()
        await _until(lambda: not session.get('running'))
    assert session['agent'] is original_agent
    assert len(provider.main_requests()) == 2


@pytest.mark.asyncio
async def test_revocation_between_text_and_attachment_keeps_file_private_and_cursor_pending(native_case, monkeypatch):
    from gateway.session_clients import _cursor
    home, hub, provider, runner, api, _adapter = native_case
    _, key = await _exchange()
    await poll_session_clients(runner, home, 'default', {})
    tid = api.threads[0]['id']
    monkeypatch.setattr('gateway.platforms.base._HERMES_ROOT', home)
    monkeypatch.setattr('gateway.platforms.base.MEDIA_DELIVERY_SAFE_ROOTS', (home / 'cache',))
    file = home / 'cache' / 'private-file.txt'
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text('private file bytes')
    # Prove this file really uploads through the owned SDK path when permitted.
    hub.publish(key, {'method': 'event', 'params': {'type': 'session.clients.attachment',
                     'payload': {'path': str(file)}}})
    await poll_session_clients(runner, home, 'default', {})
    assert api.uploads == [('private-file.txt', b'private file bytes')]
    api.uploads.clear()
    sent = len(api.posts)
    before = _cursor(home, key, tid)
    seq = hub.publish(key, {'method': 'event', 'params': {'type': 'message.complete',
                       'payload': {'text': 'in-flight text', 'attachments': [{'path': str(file)}]}}})
    api.message_hook = lambda _text: _configure(home, automatic=False)
    await poll_session_clients(runner, home, 'default', {})
    assert [text for _, text in api.posts][sent:] == ['in-flight text']
    assert api.uploads == [] and before <= _cursor(home, key, tid) < seq
    assert len(provider.main_requests()) == 1


@pytest.mark.asyncio
async def test_mailbox_owner_revalidates_activation_before_native_execution(native_case):
    from gateway.session_forum_sync import automatic_destination
    from tui_gateway.session_clients import SessionClientHub
    from tui_gateway import server
    home, hub, provider, runner, api, _adapter = native_case
    sid, key = await _exchange()
    await poll_session_clients(runner, home, 'default', {})
    tid = api.threads[0]['id']
    record = automatic_destination(home, tid)
    hub._stop.set()
    await asyncio.to_thread(hub._thread.join, 10)
    hub._thread = None
    remote = SessionClientHub(home, start=False)
    request = asyncio.create_task(asyncio.to_thread(remote.call, key, 'prompt.submit', {'text': 'revoked queued private prompt'},
                                  automatic_forum={'scope': record['scope'], 'chat_id': tid}))
    def pending():
        with hub._db() as db:
            return db.execute("SELECT 1 FROM commands WHERE state='pending'").fetchone() is not None
    try:
        await _until(pending)
        _configure(home, automatic=False)
        hub._stop.clear()
        hub.start()
        reply = await asyncio.wait_for(request, 10)
        assert reply.get('error', {}).get('code') == 4093
        assert not server._sessions[sid].get('running')
        assert len(provider.main_requests()) == 1
        assert not any('revoked queued private prompt' in str(row['frame']) for row in hub.events(key, 0))
    finally:
        hub._stop.clear()
        hub.start()
        remote.close()


@pytest.mark.asyncio
async def test_revoked_automatic_controls_and_attachments_never_enter_mailbox(native_case):
    from gateway.session_forum_sync import automatic_destination
    from tui_gateway.session_clients import SessionClientHub
    from tui_gateway import server
    home, hub, provider, runner, api, _adapter = native_case
    sid, key = await _exchange()
    await poll_session_clients(runner, home, 'default', {})
    tid = api.threads[0]['id']
    record = automatic_destination(home, tid)
    remote = SessionClientHub(home, start=False)
    _configure(home, automatic=False)
    original_agent = server._sessions[sid]['agent']
    try:
        for method, params in [('prompt.submit', {'text': 'private'}), ('session.interrupt', {}),
                               ('session.steer', {'text': 'private'}), ('file.attach', {'path': '/not-admitted'}),
                               ('image.attach', {'path': '/not-admitted'})]:
            with pytest.raises(RuntimeError, match='revoked'):
                await asyncio.to_thread(remote.call, key, method, params,
                    automatic_forum={'scope': record['scope'], 'chat_id': tid})
        with hub._db() as db:
            assert db.execute('SELECT COUNT(*) FROM commands').fetchone()[0] == 0
        assert server._sessions[sid]['agent'] is original_agent
        assert server._sessions[sid]['attached_images'] == []
        assert len(provider.main_requests()) == 1
    finally:
        remote.close()


async def _cold_create_retry(home, url, automatic):
    import discord
    from discord.ext import commands
    from plugins.platforms.discord.adapter import DiscordAdapter
    from tests.gateway.test_session_client_legacy_ownership import _close_runner
    from tui_gateway import server
    from tui_gateway.session_clients import get_hub
    _configure(home, automatic=automatic, activation='cold-second' if automatic else 'cold-first')
    hub = get_hub(home, execution_host=True)
    client, runner = None, None
    try:
        if automatic:
            # A cold public retry is owner-fenced, not permission to create a
            # replacement actor under the original logical key.
            retried = _request(DesktopPeer(), {'id': 'retry', 'method': 'session.create',
                'params': {'source': 'desktop', 'idempotency_key': 'cold-private-create'}})
            assert retried.get('error', {}).get('code') == 4092, retried
            with hub._db() as db:
                assert db.execute('SELECT COUNT(*) FROM commands').fetchone()[0] == 0
            import sqlite3
            with sqlite3.connect(home / 'session_clients_gateway.db') as db:
                decisions = list(db.execute('SELECT scope FROM forum_create_decisions'))
            assert decisions == [('',)]
        else:
            await _exchange('private logical create', idempotency_key='cold-private-create')
        discord.http.Route.BASE = url
        client = commands.Bot(command_prefix='!', intents=discord.Intents.none())
        await client._async_setup_hook()
        client._connection.user = discord.ClientUser(state=client._connection,
                                                     data=await client.http.static_login('isolated-test-token'))
        client._connection._add_guild_from_data(await client.http.get_guild(int(GUILD)))
        adapter = DiscordAdapter(PlatformConfig(enabled=True, extra={'allow_from': [OWNER], 'group_allow_from': [OWNER]}))
        adapter._client = client
        runner = _runner(home, adapter)
        await poll_session_clients(runner, home, 'default', {})
    finally:
        if runner:
            _close_runner(runner)
        server._shutdown_sessions()
        hub.close()
        if client:
            await client.close()


@pytest.mark.asyncio
async def test_original_private_create_decision_survives_actual_host_process_restart(native_case, tmp_path):
    import os
    import sys
    import discord
    from tests.fakes.fake_llm_provider import write_hermes_home
    _home, _hub, provider, _runner_instance, api, _adapter = native_case
    home = write_hermes_home(tmp_path / 'cold-profile', provider.base_url, extra_config=config())
    env = {**os.environ, 'HERMES_HOME': str(home), 'HERMES_RUNTIME_DIR': str(home / 'runtime')}
    for automatic in (False, True):
        script = ('import hermes_bootstrap; import run_agent; import asyncio; from pathlib import Path; '
                  'from tests.gateway.test_session_forum_sync import _cold_create_retry; '
                  'asyncio.run(_cold_create_retry(Path(' + repr(str(home)) + '),' +
                  repr(discord.http.Route.BASE) + ',' + repr(automatic) + '))')
        child = await asyncio.create_subprocess_exec(sys.executable, '-c', script, env=env,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        output, error = await asyncio.wait_for(child.communicate(), 30)
        assert child.returncode == 0, (output.decode(), error.decode())
        assert api.posts == [] and api.threads == []
    assert len(provider.main_requests()) == 1


async def _private_profile_exchange(home):
    import os
    import sys
    env = {**os.environ, 'HERMES_HOME': str(home), 'HERMES_RUNTIME_DIR': str(home / 'runtime')}
    script = ('import asyncio; from tui_gateway.session_clients import get_hub; '
              'from tui_gateway import server; from tests.gateway.test_session_forum_sync import _exchange; '
              'hub=get_hub(' + repr(str(home)) + ',execution_host=True); '
              'asyncio.run(_exchange("another profile private prompt")); '
              'assert hub.links("discord")==[]; server._shutdown_sessions(); hub.close()')
    child = await asyncio.create_subprocess_exec(sys.executable, '-c', script, env=env,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    output, error = await asyncio.wait_for(child.communicate(), 30)
    assert child.returncode == 0, (output.decode(), error.decode())


@pytest.mark.asyncio
async def test_unconfigured_profile_stays_private_on_same_host(native_case, tmp_path):
    from tests.fakes.fake_llm_provider import write_hermes_home
    from tui_gateway.session_clients import get_hub
    home, hub, provider, runner, api, _adapter = native_case
    await _exchange()
    await poll_session_clients(runner, home, 'default', {})
    assert len(api.threads) == 1 and len(api.posts) == 3
    other = write_hermes_home(tmp_path / 'other-profile', provider.base_url,
                              extra_config='session_clients:\n  enabled: true\n')
    await _private_profile_exchange(other)
    await poll_session_clients(runner, other, 'default', {})
    other_hub = get_hub(other)
    try:
        assert other_hub.links('discord') == []
        assert len(api.threads) == 1 and len(api.posts) == 3
        assert len(provider.main_requests()) == 2
        assert len(hub.links('discord')) == 1
    finally:
        other_hub.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('violation', ['disabled', 'foreign-guild', 'foreign-bot', 'foreign-user', 'public-overwrite'])
async def test_linked_forum_reply_requires_live_receiving_bot_and_private_policy(native_case, violation):
    import discord
    home, hub, provider, runner, api, adapter = native_case
    sid, key = await _exchange()
    await poll_session_clients(runner, home, 'default', {})
    tid, fid = api.threads[0]['id'], api.channels[0]['id']
    source = adapter.build_source(chat_id=tid, user_id=OWNER, guild_id=GUILD,
                                  parent_chat_id=fid, chat_type='thread', thread_id=tid)
    mutations = {
        'disabled': lambda: _configure(home, automatic=False),
        'foreign-guild': lambda: setattr(source, 'guild_id', '999999999999999999'),
        'foreign-user': lambda: setattr(source, 'user_id', '999999999999999999'),
        'foreign-bot': lambda: setattr(adapter._client._connection, 'user', discord.ClientUser(
            state=adapter._client._connection, data=api.user('999999999999999999'))),
        # An uncached member must not be omitted by SDK's public overwrite mapping.
        'public-overwrite': lambda: api.channels[0]['permission_overwrites'].append({
            'id': '999999999999999999', 'type': 1, 'allow': str(1 << 10), 'deny': '0'}),
    }
    mutations[violation]()
    handled, reply = await handle_session_client_message(runner, MessageEvent(text='must stay unadmitted', source=source))
    assert handled and reply is not None
    await poll_session_clients(runner, home, 'default', {})
    assert len(provider.main_requests()) == 1
    assert len(api.posts) == 3 and len(api.threads) == 1
    assert hub.lookup(key)['sid'] == sid

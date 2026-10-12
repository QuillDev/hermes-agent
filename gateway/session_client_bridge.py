"""In-place legacy GatewayRunner owner for the profile-local Desktop mailbox.

The broker carries controls and presentation, never an agent or source snapshot.
Native routing, admission, busy queues, dialogs and Discord delivery stay here.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
from concurrent.futures import TimeoutError as FutureTimeout
from pathlib import Path
import secrets

from gateway.config import Platform
from gateway.platforms.event import MessageEvent

GATEWAY_KEY = '__hermes_legacy_gateway_discovery__'
logger = logging.getLogger(__name__)


class LegacyGatewayOwner:
    def __init__(self, runner, entry, source, hub):
        self.runner, self.route, self.source, self.hub = runner, entry.session_key, source, hub
        self.key, self.sid = entry.session_id, 'gateway:' + secrets.token_hex(16)
        self.loop = asyncio.get_running_loop()
        self.text = ''
        self.generation = 0
        self.input_row = None
        self.origin = None

    def __call__(self, request):
        if self.loop.is_closed() or not self.loop.is_running():
            raise RuntimeError('Legacy gateway loop is unavailable; no replacement run was started')
        future = asyncio.run_coroutine_threadsafe(self.execute(request), self.loop)
        try:
            return future.result(25)
        except FutureTimeout:
            # Cancellation is not replay permission: a prompt already accepted by
            # the native adapter belongs to that adapter, not this control RPC.
            future.cancel()
            raise RuntimeError('Legacy gateway did not acknowledge; do not resend automatically') from None

    def entry(self):
        entry = self.runner.session_store.lookup_by_session_key(self.route)
        owner = self.hub.lookup(self.key)
        if not entry or not owner or owner['sid'] != self.sid or not owner['online']:
            raise RuntimeError('Legacy route or execution generation is no longer current')
        if self.hub.lookup(entry.session_id) != owner:
            raise RuntimeError('Legacy route changed outside the verified compression lineage')
        return entry

    def frame(self, kind, payload, *, identity=None):
        from tui_gateway.contracts.registry import check_payload
        check_payload(kind, payload)
        try:
            self.hub.publish(self.key, {'jsonrpc': '2.0', 'method': 'event', 'params': {
                'session_id': self.sid, 'type': kind, 'payload': payload}}, identity=identity)
        except (RuntimeError, sqlite3.Error) as exc:
            # An observer failure never converts a finished native turn into a
            # failed/retryable task; SQLite history remains the recovery source.
            logger.warning('Incumbent bridge presentation unavailable: %s', exc)

    def input_accepted(self):
        if self.input_row is not None:
            return
        entry = self.entry()
        db = self.runner.session_store._db_for_key(self.route)
        history = db.get_messages_as_conversation(entry.session_id, include_row_ids=True)
        user = next((m for m in reversed(history) if m.get('role') == 'user'), {})
        row_id = user.get('_row_id')
        if row_id is None or user.get('display_kind') == 'hidden':
            return
        self.input_row = row_id
        text = user.get('content')
        if isinstance(text, str):
            self.frame('session.clients.input', {'text': text, 'row_id': row_id, 'origin': self.origin},
                       identity=f'input:{entry.session_id}:{row_id}')

    def delta(self, original):
        generation = self.generation
        def publish(*args, **kwargs):
            if original:
                original(*args, **kwargs)
            if not self.runner._is_session_run_current(self.route, generation):
                return
            self.input_accepted()
            delta = args[0] if args else kwargs.get('delta', '')
            if isinstance(delta, str):
                self.text += delta
                self.frame('message.delta', {'text': delta})
        return publish

    def tool_start(self, original):
        generation = self.generation
        def publish(tool_id, name, args):
            if original:
                original(tool_id, name, args)
            if self.runner._is_session_run_current(self.route, generation):
                self.input_accepted()
                # Tool arguments and dialog payloads are not durable bridge data.
                self.frame('tool.start', {'tool_id': str(tool_id), 'name': str(name)})
        return publish

    async def snapshot(self, params):
        from tui_gateway import server
        entry = self.entry()
        db = self.runner.session_store._db_for_key(self.route)
        canonical = await asyncio.to_thread(db.get_compression_tip, entry.session_id) or entry.session_id
        await asyncio.to_thread(db.assert_resume_safe, canonical)
        history = await asyncio.to_thread(db.get_messages_as_conversation, canonical,
                                         include_ancestors=True, include_compacted=True, include_row_ids=True)
        messages = server._history_to_messages(history, profile_home=self.hub.home)
        agent = self.runner._cached_agent_for(self.route)
        running = self.runner._is_session_running(self.route)
        result = {'session_id': self.sid, 'stored_session_id': canonical,
                  'session_key': canonical, 'resumed': canonical,
                  'message_count': len(messages), 'messages': [] if params.get('omit_messages') else messages,
                  'messages_omitted': bool(params.get('omit_messages')), 'running': running,
                  'status': 'working' if running else 'idle', 'started_at': entry.created_at.timestamp(),
                  'inflight': {'assistant': self.text, 'streaming': True, 'user': ''} if running else None,
                  'info': {'model': getattr(agent, 'model', '') or '',
                                           'tools': {}, 'skills': {}, 'cwd': getattr(agent, 'cwd', None) or str(self.hub.home)}}
        return result

    async def events_since(self, params):
        rows = await asyncio.to_thread(self.hub.events, self.key, int(params.get('last_seen') or 0))
        head = await asyncio.to_thread(self.hub.event_head, self.key)
        return {'events': [row['frame']['params'] for row in rows], 'latest_seq': head,
                'truncated': bool(rows and rows[-1]['seq'] < head), 'count': len(rows),
                'epoch': self.sid, 'open_requests': []}

    async def execute(self, request):
        from gateway.run import _profile_runtime_scope
        from tui_gateway import server
        with _profile_runtime_scope(self.hub.home):
            return await self._execute_scoped(request, server)

    async def _execute_scoped(self, request, server):
        try:
            validate_request(request['method'], request['params'])
        except ValueError as exc:
            return server._err(request['id'], 4092, str(exc))
        entry = self.entry()
        source = self.runner._get_cached_session_source(self.route) or self.runner._restored_source(entry) or self.source
        source_home = await asyncio.to_thread(Path(self.runner._resolve_profile_home_for_source(source)).resolve) if source else None
        if not source or source_home != self.hub.home:
            return server._err(request['id'], 4092, 'Legacy source belongs to another profile')
        # Desktop transport authentication precedes this mailbox. Preserve the
        # original receiving bot's current admission policy, not the first viewer's identity.
        if not self.runner._is_user_authorized_for_source(source):
            return server._err(request['id'], 4092, 'Legacy route is no longer authorized')
        method, params = request['method'], request['params']
        readers = {'session.resume': self.snapshot, 'session.activate': self.snapshot,
                   'session.events.since': self.events_since}
        if reader := readers.get(method):
            return server._ok(request['id'], await reader(params))
        if method == 'session.interrupt':
            denial = self.runner._check_slash_access(source, 'stop')
            if denial is not None:
                return server._err(request['id'], 4092, str(denial))
            if not self.runner._is_session_running(self.route):
                return server._err(request['id'], 4092, 'Legacy route has no running turn to interrupt')
            generation = self.runner._current_session_run_generation(self.route)
            await self.runner._handle_message(MessageEvent(text='/stop', source=source))
            # The native stop invalidates the original run. A denial, hook or
            # refused admission must not fabricate a terminal presentation.
            if self.runner._is_session_run_current(self.route, generation):
                return server._err(request['id'], 4092, 'Legacy turn did not accept stop')
            self.frame('message.complete', {'text': '', 'status': 'interrupted'},
                       identity=f'interrupt:{self.sid}:{generation}')
            self.text = ''
            return server._ok(request['id'], {'status': 'interrupted'})
        if method == 'session.steer':
            agent = self.runner._running_agents.get(self.route)
            if not self.runner._is_session_running(self.route) or not hasattr(agent, 'steer'):
                return server._err(request['id'], 4092, 'Legacy turn is not steerable')
            event = MessageEvent(text=params['text'], source=source, allow_gateway_control=False)
            outcome = await self.runner._resolve_busy_steer_or_redirect(event, self.route, 'steer', agent)
            if not outcome.steered:
                return server._err(request['id'], 4092, 'Legacy turn did not accept steering')
            self.frame('session.clients.input', {'text': params['text'], 'row_id': None,
                                               'origin': request.get('_client_origin')})
            return server._ok(request['id'], {'status': 'steered'})
        if method == 'prompt.submit':
            if self.runner._draining or self.runner._executor_closing:
                return server._err(request['id'], 4092, 'Legacy gateway is draining; no replacement run was started')
            adapter = self.runner._delivery_adapter_for(source)
            if adapter is None or not getattr(adapter, '_message_handler', None):
                return server._err(request['id'], 4092, 'Legacy delivery adapter is unavailable')
            busy = self.runner._is_session_running(self.route)
            text = params['text']
            event = MessageEvent(text=text, source=source, allow_gateway_control=False,
                                 message_id='desktop-' + request['id'],
                                 metadata={'_session_client_origin': request.get('_client_origin') or 'desktop'})
            notice = self.runner._hm_estop_gate(event, source, False)
            if notice is not None:
                return server._err(request['id'], 4092, str(notice))
            if busy:
                # /queue reconstructs an event with control authority. Queue the
                # original conversational event through the same native FIFO,
                # without yielding between the busy check and its receipt.
                if self.runner._admit_bot_message_for_source(source):
                    event._bot_loop_admitted = True
                    self.runner._enqueue_fifo(self.route, event, adapter)
            else:
                await adapter.handle_message(event)
            if not getattr(event, '_gateway_accepted', False):
                return server._err(request['id'], 4092, 'Legacy adapter refused admission; no replacement run was started')
            return server._ok(request['id'], {'status': 'queued' if busy else 'streaming'})
        return server._err(request['id'], 4092, 'This control is not supported by the incumbent gateway bridge')


def validate_request(method, params):
    """Reject unsupported native semantics BEFORE their payload reaches SQLite."""
    allowed = {
        'session.active_list': {'profile', 'current_session_id'},
        'session.resume': {'session_id', 'profile', 'omit_messages', 'eager_build', 'lazy'},
        'session.activate': {'session_id', 'profile', 'omit_messages'},
        'session.events.since': {'session_id', 'profile', 'last_seen'},
        'session.interrupt': {'session_id', 'profile'},
        'session.steer': {'session_id', 'profile', 'text'},
        'prompt.submit': {'session_id', 'profile', 'text'},
    }
    if not isinstance(params, dict) or method not in allowed or set(params) - allowed[method]:
        raise ValueError('This method or payload is not supported by the incumbent gateway bridge')
    types = {'session_id': str, 'profile': (str, type(None)), 'current_session_id': (str, type(None)),
             'text': str, 'omit_messages': bool, 'eager_build': bool, 'lazy': bool, 'last_seen': int}
    if any(not isinstance(value, types[key]) for key, value in params.items()):
        raise ValueError('Incumbent gateway controls require scalar native arguments')
    if method not in {'prompt.submit', 'session.steer'}:
        return
    if not isinstance(params.get('text'), str) or not params['text'].strip():
        raise ValueError('The incumbent gateway bridge accepts non-empty plain text only')
    # Use the native parser, including leading Desktop attachment references.
    # Control RPCs are explicit; prompt text must never smuggle a slash operation.
    if MessageEvent(text=params['text']).is_command():
        raise ValueError('Gateway commands are not supported as incumbent conversational text')


def _control_lineage(db, session_id):
    """Authority uses verified compression edges, not the capped display history."""
    current = db.get_session(session_id)
    if not current:
        return [session_id]
    seen = {session_id}
    while db._is_compression_child_row(current):
        parent = db.get_session(current['parent_session_id'])
        if not parent or parent['id'] in seen:
            raise RuntimeError('Legacy compression ancestry is incomplete or cyclic')
        seen.add(parent['id'])
        current = parent
    # The native walker chooses the genuine continuation, excluding branch,
    # reset, delegate and stale sibling rows; it supports up to 1000 hops.
    lineage = db.get_compression_chain(current['id'])
    if session_id not in lineage or db.get_compression_tip(session_id) != lineage[-1]:
        raise RuntimeError('Legacy compression lineage does not converge on the native tip')
    previous = None
    verified = set()
    for key in lineage:
        child = db.get_session(key)
        if key in verified or not child:
            raise RuntimeError('Legacy compression lineage is incomplete or cyclic')
        if previous and (child.get('parent_session_id') != previous or not db._is_compression_child_row(child)):
            raise RuntimeError('Legacy compression lineage crosses a conversation boundary')
        verified.add(key)
        previous = key
    return lineage


def register_entry(runner, entry, source):
    """Called on the native gateway loop, before agent construction or history repair."""
    from tui_gateway.session_clients import enabled, get_hub
    home = runner._resolve_profile_home_for_source(source)
    if source.platform != Platform.DISCORD or not enabled(home):
        return None
    owners = getattr(runner, '_legacy_client_owners', None)
    if owners is None:
        owners = runner._legacy_client_owners = {}
    owner = owners.get(entry.session_key)
    hub = get_hub(home)
    ensure_discovery(runner, hub)
    db = runner.session_store._db_for_key(entry.session_key)
    lineage = _control_lineage(db, entry.session_id) if db else [entry.session_id]
    previous_owner = owner
    if owner is not None and owner.key not in lineage:
        # A native /new or explicit fork is a new conversation, not compression.
        owner = None
    created = owner is None
    if created:
        owner = LegacyGatewayOwner(runner, entry, source, hub)
    # Alias/owner registration is atomic. Failure must leave the incumbent's
    # evidence online, not retire it before a replacement can be established.
    hub.register_external(owner.key, owner.sid, owner, aliases=lineage)
    if previous_owner is not None and previous_owner is not owner:
        previous_owner.hub.retire(previous_owner.key, previous_owner.sid)
    hub.start()
    owners[entry.session_key] = owner
    if created:
        from gateway.run import _profile_runtime_scope
        with _profile_runtime_scope(hub.home):
            observe_active_agent(owner)
    return owner


def observe_active_agent(owner):
    """Add presentation sinks to a held incumbent; never rebuild its agent/prefix."""
    from gateway.warning_notifications import diagnostic_wake_muted
    runner = owner.runner
    if not runner._is_session_running(owner.route):
        return
    agent = runner._cached_agent_for(owner.route)
    state = runner._peek_session_state(owner.route)
    if agent is None or state is None or state.turn.agent is not agent or state.turn.event is None:
        return
    if diagnostic_wake_muted(state.turn.event):
        return
    owner.generation = runner._current_session_run_generation(owner.route)
    owner.origin = state.turn.event.metadata.get('_session_client_origin') or f'discord:{owner.source.chat_id}'
    owner.text = getattr(agent, '_current_streamed_assistant_text', '') or ''
    agent.stream_delta_callback = owner.delta(agent.stream_delta_callback)
    agent.tool_start_callback = owner.tool_start(agent.tool_start_callback)


def retained_agent_ids(runner):
    """Cache-sweeper protection; callers hold the native cache lock.

    Explicit native conversation boundaries still own teardown. Attaching a view
    cannot itself turn an original cached agent into a cold replacement.
    """
    retained = set()
    cache = getattr(runner, '_agent_cache', None) or {}
    for route, owner in (getattr(runner, '_legacy_client_owners', None) or {}).items():
        entry = cache.get(route)
        if not entry:
            continue
        try:
            protect = owner.hub.has_viewers(owner.key)
        except sqlite3.Error as exc:
            logger.warning('Retaining incumbent cache: viewer lease unavailable: %s', exc)
            protect = True
        if protect:
            retained.add(id(entry[0]))
    return retained


def retire_gateway_bridge(runner):
    """Close intake; retain accepted native work until its original owner settles."""
    from gateway.session_client_admission import retire_internal_admission

    def retire_owners():
        for owner in (getattr(runner, '_legacy_client_owners', None) or {}).values():
            owner.hub.retire(owner.key, owner.sid)
        for consumer in (getattr(runner, '_legacy_client_discovery', None) or {}).values():
            consumer.hub.retire(GATEWAY_KEY, '')

    return retire_internal_admission(runner, retire_owners)


def owner_for(runner, route):
    return (getattr(runner, '_legacy_client_owners', None) or {}).get(route)


class GatewayDiscovery:
    """Resolve exact native routes on the incumbent loop, without cold resume."""
    __call__ = LegacyGatewayOwner.__call__

    def __init__(self, runner, hub):
        self.runner, self.hub = runner, hub
        self.loop = asyncio.get_running_loop()

    async def active_list(self, request):
        from tui_gateway import server
        rows = []
        for owner in (getattr(self.runner, '_legacy_client_owners', None) or {}).values():
            if owner.hub.home != self.hub.home:
                continue
            if not (self.runner._is_session_running(owner.route) or self.hub.has_viewers(owner.key)):
                continue
            entry = owner.entry()
            source = self.runner._get_cached_session_source(owner.route) or self.runner._restored_source(entry)
            if source is None or not self.runner._is_user_authorized_for_source(source):
                continue
            snapshot = await owner.snapshot({'omit_messages': True})
            rows.append({'id': owner.sid, 'session_key': snapshot['stored_session_id'],
                         'current': owner.sid == request['params'].get('current_session_id'),
                         'last_active': entry.updated_at.timestamp(), 'started_at': entry.created_at.timestamp(),
                         'message_count': snapshot['message_count'], 'model': snapshot['info']['model'],
                         'preview': '', 'title': entry.display_name or '', 'status': snapshot['status']})
        return server._ok(request['id'], {'sessions': rows})

    async def execute(self, request):
        from gateway.run import _profile_runtime_scope
        from tui_gateway import server
        if request['method'] == 'session.active_list':
            with _profile_runtime_scope(self.hub.home):
                return await self.active_list(request)
        if request['method'] != 'session.resume':
            return server._err(request['id'], 4092, 'Not a legacy discovery operation')
        target = str(request['params'].get('session_id') or '')
        with _profile_runtime_scope(self.hub.home):
            entries = await asyncio.to_thread(self.runner.session_store.list_sessions)
            for entry in entries:
                source = self.runner._get_cached_session_source(entry.session_key) or self.runner._restored_source(entry)
                if source is None or source.platform != Platform.DISCORD:
                    continue
                source_home = await asyncio.to_thread(Path(self.runner._resolve_profile_home_for_source(source)).resolve)
                if source_home != self.hub.home:
                    continue
                db = self.runner.session_store._db_for_key(entry.session_key)
                if target not in _control_lineage(db, entry.session_id):
                    continue
                register_entry(self.runner, entry, source)
                return server._ok(request['id'], {'legacy_owner': True})
        return server._ok(request['id'], {'legacy_owner': False})


def ensure_discovery(runner, hub):
    services = getattr(runner, '_legacy_client_discovery', None)
    if services is None:
        services = runner._legacy_client_discovery = {}
    home = str(hub.home)
    if home not in services:
        consumer = GatewayDiscovery(runner, hub)
        hub.register_service(GATEWAY_KEY, '', consumer)
        hub.start()
        services[home] = consumer


async def start_gateway_bridge(runner):
    """Reserve existing native routes before platform adapters can admit traffic."""
    from gateway.run import _multiplex_profile_homes
    from hermes_constants import get_hermes_home
    from tui_gateway.session_clients import enabled, get_hub
    homes = (_multiplex_profile_homes(runner.config) if runner.config.multiplex_profiles
             else [(runner._primary_profile_name, get_hermes_home())])
    for _profile, home in homes:
        home = await asyncio.to_thread(Path(home).resolve)
        if enabled(home):
            ensure_discovery(runner, get_hub(home))
    entries = await asyncio.to_thread(runner.session_store.list_sessions)
    for entry in entries:
        source = runner._get_cached_session_source(entry.session_key) or runner._restored_source(entry)
        if source is not None:
            try:
                register_entry(runner, entry, source)
            except (RuntimeError, sqlite3.Error) as exc:
                # Conversation admission evidence is not replay permission.
                # Fence this route, not every adapter in the gateway process.
                logger.warning('Incumbent route %s remains fenced; no replacement run was started: %s',
                               entry.session_key, exc)


async def refresh_gateway_bridge(runner, home):
    from tui_gateway.session_clients import enabled, get_hub
    if not await asyncio.to_thread(enabled, home):
        return
    hub = await asyncio.to_thread(get_hub, home)
    if str(hub.home) not in (getattr(runner, '_legacy_client_discovery', None) or {}):
        await start_gateway_bridge(runner)


def wire_callbacks(runner, ctx, agent):
    owner = owner_for(runner, ctx.session_key)
    if owner is not None and not ctx.mute_notification_reply:
        owner.text = ''
        owner.input_row = None
        owner.generation = ctx.run_generation
        event = runner._peek_session_state(ctx.session_key).turn.event
        owner.origin = event.metadata.get('_session_client_origin') or f'discord:{ctx.source.chat_id}'
        owner.frame('message.start', {})
        agent.stream_delta_callback = owner.delta(agent.stream_delta_callback)
        agent.tool_start_callback = owner.tool_start(agent.tool_start_callback)


def completed(runner, entry, route, response, agent_result):
    owner = owner_for(runner, route)
    if owner is None:
        return
    try:
        current = register_entry(runner, entry, owner.source)
    except (RuntimeError, sqlite3.Error) as exc:
        logger.warning('Incumbent completion remains native; bridge ownership unavailable: %s', exc)
        return
    if current is not owner:
        return
    db = runner.session_store._db_for_key(route)
    history = db.get_messages_as_conversation(entry.session_id, include_row_ids=True)
    final = next((m for m in reversed(history) if m.get('role') == 'assistant'), {})
    final_id = final.get('_row_id')
    owner.frame('message.complete', {'text': response or '',
                                    'status': 'interrupted' if agent_result.get('interrupted') else 'complete',
                                    'persisted_turn': {'row_ids': [final_id] if final_id else [], 'complete': False,
                                                       'final_assistant_row_id': final_id}},
                identity=f'complete:{entry.session_id}:{final_id}' if final_id else None)
    owner.text = ''

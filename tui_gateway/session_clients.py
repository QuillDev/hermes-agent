"""Profile-local, cross-process attachment to ONE existing execution owner.

SQLite is a durable mailbox, not a second transcript or an agent scheduler.
Opt-in sessions execute in a detached host; surfaces only forward controls.
Ownership is generation-fenced, and a dead owner fails closed.
Only explicit invitations publish a private session to an external client.
"""
from __future__ import annotations

import contextlib
import contextvars
import hashlib
import json
import logging
import os
from pathlib import Path
import secrets
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import psutil
import hermes_yaml as yaml

log = logging.getLogger(__name__)
_bypass = contextvars.ContextVar('session_clients_local_dispatch', default=False)
_origin = contextvars.ContextVar('session_clients_origin', default=None)
_hubs: dict[str, SessionClientHub] = {}
_hubs_lock = threading.RLock()
_settings: dict[str, tuple[tuple[int, int, int], bool]] = {}
_REMOTE_METHODS = frozenset({
    'session.create', 'session.branch_stored', 'session.resume', 'session.activate', 'session.info', 'session.events.since',
    'session.active_list',
    'session.steer', 'session.interrupt', 'session.redirect', 'session.clients.share',
    'prompt.submit', 'image.attach', 'image.attach_bytes', 'image.detach',
    'file.attach', 'pdf.attach', 'subagent.list', 'subagent.steer',
    'subagent.interrupt', 'subagent.tail', 'session.control.read', 'session.control',
    'config.set', 'config.get', 'model.options', 'session.title', 'session.cwd.set', 'session.branch', 'session.branch_whole',
    'session.status', 'session.history', 'session.usage', 'session.context_breakdown',
    'session.undo', 'session.compress', 'session.save', 'session.archive', 'session.set_hidden',
    'session.events.stats', 'terminal.resize', 'slash.exec', 'command.dispatch',
    'prompt.background', 'prompt.btw', 'complete.path', 'message.react',
})
_SESSION_CONFIG_KEYS = frozenset({
    'model', 'fast', 'yolo', 'reasoning', 'cwd', 'terminal.cwd', 'workdir',
    'prompt', 'personality', 'verbose', 'focus',
})

# Profile/config getters can contain expanded credentials. Only live session
# control projections belong in the durable mailbox.
_SESSION_CONFIG_GETTERS = frozenset({'reasoning', 'fast'})


def _broker_getter_allowed(method: str, params: dict) -> bool:
    return method != 'config.get' or params.get('key') in _SESSION_CONFIG_GETTERS


def enabled(home: Path | str) -> bool:
    path = Path(home).resolve() / 'config.yaml'
    try:
        stat = path.stat()
        signature = (stat.st_ino, stat.st_mtime_ns, stat.st_size)
        cached = _settings.get(str(path))
        if cached and cached[0] == signature:
            return cached[1]
        cfg = yaml.safe_load(path.read_text()) or {}
        value = cfg.get('session_clients', {}).get('enabled') is True
        _settings[str(path)] = (signature, value)
        return value
    except (OSError, ValueError, yaml.YAMLError, AttributeError):
        return False


def get_hub(home: Path | str, *, execution_host: bool = False, start: bool = True) -> SessionClientHub:
    key = str(Path(home).resolve())
    with _hubs_lock:
        hub = _hubs.get(key)
        if hub is None or hub._stop.is_set():
            hub = _hubs[key] = SessionClientHub(key, execution_host=execution_host, start=start)
        if execution_host and not hub.execution_host:
            raise RuntimeError('A viewing client cannot become an execution host')
        return hub


def _owner_alive(row: dict) -> bool:
    from tui_gateway.session_client_host import HOST_KEY
    if not row['online'] and row['session_key'] != HOST_KEY:
        return False
    try:
        process = psutil.Process(row['pid'])
        # A stopping host remains fenced until it really exits, even after it
        # closes its mailbox. Zombies cannot write and must not block recovery.
        return process.create_time() == row['born'] and process.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False
    except psutil.AccessDenied:
        return True  # Unknown liveness is not permission to start a second writer.


def _broker_response(response: dict) -> dict:
    """Dialog replay is socket-local, never durable shared-client state."""
    result = response.get('result')
    if isinstance(result, dict):
        result = dict(result)
        result.pop('open_requests', None)
        result.pop('pending_approval', None)
        result.pop('pending_connection', None)
        # events.since has a required (possibly empty) list on the native wire.
        if 'open_requests' in response['result']:
            result['open_requests'] = []
        response = {**response, 'result': result}
    return response


class MailboxTransport:
    """A persistent attached client: socket disconnection never ends the run."""
    closed = False
    supports_server_requests = False

    def __init__(self, hub: SessionClientHub, key: str, auth_identity=None):
        self.hub, self.key, self.auth_identity = hub, key, auth_identity
        # Native resume captures this transport BEFORE scheduling recovery;
        # setting shared_clients only after admission is too late.
        self.suppress_interrupted_auto_continue = hub.execution_host
        self.closed = False

    def write(self, frame: dict) -> bool:
        # Hosted dialogs cannot be answered. Never acknowledge a dropped question
        # or put secret response frames in this on-disk event log.
        if self.closed:
            return False
        if self.key and frame.get('method') == 'event':
            self.hub.publish(self.key, frame)
        if frame.get('method') != 'event':
            return False
        return True

    def close(self):
        self.closed = True


class SessionClientHub:
    def __init__(self, home: Path | str, *, start: bool = True, execution_host: bool = False):
        self.home = Path(home).resolve()
        directory = self.home / 'session-clients'
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.path = directory / 'broker.sqlite'
        self.execution_host = execution_host
        self.owner = ('host:' if execution_host else '') + secrets.token_hex(16)
        self.pid = os.getpid()
        self.born = psutil.Process(self.pid).create_time()
        self._stop = threading.Event()
        self._subscribers: dict[str, dict] = {}
        self._viewer_refresh_at: dict[str, float] = {}
        self._subscriptions_lock = threading.RLock()
        self._mailboxes: dict[str, MailboxTransport] = {}
        # Native create/resume can build before registration. Serialize admissions
        # for this profile through adoption, not just the final ownership claim.
        self._admission_lock = threading.RLock()
        self._pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix='session-client-rpc')
        with self._db() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS owners (
                    session_key TEXT PRIMARY KEY, owner TEXT NOT NULL, sid TEXT NOT NULL,
                    pid INTEGER NOT NULL, born REAL NOT NULL, online INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS aliases (alias TEXT PRIMARY KEY, session_key TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS invites (
                    digest TEXT PRIMARY KEY, session_key TEXT NOT NULL, expires_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS clients (
                    platform TEXT NOT NULL, chat_id TEXT NOT NULL, user_id TEXT NOT NULL,
                    session_key TEXT NOT NULL, PRIMARY KEY(platform,chat_id));
                CREATE TABLE IF NOT EXISTS events (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, session_key TEXT NOT NULL, frame TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS event_session ON events(session_key,seq);
                CREATE TABLE IF NOT EXISTS commands (
                    id TEXT PRIMARY KEY, owner TEXT NOT NULL, session_key TEXT NOT NULL,
                    request TEXT NOT NULL, deadline REAL NOT NULL, state TEXT NOT NULL,
                    response TEXT);
                CREATE TABLE IF NOT EXISTS cursors (client TEXT PRIMARY KEY, seq INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS viewers (
                    session_key TEXT NOT NULL, client TEXT NOT NULL, pid INTEGER NOT NULL,
                    born REAL NOT NULL, expires REAL NOT NULL, PRIMARY KEY(session_key,client));
            ''')
        self.path.chmod(0o600)
        self._thread = None
        if execution_host:
            from tui_gateway.session_client_host import HOST_KEY
            self.claim(HOST_KEY, '')
        if start:
            self.start()

    def start(self):
        from agent.memory_provider import spawn_context_thread
        if self._thread is None:
            self._thread = spawn_context_thread(self._pump, name='session-clients')
            self._thread.start()

    @contextlib.contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    def lookup(self, key: str) -> dict | None:
        with self._db() as db:
            row = db.execute('SELECT * FROM owners WHERE session_key=COALESCE('
                             '(SELECT session_key FROM aliases WHERE alias=?),?)', (key, key)).fetchone()
            return dict(row) if row else None

    def claim(self, key: str, sid: str) -> dict:
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM owners WHERE session_key=COALESCE('
                             '(SELECT session_key FROM aliases WHERE alias=?),?)', (key, key)).fetchone()
            if row and _owner_alive(dict(row)) and (row['owner'] != self.owner or row['sid'] != sid):
                raise RuntimeError('This session already has a live execution owner')
            key = row['session_key'] if row else key
            db.execute('INSERT OR REPLACE INTO owners VALUES (?,?,?,?,?,1)',
                       (key, self.owner, sid, self.pid, self.born))
        return self.lookup(key)

    def _pin(self, key: str, sid: str):
        from tui_gateway import server
        session = server._sessions.get(sid)
        if session is None:
            raise ValueError('Session is no longer live')
        home = Path(session.get('profile_home') or server._hermes_home).resolve()
        if home != self.home:
            raise ValueError('Session belongs to another profile')
        with server._session_resume_lock:
            key = (self.lookup(key) or {}).get('session_key', key)
            self.claim(key, sid)
            mailbox = self._mailboxes.setdefault(key, MailboxTransport(self, key, session.get('_shared_auth_identity')))
            server._attach_session_transport(session, mailbox)
            session['shared_clients'] = True
            if self.execution_host:
                session['_hosted_no_auto_continue'] = True
                session['_broker_server_requests_unsupported'] = True
                from tui_gateway import server_requests
                server_requests.cancel(sid, reason='unsupported hosted dialog')
            session['shared_session_key'] = key
            session['_sid'] = sid
            session['close_on_disconnect'] = False
            server._cancel_ws_orphan_reap(sid)

    def share(self, sid: str) -> dict:
        from tui_gateway import server
        session = server._sessions.get(sid)
        if session is None:
            raise ValueError('Session is no longer live')
        key = str(session.get('shared_session_key') or session['session_key'])
        self._pin(key, sid)
        return self.invite(key)

    def invite(self, key: str) -> dict:
        row = self.lookup(key)
        if not row:
            raise ValueError('Session is not shared')
        key = row['session_key']
        token = secrets.token_urlsafe(32)
        expires = time.time() + 600
        with self._db() as db:
            db.execute('DELETE FROM invites WHERE expires_at<?', (time.time(),))
            db.execute('INSERT INTO invites VALUES (?,?,?)',
                       (hashlib.sha256(token.encode()).hexdigest(), key, expires))
        return {'token': token, 'expires_at': expires}

    def attach(self, token: str, platform: str, chat_id: str, user_id: str) -> dict:
        digest = hashlib.sha256(token.encode()).hexdigest()
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM invites WHERE digest=? AND expires_at>?',
                             (digest, time.time())).fetchone()
            if row is None:
                raise ValueError('Invitation is invalid, expired, used, or belongs to another profile')
            owner = db.execute('SELECT * FROM owners WHERE session_key=?', (row['session_key'],)).fetchone()
            if owner is None or not _owner_alive(dict(owner)):
                raise ValueError('Execution owner is unavailable; explicitly reopen the stored session')
            if db.execute('SELECT 1 FROM clients WHERE platform=? AND chat_id=?', (platform, chat_id)).fetchone():
                raise ValueError('This client is already attached; create a fresh thread')
            db.execute('INSERT INTO clients VALUES (?,?,?,?)', (platform, chat_id, str(user_id), row['session_key']))
            db.execute('DELETE FROM invites WHERE digest=?', (digest,))
            return dict(owner)

    def bind_client(self, key: str, platform: str, chat_id: str, user_id: str):
        row = self.lookup(key)
        if row is None or row['owner'] != self.owner:
            raise ValueError('Only the creator may bind a newly shared client')
        with self._db() as db:
            db.execute('INSERT INTO clients VALUES (?,?,?,?)', (platform, str(chat_id), str(user_id), key))

    def link(self, platform: str, chat_id: str) -> dict | None:
        with self._db() as db:
            row = db.execute('SELECT clients.*, owners.sid, owners.owner FROM clients JOIN owners USING(session_key) '
                             'WHERE platform=? AND chat_id=?', (platform, str(chat_id))).fetchone()
            return dict(row) if row else None

    def links(self, platform: str) -> list[dict]:
        with self._db() as db:
            return [dict(r) for r in db.execute('SELECT * FROM clients WHERE platform=?', (platform,))]

    def create(self) -> dict:
        result = self._create_reply('session.create', {})
        if 'error' in result:
            raise RuntimeError(result['error']['message'])
        return self.lookup(result['result']['stored_session_id'])

    def _create_reply(self, method: str, params: dict, auth_identity=None, viewer=None) -> dict:
        with self._admission_lock:
            return self._admit_reply(method, params, auth_identity, viewer)

    def _adopt_reply(self, result: dict, mailbox: MailboxTransport, viewer=None) -> dict:
        from tui_gateway import server
        if 'error' in result:
            return result
        sid = result.get('result', {}).get('session_id')
        if not sid or sid not in server._sessions:
            return result
        session = server._sessions[sid]
        session.setdefault('_shared_auth_identity', mailbox.auth_identity)
        key = (self.lookup(session['session_key']) or {}).get('session_key', session['session_key'])
        try:
            self.claim(key, sid)
        except RuntimeError:
            # A foreign generation can win the fence. Never leave a constructed
            # losing actor behind; native automatic teardown keeps stored rows.
            server._close_session_by_id(sid, end_reason='idle_timeout')
            mailbox.close()
            raise
        canonical = self._mailboxes.setdefault(key, mailbox)
        if canonical is not mailbox:
            # Native warm resume attached this admission's temporary transport.
            # Remove it before retaining the one and only publishing producer.
            server._detach_session_transport(session, mailbox)
            mailbox.close()
        canonical.key = key
        result['result']['stored_session_id'] = key
        self._retain_request_viewer(result, {'_client_viewer': viewer})
        self._pin(key, sid)
        return result

    def _admit_reply(self, method: str, params: dict, auth_identity=None, viewer=None) -> dict:
        from tui_gateway import server
        from tui_gateway.transport import bind_transport, reset_transport
        # This new session has no socket owner. Its source remains Desktop-compatible.
        scope = {'profile_home': str(self.home)}
        token = _bypass.set(True)
        mailbox = MailboxTransport(self, '', auth_identity)
        transport_token = bind_transport(mailbox)
        try:
            profile = None if Path(server._hermes_home).resolve() == self.home else self.home.name
            resolved = server._profile_home(profile) or server._hermes_home
            if Path(resolved).resolve() != self.home:
                raise ValueError('Profile cannot be resolved by the execution host')
            with server._session_profile_runtime_scope(scope):
                # Passing the profile home on the record is essential: session.create's
                # launch-home default is not the ContextVar-resolved secondary profile.
                result = server.handle_request({'id': 'shared-create', 'method': method,
                                                'params': {'source': 'desktop', **params,
                                                           **({'profile': profile} if profile else {})}})
            # Keep native None = launch-profile semantics. Rewriting it to an
            # absolute path prevents native cold-resume reuse of this runtime.
            return self._adopt_reply(result, mailbox, viewer)
        finally:
            reset_transport(transport_token)
            _bypass.reset(token)

    def publish(self, key: str, frame: dict) -> int | None:
        if not enabled(self.home):
            return
        from tui_gateway import server
        with self._db() as db:
            owner = db.execute('SELECT owner,online,sid FROM owners WHERE session_key=?', (key,)).fetchone()
            if owner is None or owner['owner'] != self.owner or not owner['online']:
                raise RuntimeError('Only the current execution owner may publish')
            session = server._sessions.get(owner['sid']) or {}
            tip = str(session.get('session_key') or '')
            if tip and tip != key:
                db.execute('INSERT OR REPLACE INTO aliases VALUES (?,?)', (tip, key))
            row = db.execute('INSERT INTO events(session_key,frame) VALUES (?,?)',
                             (key, json.dumps(frame, ensure_ascii=False)))
            seq = row.lastrowid
            if seq % 256 == 0:
                db.execute('DELETE FROM events WHERE session_key=? AND seq NOT IN '
                           '(SELECT seq FROM events WHERE session_key=? ORDER BY seq DESC LIMIT 10000)', (key, key))
            return seq

    def events(self, key: str, after: int) -> list[dict]:
        with self._db() as db:
            rows = db.execute('SELECT seq,frame FROM events WHERE session_key=? AND seq>? ORDER BY seq LIMIT 256',
                              (key, after))
            return [{'seq': r['seq'], 'frame': json.loads(r['frame'])} for r in rows]

    def event_head(self, key: str) -> int:
        with self._db() as db:
            return db.execute('SELECT COALESCE(MAX(seq),0) FROM events WHERE session_key=?', (key,)).fetchone()[0]

    def cursor(self, client: str) -> int:
        with self._db() as db:
            row = db.execute('SELECT seq FROM cursors WHERE client=?', (client,)).fetchone()
            return row['seq'] if row else 0

    def advance(self, client: str, seq: int):
        with self._db() as db:
            db.execute('INSERT INTO cursors VALUES (?,?) ON CONFLICT(client) DO UPDATE SET seq=MAX(seq,excluded.seq)',
                       (client, seq))

    def call(self, key: str, method: str, params: dict, *, timeout: float = 30,
             origin: str | None = None, expected_owner: str | None = None) -> dict:
        if not _broker_getter_allowed(method, params):
            raise ValueError('Only an allowlisted session-specific getter may cross the broker')
        if method not in _REMOTE_METHODS:
            raise ValueError('This operation is not a shared-session control')
        row = self.lookup(key)
        if row is None or not _owner_alive(row):
            raise RuntimeError('Execution owner is unavailable; no replacement run was started')
        if expected_owner is not None and row['owner'] != expected_owner:
            raise RuntimeError('Execution owner changed; explicitly resume before retrying this operation')
        key = row['session_key']
        from tui_gateway.session_client_host import HOST_KEY
        from tui_gateway.transport import current_transport
        identity = getattr(current_transport(), 'auth_identity', None)
        identity = {k: identity.get(k) for k in ('provider', 'user_id')} if isinstance(identity, dict) else None
        request = {'jsonrpc': '2.0', 'id': secrets.token_hex(16), 'method': method,
                   'params': {**params, **({'session_id': row['sid']} if key != HOST_KEY else {})},
                   '_client_origin': origin,
                   '_client_auth_identity': identity}
        if current_transport() is not None:
            request['_client_viewer'] = {'client': self.owner, 'pid': self.pid, 'born': self.born}
        request['params'].pop('profile', None)
        if row['owner'] == self.owner:
            return self._execute(key, request)
        deadline = time.time() + timeout
        with self._db() as db:
            db.execute('INSERT INTO commands VALUES (?,?,?,?,?,?,NULL)',
                       (request['id'], row['owner'], key, json.dumps(request), deadline, 'pending'))
        while time.time() < deadline and not self._stop.wait(0.03):
            with self._db() as db:
                response = db.execute('SELECT response FROM commands WHERE id=?', (request['id'],)).fetchone()
                if response and response['response']:
                    db.execute('DELETE FROM commands WHERE id=?', (request['id'],))
                    return json.loads(response['response'])
            if not _owner_alive(row):
                break
        with self._db() as db:
            db.execute("UPDATE commands SET state='expired' WHERE id=? AND state='pending'", (request['id'],))
        raise RuntimeError('Owner did not acknowledge this operation; do not resend automatically')

    def _execute(self, key: str, request: dict) -> dict:
        if not _broker_getter_allowed(request['method'], request['params']):
            return {'id': request['id'], 'error': {'code': 4092,
                    'message': 'Only an allowlisted session-specific getter may cross the broker'}}
        return _broker_response(self._execute_native(key, request))

    def _execute_native(self, key: str, request: dict) -> dict:
        from tui_gateway import server
        from tui_gateway.transport import bind_transport, reset_transport
        row = self.lookup(key)
        from tui_gateway.session_client_host import HOST_KEY
        if key == HOST_KEY and self.execution_host and row and row['owner'] == self.owner and row['online']:
            if request['method'] == 'session.active_list':
                token = _bypass.set(True)
                try:
                    profile = None if Path(server._hermes_home).resolve() == self.home else self.home.name
                    with server._session_profile_runtime_scope({'profile_home': str(self.home)}):
                        return server.handle_request({
                            'id': request['id'], 'method': 'session.active_list',
                            'params': {'profile': profile, 'current_session_id': request['params'].get('current_session_id')}})
                finally:
                    _bypass.reset(token)
            if request['method'] not in {'session.create', 'session.branch_stored', 'session.resume'}:
                return server._err(request['id'], 4092, 'Not an execution-host admission operation')
            response = self._create_reply(request['method'], request['params'], request.get('_client_auth_identity'),
                                          request.get('_client_viewer'))
            response['id'] = request['id']
            return response
        session = server._sessions.get(row['sid']) if row and row['owner'] == self.owner else None
        if session is None or Path(session.get('profile_home') or server._hermes_home).resolve() != self.home:
            return {'id': request['id'], 'error': {'code': 4092, 'message': 'Shared session owner is no longer live'}}
        if request['params'].get('session_id') != row['sid']:
            return server._err(request['id'], 4092, 'This request belongs to a retired runtime')
        # Match native shared-session semantics: a second login can attach,
        # but it never replaces the creator identity used by the running agent.
        server._warn_foreign_login(session, MailboxTransport(self, key, request.get('_client_auth_identity')))
        if request['method'] == 'session.create':
            return server._err(request['id'], 4092, 'Session creation requires the execution host')
        if request['method'] == 'session.clients.share':
            # Private hosted drafts stay lazy. Explicit sharing makes the draft
            # discoverable, using native persistence for cwd/title/user metadata.
            with server._session_profile_runtime_scope(session):
                server._ensure_session_db_row(session)
            return {'id': request['id'], 'result': self.invite(key)}
        req = {**request, 'params': dict(request['params'])}
        req.pop('_client_auth_identity', None)
        req.pop('_client_viewer', None)
        origin_token = _origin.set(req.pop('_client_origin', None))
        if req['method'] == 'session.resume':
            req['method'] = 'session.activate'
            req['params'] = {'session_id': row['sid'], 'omit_messages': bool(req['params'].get('omit_messages'))}
        token = _bypass.set(True)
        branching = req['method'] in {'session.branch', 'session.branch_whole'}
        mailbox = (MailboxTransport(self, '', session.get('_shared_auth_identity')) if branching
                   else self._mailboxes.setdefault(key, MailboxTransport(self, key, session.get('_shared_auth_identity'))))
        transport_token = bind_transport(mailbox)
        try:
            with server._session_profile_runtime_scope(session):
                if branching:
                    # The child's native init captures current_transport(). Never
                    # give a private child the parent's Discord publishing sink.
                    with self._admission_lock:
                        return self._adopt_reply(server.handle_request(req), mailbox, request.get('_client_viewer'))
                return server.handle_request(req)
        finally:
            reset_transport(transport_token)
            _bypass.reset(token)
            _origin.reset(origin_token)

    def subscribe(self, key: str, transport, *, after: int | None = None):
        with self._subscriptions_lock:
            peers = self._subscribers.setdefault(key, {})
            peers[transport] = self.event_head(key) if after is None else after
            self._viewer_lease(key, True)

    def unsubscribe(self, key: str, transport):
        with self._subscriptions_lock:
            peers = self._subscribers.get(key, {})
            peers.pop(transport, None)
            if not peers:
                self._subscribers.pop(key, None)
                self._viewer_lease(key, False)

    def _viewer_lease(self, key: str, attached: bool):
        with self._db() as db:
            if attached:
                db.execute('INSERT OR REPLACE INTO viewers VALUES (?,?,?,?,?)',
                           (key, self.owner, self.pid, self.born, time.time() + 30))
                self._viewer_refresh_at[key] = time.monotonic() + 5
            else:
                db.execute('DELETE FROM viewers WHERE session_key=? AND client=?', (key, self.owner))
                self._viewer_refresh_at.pop(key, None)

    def _retain_request_viewer(self, response: dict, request: dict):
        viewer = request.get('_client_viewer')
        key = response.get('result', {}).get('stored_session_id')
        if viewer and key:
            # Reserve attachment before returning a new runtime. LRU may run
            # before the viewing backend receives the reply and subscribes.
            with self._db() as db:
                db.execute('INSERT OR REPLACE INTO viewers VALUES (?,?,?,?,?)',
                           (key, viewer['client'], viewer['pid'], viewer['born'], time.time() + 30))

    def has_viewers(self, key: str) -> bool:
        with self._db() as db:
            if db.execute('SELECT 1 FROM clients WHERE session_key=? LIMIT 1', (key,)).fetchone():
                return True
            rows = [dict(r) for r in db.execute('SELECT * FROM viewers WHERE session_key=? AND expires>?',
                                               (key, time.time()))]
        return any(_owner_alive({**row, 'online': 1}) for row in rows)

    def retire(self, key: str, sid: str):
        """Fence only after native teardown has closed the actual actor."""
        with self._admission_lock:
            with self._db() as db:
                retired = db.execute('UPDATE owners SET online=0 WHERE session_key=? AND owner=? AND sid=?',
                                     (key, self.owner, sid)).rowcount
            if retired and (mailbox := self._mailboxes.pop(key, None)):
                mailbox.close()

    def _deliver(self):
        if not enabled(self.home):
            return
        with self._subscriptions_lock:
            for key, peers in self._subscribers.items():
                for transport, after in tuple(peers.items()):
                    if getattr(transport, '_closed', False) is True:
                        peers.pop(transport, None)
                        continue
                    for event in self.events(key, after):
                        try:
                            if not transport.write(event['frame']):
                                peers.pop(transport, None)
                                break
                        except Exception:
                            log.debug('Shared client disconnected', exc_info=True)
                            peers.pop(transport, None)
                            break
                        peers[transport] = event['seq']
                if not peers or time.monotonic() >= self._viewer_refresh_at.get(key, 0):
                    self._viewer_lease(key, bool(peers))

    def _finish_command(self, row: dict):
        try:
            response = self._execute(row['session_key'], json.loads(row['request']))
        except Exception as exc:
            log.exception('Shared-session command failed')
            response = {'id': row['id'], 'error': {'code': -32000, 'message': str(exc)}}
        with self._db() as db:
            db.execute("UPDATE commands SET response=?, state='done' WHERE id=? AND owner=?",
                       (json.dumps(_broker_response(response)), row['id'], self.owner))

    def _pump(self):
        while not self._stop.wait(0.05):
            try:
                with self._db() as db:
                    db.execute('BEGIN IMMEDIATE')
                    db.execute('DELETE FROM commands WHERE deadline<? AND state!=?', (time.time() - 600, 'running'))
                    rows = [dict(r) for r in db.execute("SELECT * FROM commands WHERE owner=? AND state='pending' AND deadline>?",
                                                       (self.owner, time.time()))]
                    for row in rows:
                        db.execute("UPDATE commands SET state='running' WHERE id=?", (row['id'],))
                for row in rows:
                    self._pool.submit(self._finish_command, row)
                self._deliver()
            except Exception:
                log.exception('Shared-session mailbox poll failed')

    def close(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(2)
        self._pool.shutdown(wait=True)
        with self._db() as db:
            db.execute('UPDATE owners SET online=0 WHERE owner=?', (self.owner,))
            db.execute('DELETE FROM viewers WHERE client=?', (self.owner,))


# Runtime IDs are scoped to the owning profile; proxies never create an AIAgent.
_remote: dict[str, tuple[str, str, str]] = {}
_remote_lock = threading.RLock()


def _profile_config_request(home: Path | str, params: dict, rid):
    """Keep profile-only settings local, resolving a proxy's profile explicitly."""
    from tui_gateway import server
    home = Path(home)
    requested = server._profile_home(params.get('profile')) or server._hermes_home
    if params.get('profile') and Path(requested).resolve() != home:
        return server._err(rid, 4092, 'Shared session belongs to another profile')
    params['profile'] = None if Path(server._hermes_home).resolve() == home else home.name
    return None


def _active_list(params: dict, rid):
    """Read the existing host only; losing it never launches a replacement."""
    from tui_gateway import server
    from tui_gateway.session_client_host import HOST_KEY
    home = server._profile_home(params.get('profile')) or server._hermes_home
    if not (Path(home) / 'session-clients' / 'broker.sqlite').exists():
        return None
    hub = get_hub(home)
    host = hub.lookup(HOST_KEY)
    if not host or host['owner'] == hub.owner or not _owner_alive(host):
        return None
    try:
        remote = hub.call(HOST_KEY, 'session.active_list',
                          {'current_session_id': params.get('current_session_id')},
                          expected_owner=host['owner'], timeout=5)
        if 'error' in remote:
            return {**remote, 'id': rid}
        token = _bypass.set(True)
        try:
            local = server.handle_request({'id': rid, 'method': 'session.active_list', 'params': params})
        finally:
            _bypass.reset(token)
        if 'error' in local:
            return local
        rows = {item['id']: item for item in local['result']['sessions']}
        for item in remote['result']['sessions']:
            owner = hub.lookup(item['session_key'])
            if owner and owner['owner'] == host['owner'] and owner['sid'] == item['id'] and _owner_alive(owner):
                rows[item['id']] = item
        return server._ok(rid, {'sessions': list(rows.values())})
    except (RuntimeError, ValueError) as exc:
        # A live-but-unresponsive host is UNKNOWN to the watchdog, not an empty
        # successful list that would end a still-executing turn.
        return server._err(rid, 4092, str(exc))


def route_request(method: str, params: dict, rid):
    """Called after wire validation and transport authentication, before local dispatch."""
    if _bypass.get():
        return None
    from tui_gateway import server
    try:
        if method == 'session.active_list':
            return _active_list(params, rid)
        return _route_session_request(method, params, rid)
    except server.ProfileUnavailableError as exc:
        return server._err(rid, 4064, str(exc))


def _route_session_request(method: str, params: dict, rid):
    from tui_gateway import server
    from tui_gateway.transport import current_transport
    sid = str(params.get('session_id') or '')
    with _remote_lock:
        target = _remote.get(sid)
    if target and ((method == 'config.set' and params.get('key') not in _SESSION_CONFIG_KEYS)
                   or (method == 'config.get' and params.get('key') not in _SESSION_CONFIG_GETTERS)):
        # Display/profile switches stay in the viewing backend, with no mailbox
        # command or invented live session. Native profile scoping still applies.
        return _profile_config_request(target[0], params, rid)
    if not target:
        from tui_gateway.session_client_host import route_hosted_session
        hosted = route_hosted_session(method, params, rid)
        if hosted is not None:
            if 'result' in hosted:
                home = server._profile_home(params.get('profile')) or server._hermes_home
                hub = get_hub(home)
                result = hosted['result']
                key = result['stored_session_id']
                with _remote_lock:
                    _remote[result['session_id']] = (str(hub.home), key, hub.lookup(key)['owner'])
                if current_transport() is not None:
                    hub.subscribe(key, current_transport())
            return hosted
    if target and method == 'session.close':
        home, key, _generation = target
        get_hub(home).unsubscribe(key, current_transport())
        return server._ok(rid, {'closed': True})
    if target and method in _REMOTE_METHODS:
        home, key, _generation = target
    elif method == 'session.resume':
        home = server._profile_home(params.get('profile')) or server._hermes_home
        if not enabled(home) and not (Path(home) / 'session-clients' / 'broker.sqlite').exists():
            return None
        key = sid
    else:
        return None
    hub = get_hub(home)
    owner = hub.lookup(key)
    if owner is None or owner['owner'] == hub.owner:
        return None
    key = owner['session_key']
    if target and (owner['owner'] != target[2] or owner['sid'] != sid):
        return server._err(rid, 4092, 'This view belongs to a retired execution owner; '
                           'explicitly resume the stored session before controlling it')
    if not enabled(home):
        return server._err(rid, 4093, 'Shared clients are disabled; no replacement run was started')
    if params.get('profile'):
        requested = server._profile_home(params['profile']) or server._hermes_home
        if Path(requested).resolve() != hub.home:
            return server._err(rid, 4092, 'Shared session belongs to another profile')
    try:
        head = hub.event_head(key)
        response = hub.call(key, method, params, expected_owner=owner['owner'])
        response['id'] = rid
        if 'result' in response and method in {'session.resume', 'session.activate', 'session.branch', 'session.branch_whole'}:
            runtime = response['result']['session_id']
            if response['result'].get('stored_session_id'):
                key = response['result']['stored_session_id']
                owner = hub.lookup(key)
            with _remote_lock:
                _remote[runtime] = (str(hub.home), key, owner['owner'])
            if current_transport() is not None:
                hub.subscribe(key, current_transport(), after=head)
        return response
    except (RuntimeError, ValueError) as exc:
        return server._err(rid, 4092, str(exc))


def input_accepted(sid: str, text, row_id: int | None, display_kind=None):
    from tui_gateway import server
    if not (server._sessions.get(sid) or {}).get('shared_clients') or not isinstance(text, str) or display_kind == 'hidden':
        return
    server._emit("session.clients.input", sid, {'text': text, 'row_id': row_id, 'origin': _origin.get()})


def rpc_observed(method: str, params: dict, response: dict):
    result = response.get('result') or {}
    accepted = {'queued', 'steered', 'redirected'}
    if method in {'prompt.submit', 'session.steer', 'session.redirect'} and result.get('status') in accepted:
        input_accepted(str(params.get('session_id') or ''), params.get('text'), None, params.get('display_kind'))
    attachment_accepted(method, params, response)


def attachment_accepted(method: str, params: dict, response: dict):
    if method not in {'image.attach', 'image.attach_bytes', 'file.attach', 'pdf.attach'}:
        return
    from tui_gateway import server
    sid = str(params.get('session_id') or '')
    session = server._sessions.get(sid)
    result = response.get('result') or {}
    if not session or not session.get('shared_clients') or not result.get('attached'):
        return
    paths = [result['path']] if result.get('path') else [r['path'] for r in result.get('pages', []) if r.get('path')]
    for path in paths:
        server._emit("session.clients.attachment", sid,
                     {'path': str(path), 'name': Path(path).name, 'origin': _origin.get()})


def register(server):
    def share(rid, params):
        session, error = server._sess_nowait(params, rid)
        if error:
            return error
        home = session.get('profile_home') or server._hermes_home
        if not enabled(home):
            return server._err(rid, 4093, 'Enable session_clients.enabled in this profile before sharing')
        if not get_hub(home).execution_host:
            return server._err(rid, 4092, 'This session predates shared execution. Start a new session, '
                               'or finish this run and reopen its stored history, before sharing.')
        try:
            return server._ok(rid, get_hub(home).share(params['session_id']))
        except (ValueError, RuntimeError) as exc:
            return server._err(rid, 4092, str(exc))
    server.register_method('session.clients.share', share)
